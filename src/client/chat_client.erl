-module(chat_client).
-behaviour(gen_server).

-export([start_link/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(AUTO_SEND_INTERVAL_MS, 1000).

start_link(Host, Port) ->
    gen_server:start_link(?MODULE, [Host, Port], []).

init([Host, Port]) ->
    Options = [binary, {packet, 4}, {active, once}],
    case gen_tcp:connect(Host, Port, Options) of
        {ok, Socket} ->
            {ok, #{socket => Socket,
                   status => connected,
                   role_id => undefined,
                   role_name => undefined,
                   channel_ids => #{},
                   auto_send_seq => 1}};
        {error, Reason} ->
            {stop, {connect_failed, Reason}}
    end.

handle_call(Request, _From, State) ->
    {stop, {unsupported_call, Request}, State}.

handle_cast(Request, State) ->
    {stop, {unsupported_cast, Request}, State}.

handle_info({login, RoleName, Password}, State) ->
    {noreply, do_login(RoleName, Password, State)};
handle_info(list_channels, State) ->
    {noreply, do_list_channels(State)};
handle_info({join_channel, ChannelId}, State) ->
    {noreply, do_join_channel(ChannelId, State)};
handle_info({leave_channel, ChannelId}, State) ->
    {noreply, do_leave_channel(ChannelId, State)};
handle_info({send_channel, ChannelId, Content}, State) ->
    {noreply, do_send_channel(ChannelId, Content, State)};
handle_info({send_private, TargetRoleName, Content}, State) ->
    {noreply, do_send_private(TargetRoleName, Content, State)};
handle_info(auto_send_channel,
            #{status := online,
              role_name := RoleName,
              auto_send_seq := Sequence} = State) ->
    Content = auto_message(RoleName, Sequence),
    SentState = do_send_channel(1, Content, State),
    schedule_auto_send(),
    {noreply, SentState#{auto_send_seq := Sequence + 1}};
handle_info(stop, State) ->
    {stop, normal, State};
handle_info({tcp, Socket, Packet}, #{socket := Socket} = State) ->
    NewState = handle_server_packet(Packet, State),
    case inet:setopts(Socket, [{active, once}]) of
        ok ->
            {noreply, NewState};
        {error, Reason} ->
            {stop, {socket_activation_failed, Reason}, NewState}
    end;
handle_info({tcp_closed, Socket}, #{socket := Socket} = State) ->
    {stop, normal, State};
handle_info({tcp_error, Socket, Reason}, #{socket := Socket} = State) ->
    {stop, {tcp_error, Reason}, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #{socket := Socket}) ->
    gen_tcp:close(Socket).

do_login(_RoleName, _Password, #{status := online} = State) ->
    report_result(login, {error, already_logged_in}, State);
do_login(_RoleName, _Password, #{status := logging_in} = State) ->
    report_result(login, {error, login_in_progress}, State);
do_login(RoleName, Password, State) ->
    case normalize_texts([RoleName, Password]) of
        {ok, [RoleNameBinary, PasswordBinary]} ->
            Packet = chat_client_protocol:encode_login(
                RoleNameBinary, PasswordBinary),
            SentState = State#{status := logging_in,
                              role_name := RoleNameBinary},
            send_packet(login, Packet, SentState, State);
        {error, Reason} ->
            report_result(login, {error, Reason}, State)
    end.

do_list_channels(State) ->
    Packet = chat_client_protocol:encode_channel_list(),
    send_packet(list_channels, Packet, State, State).

do_join_channel(ChannelId, State) when is_integer(ChannelId),
                                       ChannelId >= 0,
                                       ChannelId =< 16#FFFFFFFF ->
    Packet = chat_client_protocol:encode_channel_join(ChannelId),
    send_packet({join_channel, ChannelId}, Packet, State, State);
do_join_channel(_ChannelId, State) ->
    report_result(join_channel, {error, invalid_channel_id}, State).

do_leave_channel(ChannelId, State) when is_integer(ChannelId),
                                        ChannelId >= 0,
                                        ChannelId =< 16#FFFFFFFF ->
    Packet = chat_client_protocol:encode_channel_leave(ChannelId),
    send_packet({leave_channel, ChannelId}, Packet, State, State);
do_leave_channel(_ChannelId, State) ->
    report_result(leave_channel, {error, invalid_channel_id}, State).

do_send_channel(ChannelId, Content, State)
  when is_integer(ChannelId), ChannelId >= 0, ChannelId =< 16#FFFFFFFF ->
    case normalize_text(Content) of
        {ok, ContentBinary} ->
            Packet = chat_client_protocol:encode_channel_send(
                ChannelId, ContentBinary),
            send_packet({send_channel, ChannelId}, Packet, State, State);
        {error, Reason} ->
            report_result(send_channel, {error, Reason}, State)
    end;
do_send_channel(_ChannelId, _Content, State) ->
    report_result(send_channel, {error, invalid_channel_id}, State).

do_send_private(TargetRoleName, Content, State) ->
    case normalize_texts([TargetRoleName, Content]) of
        {ok, [TargetRoleNameBinary, ContentBinary]} ->
            Packet = chat_client_protocol:encode_private_send(
                TargetRoleNameBinary, ContentBinary),
            send_packet({send_private, TargetRoleNameBinary},
                        Packet, State, State);
        {error, Reason} ->
            report_result(send_private, {error, Reason}, State)
    end.

send_packet(Action, Packet, SentState, #{socket := Socket} = CurrentState) ->
    case gen_tcp:send(Socket, Packet) of
        ok ->
            SentState;
        {error, Reason} ->
            report_result(Action, {error, {send_failed, Reason}}, CurrentState)
    end.

normalize_texts(Texts) ->
    normalize_texts(Texts, []).

normalize_texts([], Acc) ->
    {ok, lists:reverse(Acc)};
normalize_texts([Text | Rest], Acc) ->
    case normalize_text(Text) of
        {ok, Binary} -> normalize_texts(Rest, [Binary | Acc]);
        {error, Reason} -> {error, Reason}
    end.

normalize_text(Text) ->
    try unicode:characters_to_binary(Text) of
        Binary when is_binary(Binary) ->
            {ok, Binary};
        _Error ->
            {error, invalid_text}
    catch
        error:badarg ->
            {error, invalid_text}
    end.

handle_server_packet(Packet, State) ->
    case chat_client_protocol:decode_packet(Packet) of
        {ok, {channel_push, Message}} ->
            print_channel_push(Message),
            State;
        {ok, {private_push, Message}} ->
            print_private_push(Message),
            State;
        {ok, Response} ->
            handle_response(Response, State);
        {error, Reason} ->
            report_result(protocol, {error, Reason}, State)
    end.

handle_response({login_result, {ok, RoleId, ChannelIds} = Result}, State) ->
    NewState = State#{status := online,
                     role_id := RoleId,
                     channel_ids := maps:from_list(
                         [{ChannelId, true} || ChannelId <- ChannelIds])},
    schedule_auto_send(),
    report_result(login, Result, NewState);
handle_response({login_result, {error, _Reason} = Result}, State) ->
    NewState = State#{status := connected,
                     role_id := undefined,
                     role_name := undefined,
                     channel_ids := #{}},
    report_result(login, Result, NewState);
handle_response({channel_list_result, {ok, Channels} = Result}, State) ->
    JoinedChannels = maps:from_list([
        {maps:get(channel_id, Channel), true}
     || Channel <- Channels,
        maps:get(joined, Channel)]),
    report_result(list_channels, Result,
                  State#{channel_ids := JoinedChannels});
handle_response({channel_join_result, {ok, ChannelId} = Result},
                #{channel_ids := ChannelIds} = State) ->
    NewState = State#{channel_ids := maps:put(ChannelId, true, ChannelIds)},
    report_result(join_channel, Result, NewState);
handle_response({channel_join_result,
                 {error, _Reason, _ChannelId} = Result}, State) ->
    report_result(join_channel, Result, State);
handle_response({channel_leave_result, {ok, ChannelId} = Result},
                #{channel_ids := ChannelIds} = State) ->
    NewState = State#{channel_ids := maps:remove(ChannelId, ChannelIds)},
    report_result(leave_channel, Result, NewState);
handle_response({channel_leave_result,
                 {error, _Reason, _ChannelId} = Result}, State) ->
    report_result(leave_channel, Result, State);
handle_response({channel_send_result, Result}, State) ->
    report_result(send_channel, Result, State);
handle_response({private_send_result, Result}, State) ->
    report_result(send_private, Result, State);
handle_response({server_error, RequestProtoId, Reason}, State) ->
    report_result({server_error, RequestProtoId}, {error, Reason}, State).

report_result(Action, Result, State) ->
    print_result(Action, Result),
    State.

schedule_auto_send() ->
    _ = erlang:send_after(?AUTO_SEND_INTERVAL_MS, self(), auto_send_channel),
    ok.

auto_message(RoleName, Sequence) ->
    <<RoleName/binary, " auto message ",
      (integer_to_binary(Sequence))/binary>>.

print_result(Action, Result) ->
    io:format("[~p] ~p~n", [Action, Result]).

print_channel_push(#{channel_id := ChannelId,
                     sender_role_id := SenderRoleId,
                     sender_role_name := SenderRoleName,
                     content := Content}) ->
    io:format("[channel ~p] ~ts(~p): ~ts~n",
              [ChannelId, SenderRoleName, SenderRoleId, Content]).

print_private_push(#{sender_role_id := SenderRoleId,
                     sender_role_name := SenderRoleName,
                     content := Content}) ->
    io:format("[private] ~ts(~p): ~ts~n",
              [SenderRoleName, SenderRoleId, Content]).
