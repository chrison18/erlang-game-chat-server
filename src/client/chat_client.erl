-module(chat_client).
-behaviour(gen_server).

-export([start_link/5]).
-export([init/1, handle_call/3, handle_cast/2, handle_continue/2,
         handle_info/2, terminate/2]).

-define(AUTO_SEND_INTERVAL_MS, 3000).
-define(OBSERVER_REPORT_INTERVAL_MS, 1000).

start_link(Host, Port, RoleName, Password, Mode) ->
    gen_server:start_link(
        ?MODULE, [Host, Port, RoleName, Password, Mode], []).

init([Host, Port, RoleName, Password, Mode]) ->
    Options = [binary, {packet, 4}, {active, true}],
    case gen_tcp:connect(Host, Port, Options) of
        {ok, Socket} ->
            State = mode_state(Mode, #{socket => Socket,
                                       role_name => RoleName,
                                       action_seq => 1,
                                       observer_received => 0,
                                       observer_invalid => 0}),
            {ok, State,
             {continue, {login, RoleName, Password}}};
        {error, Reason} ->
            {stop, {connect_failed, Reason}}
    end.

handle_call(Request, _From, State) ->
    {stop, {unsupported_call, Request}, State}.

handle_cast(list_channels, State) ->
    {noreply, do_list_channels(State)};
handle_cast({join_channel, ChannelId}, State) ->
    {noreply, do_join_channel(ChannelId, State)};
handle_cast({leave_channel, ChannelId}, State) ->
    {noreply, do_leave_channel(ChannelId, State)};
handle_cast({send_channel, ChannelId, Content}, State) ->
    {noreply, do_send_channel(ChannelId, Content, State)};
handle_cast({send_private, TargetRoleName, Content}, State) ->
    {noreply, do_send_private(TargetRoleName, Content, State)};
handle_cast(_Request, State) ->
    {noreply, State}.

handle_continue({login, RoleName, Password}, State) ->
    {noreply, do_login(RoleName, Password, State)}.

handle_info(auto_action,
            #{mode := normal,
              role_name := RoleName,
              action_seq := Sequence} = State) ->
    Content = auto_message(RoleName, Sequence),
    ActionState = do_auto_action(Content, State),
    schedule_next_action(),
    {noreply, ActionState#{action_seq := Sequence + 1}};
handle_info(observer_report,
            #{mode := observer,
              role_name := RoleName,
              observer_received := Received,
              observer_invalid := Invalid} = State) ->
    io:format("observer=~ts received=~p invalid=~p~n",
              [RoleName, Received, Invalid]),
    schedule_observer_report(),
    {noreply, State#{observer_received := 0,
                     observer_invalid := 0}};
handle_info({tcp, Socket, Packet}, #{socket := Socket} = State) ->
    {noreply, handle_server_packet(Packet, State)};
handle_info({tcp_closed, Socket}, #{socket := Socket} = State) ->
    {stop, normal, State};
handle_info({tcp_error, Socket, Reason}, #{socket := Socket} = State) ->
    {stop, {tcp_error, Reason}, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #{socket := Socket}) ->
    gen_tcp:close(Socket).

do_login(RoleName, Password, State) ->
    case normalize_texts([RoleName, Password]) of
        {ok, [RoleNameBinary, PasswordBinary]} ->
            Packet = chat_client_protocol:encode_login(
                RoleNameBinary, PasswordBinary),
            send_packet(login, Packet,
                        State#{role_name := RoleNameBinary});
        {error, Reason} ->
            report_result(login, {error, Reason}, State)
    end.

do_list_channels(State) ->
    Packet = chat_client_protocol:encode_channel_list(),
    send_packet(list_channels, Packet, State).

do_join_channel(ChannelId, State) when is_integer(ChannelId),
                                       ChannelId >= 0,
                                       ChannelId =< 16#FFFFFFFF ->
    Packet = chat_client_protocol:encode_channel_join(ChannelId),
    send_packet({join_channel, ChannelId}, Packet, State);
do_join_channel(_ChannelId, State) ->
    report_result(join_channel, {error, invalid_channel_id}, State).

do_leave_channel(ChannelId, State) when is_integer(ChannelId),
                                        ChannelId >= 0,
                                        ChannelId =< 16#FFFFFFFF ->
    Packet = chat_client_protocol:encode_channel_leave(ChannelId),
    send_packet({leave_channel, ChannelId}, Packet, State);
do_leave_channel(_ChannelId, State) ->
    report_result(leave_channel, {error, invalid_channel_id}, State).

do_send_channel(ChannelId, Content, State)
  when is_integer(ChannelId), ChannelId >= 0, ChannelId =< 16#FFFFFFFF ->
    case normalize_text(Content) of
        {ok, ContentBinary} ->
            Packet = chat_client_protocol:encode_channel_send(
                ChannelId, ContentBinary),
            send_packet({send_channel, ChannelId}, Packet, State);
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
                        Packet, State);
        {error, Reason} ->
            report_result(send_private, {error, Reason}, State)
    end.

send_packet(Action, Packet, #{socket := Socket} = State) ->
    case gen_tcp:send(Socket, Packet) of
        ok ->
            State;
        {error, Reason} ->
            report_result(Action, {error, {send_failed, Reason}}, State)
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
        {ok, {channel_push_batch, Messages}} ->
            handle_channel_push_batch(Messages, State);
        {ok, {channel_push, Message}} ->
            handle_channel_push(Message, State);
        {ok, {private_push, _Message}} ->
            State;
        {ok, Response} ->
            handle_response(Response, State);
        {error, Reason} ->
            handle_invalid_packet(Reason, State)
    end.

handle_response({login_result, {ok, _RoleId, ChannelIds} = Result}, State) ->
    LoggedInState = remember_channels(ChannelIds, State),
    report_result(login, Result, start_mode_after_login(LoggedInState));
handle_response({login_result, Result}, State) ->
    report_result(login, Result, State);
handle_response({channel_list_result, {ok, Channels} = Result},
                #{channel_ids := _ChannelIds} = State) ->
    JoinedIds = [maps:get(channel_id, Channel)
                 || Channel <- Channels, maps:get(joined, Channel)],
    report_result(list_channels, Result,
                  State#{channel_ids := JoinedIds});
handle_response({channel_list_result, Result}, State) ->
    report_result(list_channels, Result, State);
handle_response({channel_join_result, {ok, ChannelId} = Result},
                #{channel_ids := ChannelIds} = State) ->
    report_result(join_channel, Result,
                  State#{channel_ids := [ChannelId | ChannelIds]});
handle_response({channel_join_result, Result}, State) ->
    report_result(join_channel, Result, State);
handle_response({channel_leave_result, {ok, ChannelId} = Result},
                #{channel_ids := ChannelIds} = State) ->
    report_result(leave_channel, Result,
                  State#{channel_ids := lists:delete(ChannelId, ChannelIds)});
handle_response({channel_leave_result, Result}, State) ->
    report_result(leave_channel, Result, State);
handle_response({channel_send_result, Result}, State) ->
    report_result(send_channel, Result, State);
handle_response({private_send_result, Result}, State) ->
    report_result(send_private, Result, State);
handle_response({server_error, RequestProtoId, Reason}, State) ->
    report_result({server_error, RequestProtoId}, {error, Reason}, State).

report_result(Action, Result, #{mode := observer} = State) ->
    print_result(Action, Result),
    State;
report_result(_Action, _Result, State) ->
    State.

handle_channel_push(Message,
                    #{mode := observer,
                      observer_received := Received} = State) ->
    print_channel_push(Message),
    State#{observer_received := Received + 1};
handle_channel_push(_Message, State) ->
    State.

handle_channel_push_batch(Messages, State) ->
    lists:foldl(fun handle_channel_push/2, State, Messages).

handle_invalid_packet(Reason,
                      #{mode := observer,
                        observer_invalid := Invalid} = State) ->
    print_result(protocol, {error, Reason}),
    State#{observer_invalid := Invalid + 1};
handle_invalid_packet(_Reason, State) ->
    State.

start_mode_after_login(#{mode := observer} = State) ->
    schedule_observer_report(),
    State;
start_mode_after_login(#{mode := normal} = State) ->
    schedule_next_action(),
    State;
start_mode_after_login(State) ->
    State.

schedule_next_action() ->
    _ = erlang:send_after(
        ?AUTO_SEND_INTERVAL_MS, self(), auto_action),
    ok.

schedule_observer_report() ->
    _ = erlang:send_after(
        ?OBSERVER_REPORT_INTERVAL_MS, self(), observer_report),
    ok.

auto_message(RoleName, Sequence) ->
    <<RoleName/binary, " auto message ",
      (integer_to_binary(Sequence))/binary>>.

mode_state(observer, State) ->
    State#{mode => observer};
mode_state({normal, ClientId, StartId, EndId}, State) ->
    State#{mode => normal,
           client_id => ClientId,
           client_range => {StartId, EndId},
           channel_ids => []};
mode_state(normal, State) ->
    State#{mode => normal,
           client_id => undefined,
           client_range => undefined,
           channel_ids => []}.

remember_channels(ChannelIds, #{channel_ids := _OldChannelIds} = State) ->
    State#{channel_ids := ChannelIds};
remember_channels(_ChannelIds, State) ->
    State.

do_auto_action(Content, State) ->
    case rand:uniform(10) of
        Roll when Roll =< 4 -> do_send_channel(1, Content, State);
        Roll when Roll =< 8 -> auto_private(Content, State);
        9 -> auto_join(Content, State);
        10 -> auto_leave(Content, State)
    end.

auto_private(Content,
             #{client_id := ClientId,
               client_range := {StartId, EndId}} = State) ->
    case random_target(ClientId, StartId, EndId) of
        none -> do_send_channel(1, Content, State);
        TargetId -> do_send_private(client_role_name(TargetId), Content, State)
    end;
auto_private(Content, State) ->
    do_send_channel(1, Content, State).

%% ponytail: at most 10 channels; use sets only if the channel count grows.
auto_join(Content, #{channel_ids := ChannelIds} = State) ->
    case random_member(lists:seq(2, 10) -- ChannelIds) of
        none -> do_send_channel(1, Content, State);
        ChannelId -> do_join_channel(ChannelId, State)
    end.

auto_leave(Content, #{channel_ids := ChannelIds} = State) ->
    case random_member(lists:delete(1, ChannelIds)) of
        none -> do_send_channel(1, Content, State);
        ChannelId -> do_leave_channel(ChannelId, State)
    end.

random_target(_ClientId, Id, Id) ->
    none;
random_target(ClientId, StartId, EndId) ->
    Candidate = StartId + rand:uniform(EndId - StartId) - 1,
    case Candidate >= ClientId of
        true -> Candidate + 1;
        false -> Candidate
    end.

random_member([]) ->
    none;
random_member(Items) ->
    lists:nth(rand:uniform(length(Items)), Items).

client_role_name(ClientId) ->
    <<"client_", (integer_to_binary(ClientId))/binary>>.

print_result(Action, Result) ->
    io:format("[~p] ~p~n", [Action, Result]).

print_channel_push(#{channel_id := ChannelId,
                     sender_role_id := SenderRoleId,
                     sender_role_name := SenderRoleName,
                     content := Content}) ->
    io:format("[channel ~p] ~ts(~p): ~ts~n",
              [ChannelId, SenderRoleName, SenderRoleId, Content]).
