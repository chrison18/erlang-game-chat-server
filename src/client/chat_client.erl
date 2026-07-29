-module(chat_client).
-behaviour(gen_server).

-export([start_link/2,
         login/3,
         list_channels/1,
         join_channel/2,
         leave_channel/2,
         send_channel/3,
         send_private/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

start_link(Host, Port) ->
    gen_server:start_link(?MODULE, [Host, Port], []).

login(ClientPid, RoleName, Password) ->
    gen_server:call(ClientPid, {
        login,
        unicode:characters_to_binary(RoleName),
        unicode:characters_to_binary(Password)
    }).

list_channels(ClientPid) ->
    gen_server:call(ClientPid, list_channels).

join_channel(ClientPid, ChannelId) ->
    gen_server:call(ClientPid, {join_channel, ChannelId}).

leave_channel(ClientPid, ChannelId) ->
    gen_server:call(ClientPid, {leave_channel, ChannelId}).

send_channel(ClientPid, ChannelId, Content) ->
    gen_server:call(ClientPid, {
        send_channel,
        ChannelId,
        unicode:characters_to_binary(Content)
    }).

send_private(ClientPid, TargetRoleName, Content) ->
    gen_server:call(ClientPid, {
        send_private,
        unicode:characters_to_binary(TargetRoleName),
        unicode:characters_to_binary(Content)
    }).

init([Host, Port]) ->
    Options = [binary, {packet, 4}, {active, once}],
    case gen_tcp:connect(Host, Port, Options) of
        {ok, Socket} ->
            {ok, #{socket => Socket,
                   pending => undefined,
                   role_id => undefined,
                   channel_ids => #{}}};
        {error, Reason} ->
            {stop, {connect_failed, Reason}}
    end.

handle_call({login, RoleName, Password}, From,
            #{socket := Socket, pending := undefined} = State) ->
    Packet = chat_client_protocol:encode_login(RoleName, Password),
    send_request(login, Packet, From, Socket, State);
handle_call(list_channels, From,
            #{socket := Socket, pending := undefined} = State) ->
    Packet = chat_client_protocol:encode_channel_list(),
    send_request(list_channels, Packet, From, Socket, State);
handle_call({join_channel, ChannelId}, From,
            #{socket := Socket, pending := undefined} = State) ->
    Packet = chat_client_protocol:encode_channel_join(ChannelId),
    send_request({join_channel, ChannelId}, Packet, From, Socket, State);
handle_call({leave_channel, ChannelId}, From,
            #{socket := Socket, pending := undefined} = State) ->
    Packet = chat_client_protocol:encode_channel_leave(ChannelId),
    send_request({leave_channel, ChannelId}, Packet, From, Socket, State);
handle_call({send_channel, ChannelId, Content}, From,
            #{socket := Socket, pending := undefined} = State) ->
    Packet = chat_client_protocol:encode_channel_send(ChannelId, Content),
    send_request({send_channel, ChannelId}, Packet, From, Socket, State);
handle_call({send_private, TargetRoleName, Content}, From,
            #{socket := Socket, pending := undefined} = State) ->
    Packet = chat_client_protocol:encode_private_send(TargetRoleName, Content),
    send_request({send_private, TargetRoleName}, Packet, From, Socket, State);
handle_call(_Request, _From, #{pending := Pending} = State)
  when Pending =/= undefined ->
    {reply, {error, request_busy}, State};
handle_call(Request, _From, State) ->
    {reply, {error, {unsupported_call, Request}}, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({tcp, Socket, Packet}, #{socket := Socket} = State) ->
    NewState = handle_server_packet(Packet, State),
    case inet:setopts(Socket, [{active, once}]) of
        ok ->
            {noreply, NewState};
        {error, Reason} ->
            {stop, {socket_activation_failed, Reason}, NewState}
    end;
handle_info({tcp_closed, Socket}, #{socket := Socket} = State) ->
    reply_pending(State, {error, connection_closed}),
    {stop, normal, State};
handle_info({tcp_error, Socket, Reason}, #{socket := Socket} = State) ->
    reply_pending(State, {error, {tcp_error, Reason}}),
    {stop, {tcp_error, Reason}, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #{socket := Socket}) ->
    gen_tcp:close(Socket).

handle_server_packet(Packet, State) ->
    case chat_client_protocol:decode_packet(Packet) of
        {ok, {channel_push, Message}} ->
            print_channel_push(Message),
            State;
        {ok, {private_push, Message}} ->
            print_private_push(Message),
            State;
        {ok, Response} ->
            handle_pending_response(Response, State);
        {error, Reason} ->
            handle_protocol_error(Reason, State)
    end.

handle_pending_response(Response,
                        #{pending := {RequestType, From}} = State) ->
    handle_response(RequestType, Response, From, State);
handle_pending_response(_Response, State) ->
    State.

handle_protocol_error(Reason, #{pending := {_RequestType, From}} = State) ->
    gen_server:reply(From, {error, {protocol_error, Reason}}),
    State#{pending := undefined};
handle_protocol_error(_Reason, State) ->
    State.

handle_response(login,
                {login_result, {ok, RoleId, ChannelIds} = Result},
                From, State) ->
    gen_server:reply(From, Result),
    State#{pending := undefined,
           role_id := RoleId,
           channel_ids := maps:from_list(
               [{ChannelId, true} || ChannelId <- ChannelIds])};
handle_response(login, {login_result, {error, _Reason} = Result},
                From, State) ->
    gen_server:reply(From, Result),
    State#{pending := undefined};
handle_response(list_channels,
                {channel_list_result, {ok, Channels} = Result},
                From, State) ->
    gen_server:reply(From, Result),
    JoinedChannels = maps:from_list([
        {maps:get(channel_id, Channel), true}
     || Channel <- Channels,
        maps:get(joined, Channel)]),
    State#{pending := undefined, channel_ids := JoinedChannels};
handle_response({join_channel, _RequestedChannelId},
                {channel_join_result, {ok, ChannelId} = Result},
                From, #{channel_ids := ChannelIds} = State) ->
    gen_server:reply(From, Result),
    State#{pending := undefined,
           channel_ids := maps:put(ChannelId, true, ChannelIds)};
handle_response({join_channel, _RequestedChannelId},
                {channel_join_result, {error, _Reason} = Result},
                From, State) ->
    gen_server:reply(From, Result),
    State#{pending := undefined};
handle_response({leave_channel, _RequestedChannelId},
                {channel_leave_result, {ok, ChannelId} = Result},
                From, #{channel_ids := ChannelIds} = State) ->
    gen_server:reply(From, Result),
    State#{pending := undefined,
           channel_ids := maps:remove(ChannelId, ChannelIds)};
handle_response({leave_channel, _RequestedChannelId},
                {channel_leave_result, {error, _Reason} = Result},
                From, State) ->
    gen_server:reply(From, Result),
    State#{pending := undefined};
handle_response({send_channel, _RequestedChannelId},
                {channel_send_result, Result}, From, State) ->
    gen_server:reply(From, Result),
    State#{pending := undefined};
handle_response({send_private, _RequestedTargetName},
                {private_send_result, Result}, From, State) ->
    gen_server:reply(From, Result),
    State#{pending := undefined};
handle_response(_RequestType, {server_error, RequestProtoId, Reason},
                From, State) ->
    gen_server:reply(From, {error, {server_error, RequestProtoId, Reason}}),
    State#{pending := undefined};
handle_response(_RequestType, Response, From, State) ->
    gen_server:reply(From, {error, {unexpected_response, Response}}),
    State#{pending := undefined}.

send_request(RequestType, Packet, From, Socket, State) ->
    case gen_tcp:send(Socket, Packet) of
        ok ->
            {noreply, State#{pending := {RequestType, From}}};
        {error, Reason} ->
            {reply, {error, {send_failed, Reason}}, State}
    end.

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

reply_pending(#{pending := {_RequestType, From}}, Reply) ->
    gen_server:reply(From, Reply);
reply_pending(#{pending := undefined}, _Reply) ->
    ok.
