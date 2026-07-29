-module(chat_client).
-behaviour(gen_server).

-export([start_link/2, login/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

start_link(Host, Port) ->
    gen_server:start_link(?MODULE, [Host, Port], []).

login(ClientPid, RoleName, Password) ->
    gen_server:call(ClientPid, {
        login,
        unicode:characters_to_binary(RoleName),
        unicode:characters_to_binary(Password)
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
    case gen_tcp:send(Socket, Packet) of
        ok ->
            {noreply, State#{pending := {login, From}}};
        {error, Reason} ->
            {reply, {error, {send_failed, Reason}}, State}
    end;
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

handle_server_packet(Packet, #{pending := {login, From}} = State) ->
    case chat_client_protocol:decode_packet(Packet) of
        {ok, {login_result, {ok, RoleId, ChannelIds} = Result}} ->
            gen_server:reply(From, Result),
            State#{pending := undefined,
                   role_id := RoleId,
                   channel_ids := maps:from_list(
                       [{ChannelId, true} || ChannelId <- ChannelIds])};
        {ok, {login_result, {error, _Reason} = Result}} ->
            gen_server:reply(From, Result),
            State#{pending := undefined};
        {ok, {server_error, RequestProtoId, Reason}} ->
            gen_server:reply(From, {error, {server_error, RequestProtoId, Reason}}),
            State#{pending := undefined};
        {error, Reason} ->
            gen_server:reply(From, {error, {protocol_error, Reason}}),
            State#{pending := undefined}
    end;
handle_server_packet(_Packet, State) ->
    State.

reply_pending(#{pending := {_RequestType, From}}, Reply) ->
    gen_server:reply(From, Reply);
reply_pending(#{pending := undefined}, _Reply) ->
    ok.
