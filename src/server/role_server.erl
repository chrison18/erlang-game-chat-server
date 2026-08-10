-module(role_server).
-behaviour(gen_server).

-include("chat_protocol.hrl").
-include("chat_record.hrl").

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

start_link() ->
    gen_server:start_link(?MODULE, [], []).

init([]) ->
    {ok, #{socket => undefined}}.

handle_call(Request, _From, State) ->
    {reply, {error, {unsupported_call, Request}}, State}.

handle_cast({push_channel, ChannelId, SenderRoleId, SenderRoleName, Content},
            #{socket := Socket} = State) ->
    Packet = chat_server_protocol:encode_channel_push(
        ChannelId, SenderRoleId, SenderRoleName, Content),
    handle_push_send(Socket, Packet, State);
handle_cast({push_channel_batch, Packet}, #{socket := Socket} = State) ->
    handle_push_send(Socket, Packet, State);
handle_cast({push_private, SenderRoleId, SenderRoleName, Content},
            #{socket := Socket} = State) ->
    Packet = chat_server_protocol:encode_private_push(
        SenderRoleId, SenderRoleName, Content),
    handle_push_send(Socket, Packet, State);
handle_cast({push_nearby, SenderRoleId, SenderRoleName, {X, Y}, Content},
            #{socket := Socket} = State) ->
    Packet = chat_server_protocol:encode_nearby_push(
        SenderRoleId, SenderRoleName, X, Y, Content),
    handle_push_send(Socket, Packet, State);
handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({socket_ready, Socket}, #{socket := undefined} = State) ->
    case inet:setopts(Socket, [{active, true}]) of
        ok ->
            {noreply, State#{socket := Socket}};
        {error, Reason} ->
            {stop, {socket_activation_failed, Reason}, State#{socket := Socket}}
    end;
handle_info({tcp, Socket, Packet}, #{socket := Socket} = State) ->
    case handle_packet(Packet, Socket) of
        ok ->
            {noreply, State};
        {error, Reason} ->
            {stop, Reason, State}
    end;
handle_info({tcp_closed, Socket}, #{socket := Socket} = State) ->
    {stop, normal, State};
handle_info({tcp_error, Socket, Reason}, #{socket := Socket} = State) ->
    {stop, {tcp_error, Reason}, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #{socket := undefined}) ->
    ok;
terminate(_Reason, #{socket := Socket}) ->
    gen_tcp:close(Socket).

handle_packet(Packet, Socket) ->
    case chat_server_protocol:decode_request(Packet) of
        {ok, {login, RoleName, Password}} ->
            handle_login(Socket, RoleName, Password);
        {ok, list_channels} ->
            handle_authenticated_request(
                ?PROTO_CHANNEL_LIST_REQUEST, list_channels, Socket);
        {ok, {join_channel, ChannelId}} ->
            handle_authenticated_request(
                ?PROTO_CHANNEL_JOIN_REQUEST, {join_channel, ChannelId}, Socket);
        {ok, {leave_channel, ChannelId}} ->
            handle_authenticated_request(
                ?PROTO_CHANNEL_LEAVE_REQUEST, {leave_channel, ChannelId}, Socket);
        {ok, {send_channel, ChannelId, Content}} ->
            handle_authenticated_request(
                ?PROTO_CHANNEL_SEND_REQUEST,
                {send_channel, ChannelId, Content}, Socket);
        {ok, {send_private, TargetRoleName, Content}} ->
            handle_authenticated_request(
                ?PROTO_PRIVATE_SEND_REQUEST,
                {send_private, TargetRoleName, Content}, Socket);
        {ok, {move, Direction}} ->
            handle_authenticated_request(
                ?PROTO_MAP_MOVE_REQUEST, {move, Direction}, Socket);
        {ok, {teleport, Position}} ->
            handle_authenticated_request(
                ?PROTO_MAP_TELEPORT_REQUEST, {teleport, Position}, Socket);
        {ok, {send_nearby, Content}} ->
            handle_authenticated_request(
                ?PROTO_NEARBY_SEND_REQUEST, {send_nearby, Content}, Socket);
        {ok, {request, ProtoId, _Data}} ->
            case get(role_id) of
                undefined -> send_packet(Socket,
                    chat_server_protocol:encode_error(ProtoId, not_logged_in));
                _RoleId -> send_packet(Socket,
                    chat_server_protocol:encode_error(ProtoId, unknown_proto))
            end;
        {error, {invalid_packet, ProtoId}} ->
            send_packet(Socket,
                chat_server_protocol:encode_error(ProtoId, invalid_packet));
        {error, {unknown_proto, ProtoId}} ->
            send_packet(Socket,
                chat_server_protocol:encode_error(ProtoId, unknown_proto))
    end.

handle_authenticated_request(ProtoId, Request, Socket) ->
    case get(role_id) of
        undefined ->
            send_packet(Socket,
                chat_server_protocol:encode_error(ProtoId, not_logged_in));
        RoleId ->
            handle_business_request(Request, Socket, RoleId)
    end.

handle_business_request(list_channels, Socket, _RoleId) ->
    JoinedChannels = get(channel_ids),
    ChannelList = [
        {ChannelId,
         ChannelType,
         joined_value(maps:is_key(ChannelId, JoinedChannels)),
         ChannelName}
     || {ChannelId, ChannelType, ChannelName} <- channel_server:channels()],
    send_packet(Socket,
        chat_server_protocol:encode_channel_list_result(ChannelList));
handle_business_request({join_channel, ChannelId}, Socket, RoleId) ->
    ProtocolResult = case channel_server:join(ChannelId, RoleId, self()) of
        {ok, ChannelId} = Result ->
            put(channel_ids, maps:put(ChannelId, true, get(channel_ids))),
            Result;
        {error, Reason} ->
            {error, Reason, ChannelId}
    end,
    send_packet(Socket,
        chat_server_protocol:encode_channel_join_result(ProtocolResult));
handle_business_request({leave_channel, ChannelId}, Socket, RoleId) ->
    ProtocolResult = case channel_server:leave(ChannelId, RoleId) of
        {ok, ChannelId} = Result ->
            put(channel_ids, maps:remove(ChannelId, get(channel_ids))),
            Result;
        {error, Reason} ->
            {error, Reason, ChannelId}
    end,
    send_packet(Socket,
        chat_server_protocol:encode_channel_leave_result(ProtocolResult));
handle_business_request({send_channel, ChannelId, Content}, Socket, RoleId) ->
    Result = send_channel_message(ChannelId, RoleId, get(role_name), Content),
    ProtocolResult = case Result of
        {ok, ChannelId} -> {ok, ChannelId};
        {error, Reason} -> {error, Reason, ChannelId}
    end,
    send_packet(Socket,
        chat_server_protocol:encode_channel_send_result(ProtocolResult));
handle_business_request({send_private, TargetRoleName, Content}, Socket,
                        RoleId) ->
    Result = send_private_message(
        TargetRoleName, RoleId, get(role_name), Content),
    ProtocolResult = case Result of
        {ok, TargetRoleName} -> {ok, TargetRoleName};
        {error, target_offline} ->
            {error, target_offline, TargetRoleName}
    end,
    send_packet(Socket,
        chat_server_protocol:encode_private_send_result(ProtocolResult));
handle_business_request({move, Direction}, Socket, _RoleId) ->
    Result = move(Direction, get(position)),
    send_packet(Socket, chat_server_protocol:encode_move_result(Result));
handle_business_request({teleport, Position}, Socket, _RoleId) ->
    Result = teleport(Position, get(position)),
    send_packet(Socket, chat_server_protocol:encode_teleport_result(Result));
handle_business_request({send_nearby, Content}, Socket, RoleId) ->
    Position = get(position),
    Targets = map_server:nearby(Position),
    lists:foreach(
        fun(TargetPid) ->
            gen_server:cast(TargetPid, {
                push_nearby,
                RoleId,
                get(role_name),
                Position,
                Content
            })
        end,
        Targets),
    send_packet(Socket,
        chat_server_protocol:encode_nearby_send_result(
            {ok, length(Targets)})).

joined_value(true) -> 1;
joined_value(false) -> 0.

handle_login(Socket, RoleName, Password) ->
    case get(role_id) of
        undefined ->
            case role_online_server:login(self(), RoleName, Password) of
                {ok, RoleId} ->
                    InitialPosition = {0, 0},
                    ok = map_server:enter(self(), InitialPosition),
                    ChannelIds = join_initial_channels(RoleId),
                    put(role_id, RoleId),
                    put(role_name, RoleName),
                    put(position, InitialPosition),
                    put(channel_ids,
                        maps:from_list([{ChannelId, true} || ChannelId <- ChannelIds])),
                    send_packet(Socket,
                        chat_server_protocol:encode_login_result(
                            {ok, RoleId, InitialPosition, ChannelIds}));
                {error, Reason} ->
                    send_packet(Socket,
                        chat_server_protocol:encode_login_result({error, Reason}))
            end;
        _RoleId ->
            send_packet(Socket,
                chat_server_protocol:encode_login_result(
                    {error, already_online}))
    end.

join_initial_channels(RoleId) ->
    PublicCount = rand:uniform(3),
    RandomizedPublicIds = [
        ChannelId
     || {_RandomKey, ChannelId} <-
            lists:sort([{rand:uniform(), Id} || Id <- lists:seq(2, 10)])
    ],
    ChannelIds = [1 | lists:sublist(RandomizedPublicIds, PublicCount)],
    lists:foreach(
        fun(ChannelId) ->
            {ok, ChannelId} =
                channel_server:join(ChannelId, RoleId, self())
        end,
        ChannelIds
    ),
    ChannelIds.

send_channel_message(ChannelId, RoleId, RoleName, Content) ->
    case channel_server:channel(ChannelId) of
        error ->
            {error, invalid_channel};
        {ok, _ChannelType, _ChannelName} ->
            case maps:is_key(ChannelId, get(channel_ids)) of
                false ->
                    {error, not_joined};
                true ->
                    channel_server:send_channel(
                        ChannelId, RoleId, RoleName, Content)
            end
    end.

send_private_message(TargetRoleName, SenderRoleId, SenderRoleName, Content) ->
    case ets:lookup(online_roles, TargetRoleName) of
        [#online_role{role_pid = TargetRolePid}] ->
            gen_server:cast(TargetRolePid, {
                push_private,
                SenderRoleId,
                SenderRoleName,
                Content
            }),
            {ok, TargetRoleName};
        [] ->
            {error, target_offline}
    end.

move(Direction, {X, Y} = Position) ->
    Target = case Direction of
        up -> {X - 1, Y};
        down -> {X + 1, Y};
        left -> {X, Y - 1};
        right -> {X, Y + 1};
        invalid -> invalid
    end,
    case Target of
        invalid ->
            {error, invalid_direction, Position};
        _ ->
            case map_server:valid_position(Target) of
                true -> relocate(Position, Target);
                false -> {error, out_of_bounds, Position}
            end
    end.

teleport(Target, Position) ->
    case map_server:valid_position(Target) of
        true -> relocate(Position, Target);
        false -> {error, invalid_position, Position}
    end.

relocate(OldPosition, NewPosition) ->
    ok = map_server:relocate(self(), OldPosition, NewPosition),
    put(position, NewPosition),
    {ok, NewPosition}.

handle_push_send(Socket, Packet, State) ->
    case send_packet(Socket, Packet) of
        ok ->
            {noreply, State};
        {error, Reason} ->
            {stop, Reason, State}
    end.

send_packet(Socket, Packet) ->
    case gen_tcp:send(Socket, Packet) of
        ok -> ok;
        {error, Reason} -> {error, {tcp_send_failed, Reason}}
    end.
