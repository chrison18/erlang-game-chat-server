-module(role_server).
-behaviour(gen_server).

%% 一条 TCP 连接对应一个 Role 进程：解码请求、调用业务进程并维护已确认状态。
%% 频道、地图和位置只在下游操作成功后写入进程字典。

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

handle_cast({push_packets, Packets}, #{socket := Socket} = State) ->
    do_push_packets(Socket, Packets),
    {noreply, State};
handle_cast({push_batch, Packet}, #{socket := Socket} = State) ->
    do_push_batch(Socket, Packet),
    {noreply, State};
handle_cast({push_private, SenderRoleId, SenderRoleName, Content},
            #{socket := Socket} = State) ->
    do_push_private(Socket, SenderRoleId, SenderRoleName, Content),
    {noreply, State};
handle_cast({map_result, MapPid, Operation, Result},
            #{socket := Socket} = State) ->
    do_map_result(Socket, MapPid, Operation, Result),
    {noreply, State};
handle_cast({rejoin_channel, ChannelId}, State) ->
    do_rejoin_channel(ChannelId),
    {noreply, State};
handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({socket_ready, Socket}, #{socket := undefined} = State) ->
    %% listener 完成 Socket 所有权交接后，Role 才切到 active 模式接收 TCP 消息。
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
        stop ->
            {stop, normal, State};
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
    %% 协议层只负责解析；除登录外的请求统一经过登录态检查再进入业务分支。
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
        {ok, {join_map, MapId}} ->
            handle_authenticated_request(
                ?PROTO_MAP_JOIN_REQUEST, {join_map, MapId}, Socket);
        {ok, leave_map} ->
            handle_authenticated_request(
                ?PROTO_MAP_LEAVE_REQUEST, leave_map, Socket);
        {ok, {send_map, Content}} ->
            handle_authenticated_request(
                ?PROTO_MAP_CHAT_SEND_REQUEST, {send_map, Content}, Socket);
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
        _ ->
            handle_business_request(Request, Socket)
    end.

handle_business_request(list_channels, Socket) ->
    JoinedChannels = get(channel_ids),
    ChannelList = [
        {ChannelId,
         ChannelType,
         joined_value(maps:is_key(ChannelId, JoinedChannels)),
         ChannelName}
     || {ChannelId, ChannelType, ChannelName} <- channel_server:channels()],
    send_packet(Socket,
        chat_server_protocol:encode_channel_list_result(ChannelList));
handle_business_request({join_channel, ChannelId}, Socket) ->
    %% 频道进程确认成功后才更新 Role 的本地成员缓存。
    ProtocolResult = case channel_server:join(ChannelId, get(role_id), self()) of
        {ok, ChannelId} = Result ->
            put(channel_ids, maps:put(ChannelId, true, get(channel_ids))),
            Result;
        {error, Reason} ->
            {error, Reason, ChannelId}
    end,
    send_packet(Socket,
        chat_server_protocol:encode_channel_join_result(ProtocolResult));
handle_business_request({leave_channel, ChannelId}, Socket) ->
    ProtocolResult = case channel_server:leave(ChannelId, get(role_id)) of
        {ok, ChannelId} = Result ->
            put(channel_ids, maps:remove(ChannelId, get(channel_ids))),
            Result;
        {error, Reason} ->
            {error, Reason, ChannelId}
    end,
    send_packet(Socket,
        chat_server_protocol:encode_channel_leave_result(ProtocolResult));
handle_business_request({send_channel, ChannelId, Content}, Socket) ->
    Result = send_channel_message(
        ChannelId, get(role_id), get(role_name), Content),
    ProtocolResult = case Result of
        {ok, ChannelId} -> {ok, ChannelId};
        {error, Reason} -> {error, Reason, ChannelId}
    end,
    send_packet(Socket,
        chat_server_protocol:encode_channel_send_result(ProtocolResult));
handle_business_request({send_private, TargetRoleName, Content}, Socket) ->
    Result = send_private_message(
        TargetRoleName, get(role_id), get(role_name), Content),
    ProtocolResult = case Result of
        {ok, TargetRoleName} -> {ok, TargetRoleName};
        {error, target_offline} ->
            {error, target_offline, TargetRoleName}
    end,
    send_packet(Socket,
        chat_server_protocol:encode_private_send_result(ProtocolResult));
handle_business_request({move, Direction}, Socket) ->
    case get(map_id) of
        undefined ->
            send_packet(Socket,
                chat_server_protocol:encode_move_result({error, not_in_map}));
        _MapId ->
            case map_server:move(get(map_pid), self(), Direction) of
                ok -> ok;
                {error, map_unavailable} ->
                    send_packet(Socket,
                        chat_server_protocol:encode_move_result(
                            {error, map_unavailable, get(position)}))
            end
    end;
handle_business_request({teleport, Position}, Socket) ->
    case get(map_id) of
        undefined ->
            send_packet(Socket,
                chat_server_protocol:encode_teleport_result(
                    {error, not_in_map}));
        _MapId ->
            case map_server:teleport(get(map_pid), self(), Position) of
                ok -> ok;
                {error, map_unavailable} ->
                    send_packet(Socket,
                        chat_server_protocol:encode_teleport_result(
                            {error, map_unavailable, get(position)}))
            end
    end;
handle_business_request({send_nearby, Content}, Socket) ->
    case get(map_id) of
        undefined ->
            send_packet(Socket,
                chat_server_protocol:encode_nearby_send_result(
                    {error, not_in_map}));
        _MapId ->
            case map_server:send_nearby(
                     get(map_pid), self(), get(role_name), Content) of
                ok -> ok;
                {error, map_unavailable} -> {error, map_unavailable}
            end
    end;
handle_business_request({join_map, MapId}, Socket) ->
    SpawnPosition = map_server:random_position(),
    ProtocolResult = case get(map_id) of
        CurrentMapId when is_integer(CurrentMapId) ->
            {error, already_in_map, CurrentMapId};
        undefined ->
            case map_server:valid_map(MapId) of
                false ->
                    {error, invalid_map, MapId};
                true ->
                    MapPid = map_server:pid(MapId),
                    case map_server:join(
                             MapPid, get(role_id), self(), SpawnPosition) of
                        {ok, {MapId, Position}} ->
                            put(map_id, MapId),
                            put(map_pid, MapPid),
                            put(position, Position),
                            {ok, MapId, Position};
                        {error, {already_in_map, CurrentMapId}} ->
                            {error, already_in_map, CurrentMapId};
                        {error, map_unavailable} ->
                            {error, map_unavailable, MapId}
                    end
            end
    end,
    send_packet(Socket,
        chat_server_protocol:encode_map_join_result(ProtocolResult));
handle_business_request(leave_map, Socket) ->
    %% 地图进程确认清理完成后再擦除本地状态，失败时仍保留原归属。
    ProtocolResult = case get(map_id) of
        undefined ->
            {error, not_in_map};
        MapId ->
            case map_server:leave(get(map_pid), self()) of
                {ok, _MapId} = Result ->
                    erase(map_id),
                    erase(map_pid),
                    erase(position),
                    Result;
                {error, not_in_map} = Error ->
                    Error;
                {error, map_unavailable} ->
                    {error, map_unavailable, MapId}
            end
    end,
    send_packet(Socket,
        chat_server_protocol:encode_map_leave_result(ProtocolResult));
handle_business_request({send_map, Content}, Socket) ->
    case get(map_id) of
        undefined ->
            send_packet(Socket,
                chat_server_protocol:encode_map_chat_send_result(
                    {error, not_in_map}));
        MapId ->
            case map_server:send_map(
                     get(map_pid), self(), get(role_name), Content) of
                ok -> ok;
                {error, map_unavailable} ->
                    send_packet(Socket,
                        chat_server_protocol:encode_map_chat_send_result(
                            {error, map_unavailable, MapId}))
            end
    end.

joined_value(true) -> 1;
joined_value(false) -> 0.

handle_login(Socket, RoleName, Password) ->
    case get(role_id) of
        undefined ->
            case role_online_server:login(RoleName, Password) of
                {ok, RoleId} ->
                    complete_login(Socket, RoleId, RoleName);
                {error, Reason} ->
                    send_packet(Socket,
                        chat_server_protocol:encode_login_result({error, Reason}))
            end;
        _RoleId ->
            send_packet(Socket,
                chat_server_protocol:encode_login_result(
                    {error, already_online}))
    end.

complete_login(Socket, RoleId, RoleName) ->
    InitialPosition = map_server:random_position(),
    InitialMapId = map_server:default_map_id(),
    InitialMapPid = map_server:pid(InitialMapId),
    %% 登录初始化顺序：进入地图 -> 加入初始频道 -> 发布 Role 本地状态和成功响应。
    case map_server:join(
             InitialMapPid, RoleId, self(), InitialPosition) of
        {ok, {InitialMapId, InitialPosition}} ->
            case join_initial_channels(RoleId) of
                {ok, ChannelIds} ->
                    put(role_id, RoleId),
                    put(role_name, RoleName),
                    put(map_id, InitialMapId),
                    put(map_pid, InitialMapPid),
                    put(position, InitialPosition),
                    put(channel_ids, maps:from_list(
                        [{ChannelId, true} || ChannelId <- ChannelIds])),
                    send_packet(Socket,
                        chat_server_protocol:encode_login_result(
                            {ok, RoleId, InitialPosition, ChannelIds}));
                {error, _Reason} ->
                    %% 半初始化连接直接关闭，monitor 会清理已登记的在线与地图状态。
                    reject_unavailable_login(Socket)
            end;
        {error, _Reason} ->
            reject_unavailable_login(Socket)
    end.

reject_unavailable_login(Socket) ->
    case send_packet(Socket,
             chat_server_protocol:encode_login_result(
                 {error, service_unavailable})) of
        ok -> stop;
        Error -> Error
    end.

join_initial_channels(RoleId) ->
    PublicCount = rand:uniform(3),
    RandomizedPublicIds = [
        ChannelId
     || {_RandomKey, ChannelId} <-
            lists:sort([{rand:uniform(), Id} || Id <- lists:seq(2, 10)])
    ],
    ChannelIds = [1 | lists:sublist(RandomizedPublicIds, PublicCount)],
    case join_channels(ChannelIds, RoleId) of
        ok -> {ok, ChannelIds};
        Error -> Error
    end.

join_channels([], _RoleId) ->
    ok;
join_channels([ChannelId | Rest], RoleId) ->
    case channel_server:join(ChannelId, RoleId, self()) of
        {ok, ChannelId} -> join_channels(Rest, RoleId);
        {error, _Reason} = Error -> Error
    end.

do_rejoin_channel(ChannelId) ->
    case get(channel_ids) of
        #{ChannelId := true} ->
            _ = channel_server:join(ChannelId, get(role_id), self()),
            ok;
        _ ->
            ok
    end.

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

do_push_packets(_Socket, []) ->
    ok;
do_push_packets(Socket, [Packet | Packets]) ->
    case gen_tcp:send(Socket, Packet) of
        ok -> do_push_packets(Socket, Packets);
        {error, _Reason} -> ok
    end.

do_push_batch(Socket, Packet) ->
    _ = gen_tcp:send(Socket, Packet),
    ok.

do_push_private(Socket, SenderRoleId, SenderRoleName, Content) ->
    Packet = chat_server_protocol:encode_private_push(
        SenderRoleId, SenderRoleName, Content),
    _ = gen_tcp:send(Socket, Packet),
    ok.

do_map_result(Socket, MapPid, Operation, Result) ->
    Packet = case {get(map_pid), Operation, Result} of
        {MapPid, move, {ok, Position} = MoveResult} ->
            put(position, Position),
            chat_server_protocol:encode_move_result(MoveResult);
        {MapPid, move, {error, not_in_map} = MoveResult} ->
            clear_map_state(),
            chat_server_protocol:encode_move_result(MoveResult);
        {MapPid, move, MoveResult} ->
            chat_server_protocol:encode_move_result(MoveResult);
        {MapPid, teleport, {ok, Position} = TeleportResult} ->
            put(position, Position),
            chat_server_protocol:encode_teleport_result(TeleportResult);
        {MapPid, teleport, {error, not_in_map} = TeleportResult} ->
            clear_map_state(),
            chat_server_protocol:encode_teleport_result(TeleportResult);
        {MapPid, teleport, TeleportResult} ->
            chat_server_protocol:encode_teleport_result(TeleportResult);
        {MapPid, send_nearby, NearbyResult} ->
            chat_server_protocol:encode_nearby_send_result(NearbyResult);
        {MapPid, send_map, MapResult} ->
            chat_server_protocol:encode_map_chat_send_result(MapResult);
        {_CurrentMapPid, _Operation, _Result} ->
            ignored
    end,
    case Packet of
        ignored ->
            ok;
        _ ->
            _ = gen_tcp:send(Socket, Packet),
            ok
    end.

clear_map_state() ->
    erase(map_id),
    erase(map_pid),
    erase(position),
    ok.

send_packet(Socket, Packet) ->
    case gen_tcp:send(Socket, Packet) of
        ok -> ok;
        {error, Reason} -> {error, {tcp_send_failed, Reason}}
    end.
