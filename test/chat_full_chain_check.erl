-module(chat_full_chain_check).

-include("chat_protocol.hrl").
-include("chat_record.hrl").

-export([run/0]).

-define(TIMEOUT, 5000).

run() ->
    _ = application:load(chat),
    Port = free_port(),
    ok = application:set_env(chat, port, Port),
    ok = application:start(chat),
    try
        check(Port)
    after
        application:stop(chat)
    end,
    io:format("full_chain_check: ok~n").

check(Port) ->
    {Alice, AliceId, AliceChannels} = login(Port, <<"alice">>, <<"pw">>),
    {Bob, BobId, _BobChannels} = login(Port, <<"bob">>, <<"pw">>),
    check_initial_channels(AliceChannels),
    check_login_errors(Port),
    check_protocol_errors(Port),
    check_nearby_batch_worker(),
    check_nearby_batch_metrics(),
    check_world_member_shards(2),
    BobRolePid = check_map_flow(Alice, AliceId, Bob, BobId),
    check_multi_map_flow(Alice, AliceId, Bob, BobId),
    check_map_channel_unavailable(Port, Alice, AliceId, Bob, BobId),
    check_map_channel_recovery(Alice, AliceId, Bob, BobId),
    check_map_channel_timeout(Bob),
    check_map_worker_recovery(Alice, AliceId, Bob, BobId),
    check_map_batch_boundaries(Port, Alice, AliceId, Bob),
    check_map_leave_does_not_wait_for_broadcast_worker(Port, Alice, AliceId, Bob),
    check_map_broadcast_worker_recovery(Alice, AliceId, Bob),
    check_relocate_waits_for_map_worker(Alice),
    check_pending_join_recovery(),
    check_map_worker_timeout(),
    check_channel_flow(Alice, AliceId, AliceChannels, Bob, BobId),
    check_channel_batch_boundaries(Alice, AliceId, Bob),
    check_channel_full_batch(Alice, AliceId),
    check_channel_timeout(Alice, AliceId, Bob, BobId),
    check_public_channel_unavailable(Alice, AliceId, Bob, BobId),
    check_public_channel_recovery(Alice, AliceId, Bob, BobId),
    check_role_packet_batch(Alice, AliceId),
    check_world_batch_order(Alice, AliceId, Bob),
    check_socket_writer_broadcast(Alice, AliceId, Bob),
    check_private_flow(Alice, AliceId, Bob, BobId),
    check_worker_recovery(Alice, AliceId),
    check_nearby_worker_recovery(Alice, AliceId),
    check_metrics(),
    gen_tcp:close(Bob),
    wait_until(fun() -> ets:info(online_roles, size) =:= 1 end),
    wait_until(fun() -> world_member_count() =:= 1 end),
    check_map_cleanup(BobRolePid),
    check_world_member_shards(1),
    send(Alice, chat_client_protocol:encode_private_send(
        <<"bob">>, <<"after-close">>)),
    {private_send_result, {error, target_offline, <<"bob">>}} = recv(Alice),
    check_real_client(Port),
    check_main_restart(Alice),
    check_map_restart(Port),
    check_restart_accepts_login(Port),
    ok.

check_initial_channels(ChannelIds) ->
    PublicIds = lists:delete(1, ChannelIds),
    true = lists:member(1, ChannelIds),
    true = length(PublicIds) >= 1 andalso length(PublicIds) =< 3,
    true = length(PublicIds) =:= length(lists:usort(PublicIds)),
    true = lists:all(fun(Id) -> Id >= 2 andalso Id =< 10 end, PublicIds).

check_login_errors(Port) ->
    WrongPassword = connect(Port),
    send(WrongPassword, chat_client_protocol:encode_login(
        <<"alice">>, <<"wrong">>)),
    {login_result, {error, invalid_login}} = recv(WrongPassword),
    gen_tcp:close(WrongPassword),

    Duplicate = connect(Port),
    send(Duplicate, chat_client_protocol:encode_login(
        <<"alice">>, <<"pw">>)),
    {login_result, {error, already_online}} = recv(Duplicate),
    gen_tcp:close(Duplicate).

check_protocol_errors(Port) ->
    Socket = connect(Port),
    send(Socket, chat_client_protocol:encode_channel_list()),
    {server_error, ?PROTO_CHANNEL_LIST_REQUEST, not_logged_in} = recv(Socket),
    send(Socket, chat_client_protocol:encode_move(down)),
    {server_error, ?PROTO_MAP_MOVE_REQUEST, not_logged_in} = recv(Socket),
    send(Socket, chat_client_protocol:encode_map_join(2)),
    {server_error, ?PROTO_MAP_JOIN_REQUEST, not_logged_in} = recv(Socket),
    send(Socket, <<?PROTO_MAP_TELEPORT_REQUEST:16, 1:8>>),
    {server_error, ?PROTO_MAP_TELEPORT_REQUEST, invalid_packet} = recv(Socket),
    send(Socket, <<?PROTO_CHANNEL_JOIN_REQUEST:16, 1:8>>),
    {server_error, ?PROTO_CHANNEL_JOIN_REQUEST, invalid_packet} = recv(Socket),
    send(Socket, <<7777:16>>),
    {server_error, 7777, unknown_proto} = recv(Socket),
    gen_tcp:close(Socket).

check_map_flow(Alice, AliceId, Bob, BobId) ->
    AliceRolePid = role_pid(<<"alice">>),
    BobRolePid = role_pid(<<"bob">>),
    MapPid = whereis(map_router),
    MapPid = ets:info(map_cells_1, owner),
    MapPid = ets:info(map_role_positions, owner),
    bag = ets:info(map_cells_1, type),
    public = ets:info(map_cells_1, protection),
    {error, invalid_position} = map_router:join(
        999999, self(), 3, {-1, 0}),
    {error, not_in_map} = map_router:location(self()),
    {ok, AliceSpawn} = map_position(AliceRolePid),
    {ok, BobSpawn} = map_position(BobRolePid),
    true = map_router:valid_position(AliceSpawn),
    true = map_router:valid_position(BobSpawn),
    {ok, {1, AliceSpawn}} = map_location(AliceRolePid),
    {ok, {1, BobSpawn}} = map_location(BobRolePid),
    true = length(lists:usort(
        [map_router:random_position() || _ <- lists:seq(1, 20)])) > 1,
    teleport_to(Alice, {0, 0}),
    teleport_to(Bob, {0, 0}),
    2 = length(ets:lookup(map_cells_1, {1, 0, 0})),
    ok = sys:suspend(MapPid),
    try
        {ok, SuspendedTargets} = map_router:nearby({1, {0, 0}}),
        true = lists:keymember(AliceRolePid, 1, SuspendedTargets),
        true = lists:keymember(BobRolePid, 1, SuspendedTargets)
    after
        ok = sys:resume(MapPid)
    end,

    send(Alice, <<?PROTO_MAP_MOVE_REQUEST:16, 99:8>>),
    {move_result, {error, invalid_direction, {0, 0}}} = recv(Alice),
    send(Alice, chat_client_protocol:encode_move(up)),
    {move_result, {error, out_of_bounds, {0, 0}}} = recv(Alice),
    check_move(Alice, down, {1, 0}),
    check_move(Alice, right, {1, 1}),
    check_move(Alice, up, {0, 1}),
    check_move(Alice, left, {0, 0}),
    {ok, {0, 0}} = map_position(AliceRolePid),

    send(Bob, chat_client_protocol:encode_teleport(99, 99)),
    {teleport_result, {ok, {99, 99}}} = recv(Bob),
    send(Bob, chat_client_protocol:encode_teleport(100, 0)),
    {teleport_result, {error, invalid_position, {99, 99}}} = recv(Bob),
    {ok, {99, 99}} = map_position(BobRolePid),

    send(Alice, chat_client_protocol:encode_nearby_send(<<"corner">>)),
    {nearby_send_result, {ok, 1}} = recv(Alice),
    expect_nearby_push(Alice, AliceId, <<"alice">>, {0, 0}, <<"corner">>),
    {error, timeout} = gen_tcp:recv(Bob, 0, 100),

    send(Bob, chat_client_protocol:encode_teleport(1, 1)),
    {teleport_result, {ok, {1, 1}}} = recv(Bob),
    send(Alice, chat_client_protocol:encode_nearby_send(<<"near">>)),
    {nearby_send_result, {ok, 2}} = recv(Alice),
    expect_nearby_push(Alice, AliceId, <<"alice">>, {0, 0}, <<"near">>),
    expect_nearby_push(Bob, AliceId, <<"alice">>, {0, 0}, <<"near">>),

    send(Bob, chat_client_protocol:encode_teleport(2, 2)),
    {teleport_result, {ok, {2, 2}}} = recv(Bob),
    send(Alice, chat_client_protocol:encode_nearby_send(<<"far">>)),
    {nearby_send_result, {ok, 1}} = recv(Alice),
    expect_nearby_push(Alice, AliceId, <<"alice">>, {0, 0}, <<"far">>),
    {error, timeout} = gen_tcp:recv(Bob, 0, 100),

    send(Bob, chat_client_protocol:encode_teleport(0, 0)),
    {teleport_result, {ok, {0, 0}}} = recv(Bob),
    2 = length(ets:lookup(map_cells_1, {1, 0, 0})),
    true = BobId =/= AliceId,
    BobRolePid.

check_multi_map_flow(Alice, AliceId, Bob, BobId) ->
    BobRolePid = role_pid(<<"bob">>),
    send(Alice, chat_client_protocol:encode_map_join(99)),
    {map_join_result, {error, invalid_map, 99}} = recv(Alice),
    send(Alice, chat_client_protocol:encode_map_join(2)),
    {map_join_result, {error, already_in_map, 1}} = recv(Alice),

    send(Bob, chat_client_protocol:encode_map_leave()),
    {map_leave_result, {ok, 1}} = recv(Bob),
    {error, not_in_map} = map_router:location(BobRolePid),
    send(Bob, chat_client_protocol:encode_move(down)),
    {move_result, {error, not_in_map}} = recv(Bob),
    send(Bob, chat_client_protocol:encode_teleport(1, 1)),
    {teleport_result, {error, not_in_map}} = recv(Bob),
    send(Bob, chat_client_protocol:encode_nearby_send(<<"outside">>)),
    {nearby_send_result, {error, not_in_map}} = recv(Bob),
    send(Bob, chat_client_protocol:encode_map_chat_send(<<"outside">>)),
    {map_chat_send_result, {error, not_in_map}} = recv(Bob),
    send(Bob, chat_client_protocol:encode_map_leave()),
    {map_leave_result, {error, not_in_map}} = recv(Bob),

    send(Bob, chat_client_protocol:encode_map_join(2)),
    {map_join_result, {ok, 2, MapTwoSpawn}} = recv(Bob),
    true = map_router:valid_position(MapTwoSpawn),
    {ok, {2, MapTwoSpawn}} = map_location(BobRolePid),
    1 = length(ets:lookup(map_role_positions, BobRolePid)),
    send(Bob, chat_client_protocol:encode_map_join(1)),
    {map_join_result, {error, already_in_map, 2}} = recv(Bob),

    send(Alice, chat_client_protocol:encode_map_chat_send(<<"map-one">>)),
    {map_chat_send_result, {ok, 1}} = recv(Alice),
    expect_map_chat_push(Alice, 1, AliceId, <<"alice">>, <<"map-one">>),
    {error, timeout} = gen_tcp:recv(Bob, 0, 100),
    send(Bob, chat_client_protocol:encode_map_chat_send(<<"map-two">>)),
    {map_chat_send_result, {ok, 2}} = recv(Bob),
    expect_map_chat_push(Bob, 2, BobId, <<"bob">>, <<"map-two">>),
    {error, timeout} = gen_tcp:recv(Alice, 0, 100),

    send(Alice, chat_client_protocol:encode_nearby_send(<<"map-isolated">>)),
    {nearby_send_result, {ok, 1}} = recv(Alice),
    expect_nearby_push(
        Alice, AliceId, <<"alice">>, {0, 0}, <<"map-isolated">>),
    {error, timeout} = gen_tcp:recv(Bob, 0, 100),

    send(Bob, chat_client_protocol:encode_map_leave()),
    {map_leave_result, {ok, 2}} = recv(Bob),
    send(Bob, chat_client_protocol:encode_map_join(3)),
    {map_join_result, {ok, 3, MapThreeSpawn}} = recv(Bob),
    true = map_router:valid_position(MapThreeSpawn),
    {ok, {3, MapThreeSpawn}} = map_location(BobRolePid),
    send(Bob, chat_client_protocol:encode_map_chat_send(<<"map-three">>)),
    {map_chat_send_result, {ok, 3}} = recv(Bob),
    expect_map_chat_push(Bob, 3, BobId, <<"bob">>, <<"map-three">>),
    {error, timeout} = gen_tcp:recv(Alice, 0, 100),
    send(Bob, chat_client_protocol:encode_nearby_send(<<"map-three-nearby">>)),
    {nearby_send_result, {ok, 1}} = recv(Bob),
    expect_nearby_push(
        Bob, BobId, <<"bob">>, MapThreeSpawn, <<"map-three-nearby">>),
    {error, timeout} = gen_tcp:recv(Alice, 0, 100),
    send(Bob, chat_client_protocol:encode_map_leave()),
    {map_leave_result, {ok, 3}} = recv(Bob),
    send(Bob, chat_client_protocol:encode_map_join(1)),
    {map_join_result, {ok, 1, MapOneSpawn}} = recv(Bob),
    true = map_router:valid_position(MapOneSpawn),
    teleport_to(Bob, {0, 0}),
    send(Alice, chat_client_protocol:encode_map_chat_send(<<"together">>)),
    {map_chat_send_result, {ok, 1}} = recv(Alice),
    expect_map_chat_push(Alice, 1, AliceId, <<"alice">>, <<"together">>),
    expect_map_chat_push(Bob, 1, AliceId, <<"alice">>, <<"together">>),
    send(Alice, chat_client_protocol:encode_map_chat_send(<<"map-order-1">>)),
    send(Alice, chat_client_protocol:encode_map_chat_send(<<"map-order-2">>)),
    {map_chat_send_result, {ok, 1}} = recv(Alice),
    {map_chat_send_result, {ok, 1}} = recv(Alice),
    {map_chat_push_batch, AliceMapMessages} = recv(Alice),
    {map_chat_push_batch, BobMapMessages} = recv(Bob),
    [<<"map-order-1">>, <<"map-order-2">>] =
        map_message_contents(AliceMapMessages, 1, AliceId),
    [<<"map-order-1">>, <<"map-order-2">>] =
        map_message_contents(BobMapMessages, 1, AliceId),
    {ok, {1, {0, 0}}} = map_location(BobRolePid).

check_map_channel_unavailable(Port, Alice, AliceId, Bob, BobId) ->
    MapPid = whereis(map_router),
    AliceRolePid = role_pid(<<"alice">>),
    BobRolePid = role_pid(<<"bob">>),
    send(Bob, chat_client_protocol:encode_map_leave()),
    {map_leave_result, {ok, 1}} = recv(Bob),
    {error, not_in_map} = map_router:location(BobRolePid),

    ChildId = {map_channel_server, 1},
    ok = supervisor:terminate_child(channel_sup, ChildId),
    undefined = whereis(map_channel_server_1),
    lists:foreach(
        fun({MapId, ChannelName}) ->
            OtherChildId = {map_channel_server, MapId},
            ok = supervisor:terminate_child(channel_sup, OtherChildId),
            undefined = whereis(ChannelName)
        end,
        [{2, map_channel_server_2}, {3, map_channel_server_3}]),

    send(Alice, chat_client_protocol:encode_map_chat_send(<<"unavailable">>)),
    {map_chat_send_result, {error, map_unavailable, 1}} = recv(Alice),
    send(Alice, chat_client_protocol:encode_map_leave()),
    {map_leave_result, {error, map_unavailable, 1}} = recv(Alice),
    {ok, {1, {0, 0}}} = map_router:location(AliceRolePid),
    send(Bob, chat_client_protocol:encode_map_join(1)),
    {map_join_result, {error, map_unavailable, 1}} = recv(Bob),
    {error, not_in_map} = map_router:location(BobRolePid),

    FailedLogin = connect(Port),
    send(FailedLogin, chat_client_protocol:encode_login(
        <<"map_unavailable_login">>, <<"pw">>)),
    {login_result, {error, service_unavailable}} = recv(FailedLogin),
    wait_socket_closed(FailedLogin, 20),
    wait_until(fun() ->
        ets:lookup(online_roles, <<"map_unavailable_login">>) =:= []
    end),
    MapPid = whereis(map_router),
    2 = ets:info(online_roles, size),

    {ok, _ChannelPid} = supervisor:restart_child(channel_sup, ChildId),
    {ok, _MapTwoChannelPid} = supervisor:restart_child(
        channel_sup, {map_channel_server, 2}),
    {ok, _MapThreeChannelPid} = supervisor:restart_child(
        channel_sup, {map_channel_server, 3}),
    wait_until(fun() ->
        case catch sys:get_state(map_channel_server_1) of
            #channel_state{members = Members} ->
                maps:is_key(AliceId, Members) andalso
                not maps:is_key(BobId, Members);
            _ ->
                false
        end
    end),
    send(Bob, chat_client_protocol:encode_map_join(1)),
    {map_join_result, {ok, 1, Position}} = recv(Bob),
    true = map_router:valid_position(Position),
    teleport_to(Bob, {0, 0}),
    {ok, {1, {0, 0}}} = map_router:location(BobRolePid).

check_map_channel_recovery(Alice, AliceId, Bob, BobId) ->
    OldChannel = whereis(map_channel_server_1),
    exit(OldChannel, kill),
    wait_until(fun() -> changed(map_channel_server_1, OldChannel) end),
    wait_until(fun() ->
        case catch sys:get_state(map_channel_server_1) of
            #channel_state{members = Members} ->
                maps:is_key(AliceId, Members) andalso
                maps:is_key(BobId, Members);
            _ ->
                false
        end
    end),
    send(Alice, chat_client_protocol:encode_map_chat_send(
        <<"map-after-restart">>)),
    {map_chat_send_result, {ok, 1}} = recv(Alice),
    expect_map_chat_push(
        Alice, 1, AliceId, <<"alice">>, <<"map-after-restart">>),
    expect_map_chat_push(
        Bob, 1, AliceId, <<"alice">>, <<"map-after-restart">>).

check_map_channel_timeout(Bob) ->
    BobRolePid = role_pid(<<"bob">>),
    send(Bob, chat_client_protocol:encode_map_leave()),
    {map_leave_result, {ok, 1}} = recv(Bob),
    OldChannel = whereis(map_channel_server_1),
    ok = sys:suspend(OldChannel),
    send(Bob, chat_client_protocol:encode_map_join(1)),
    {map_join_result, {error, map_unavailable, 1}} = recv(Bob),
    {error, not_in_map} = map_router:location(BobRolePid),
    ok = sys:resume(OldChannel),
    OldChannel = whereis(map_channel_server_1),
    wait_until(fun() ->
        case catch sys:get_state(map_channel_server_1) of
            #channel_state{members = Members} ->
                not maps:is_key(role_id(<<"bob">>), Members);
            _ ->
                false
        end
    end),
    send(Bob, chat_client_protocol:encode_map_join(1)),
    {map_join_result, {ok, 1, Position}} = recv(Bob),
    true = map_router:valid_position(Position),
    teleport_to(Bob, {0, 0}),
    {ok, {1, {0, 0}}} = map_router:location(BobRolePid),
    wait_until(fun() ->
        case catch sys:get_state(map_channel_server_1) of
            #channel_state{members = Members} ->
                maps:is_key(role_id(<<"alice">>), Members) andalso
                maps:is_key(role_id(<<"bob">>), Members);
            _ ->
                false
        end
    end),
    true = is_process_alive(role_pid(<<"alice">>)),
    true = is_process_alive(role_pid(<<"bob">>)).

check_map_worker_recovery(Alice, AliceId, Bob, BobId) ->
    AliceRolePid = role_pid(<<"alice">>),
    BobRolePid = role_pid(<<"bob">>),
    Router = whereis(map_router),
    Channel = whereis(map_channel_server_1),
    OldWorker = whereis(map_worker_1),
    exit(OldWorker, kill),
    wait_until(fun() -> changed(map_worker_1, OldWorker) end),
    Router = whereis(map_router),
    Channel = whereis(map_channel_server_1),
    {ok, {1, {0, 0}}} = map_router:location(AliceRolePid),
    {ok, {1, {0, 0}}} = map_router:location(BobRolePid),
    ExpectedRolePids = lists:sort([AliceRolePid, BobRolePid]),
    ExpectedRolePids = lists:sort([
        RolePid || {{1, 0, 0}, {RolePid, _Writer}} <-
                       ets:lookup(map_cells_1, {1, 0, 0})]),
    send(Alice, chat_client_protocol:encode_map_chat_send(
        <<"map-worker-recovered">>)),
    {map_chat_send_result, {ok, 1}} = recv(Alice),
    expect_map_chat_push(
        Alice, 1, AliceId, <<"alice">>, <<"map-worker-recovered">>),
    expect_map_chat_push(
        Bob, 1, AliceId, <<"alice">>, <<"map-worker-recovered">>),
    true = AliceId =/= BobId.

check_map_batch_boundaries(Port, Alice, AliceId, Bob) ->
    Charlie = connect(Port),
    send(Charlie, chat_client_protocol:encode_login(
        <<"charlie">>, <<"pw">>)),
    {login_result, {ok, _CharlieId, CharlieMapId,
                    _Position, _Channels}} = recv(Charlie),
    send(Charlie, chat_client_protocol:encode_map_leave()),
    {map_leave_result, {ok, CharlieMapId}} = recv(Charlie),

    BeforeJoinPacket = chat_server_protocol:encode_map_chat_push(
        1, AliceId, <<"alice">>, <<"before-map-join">>),
    _ = sys:replace_state(
        map_channel_server_1,
        fun(#channel_state{packets = [], batch_size = 0} = State) ->
            State#channel_state{packets = [BeforeJoinPacket], batch_size = 1}
        end),
    send(Charlie, chat_client_protocol:encode_map_join(1)),
    {map_join_result, {ok, 1, JoinPosition}} = recv(Charlie),
    true = map_router:valid_position(JoinPosition),
    send(Alice, chat_client_protocol:encode_map_chat_send(
        <<"after-map-join">>)),
    {map_chat_send_result, {ok, 1}} = recv(Alice),
    {map_chat_push_batch, AliceMessages} = recv(Alice),
    {map_chat_push_batch, BobMessages} = recv(Bob),
    {map_chat_push_batch, CharlieMessages} = recv(Charlie),
    [<<"before-map-join">>, <<"after-map-join">>] =
        map_message_contents(AliceMessages, 1, AliceId),
    [<<"before-map-join">>, <<"after-map-join">>] =
        map_message_contents(BobMessages, 1, AliceId),
    [<<"after-map-join">>] =
        map_message_contents(CharlieMessages, 1, AliceId),

    send(Alice, chat_client_protocol:encode_map_chat_send(
        <<"before-map-leave">>)),
    {map_chat_send_result, {ok, 1}} = recv(Alice),
    send(Charlie, chat_client_protocol:encode_map_leave()),
    recv_map_leave(Charlie, AliceId),
    expect_map_chat_push(
        Alice, 1, AliceId, <<"alice">>, <<"before-map-leave">>),
    expect_map_chat_push(
        Bob, 1, AliceId, <<"alice">>, <<"before-map-leave">>),
    gen_tcp:close(Charlie).

recv_map_leave(Socket, SenderId) ->
    recv_map_leave(Socket, SenderId, false, false).

recv_map_leave(_Socket, _SenderId, true, true) ->
    ok;
recv_map_leave(Socket, SenderId, SawResult, SawBatch) ->
    case recv(Socket) of
        {map_leave_result, {ok, 1}} ->
            recv_map_leave(Socket, SenderId, true, SawBatch);
        {map_chat_push_batch, Messages} ->
            [<<"before-map-leave">>] =
                map_message_contents(Messages, 1, SenderId),
            recv_map_leave(Socket, SenderId, SawResult, true)
    end.

check_map_leave_does_not_wait_for_broadcast_worker(Port, Alice, AliceId, Bob) ->
    {Charlie, CharlieId, _Channels} = login(
        Port, <<"leave_barrier">>, <<"pw">>),
    Worker = worker_for(CharlieId),
    ok = sys:suspend(Worker),
    try
        send(Alice, chat_client_protocol:encode_map_chat_send(
            <<"leave-barrier-tail">>)),
        {map_chat_send_result, {ok, 1}} = recv(Alice),
        wait_until(fun() ->
            {message_queue_len, Length} = process_info(Worker, message_queue_len),
            Length > 0
        end),
        send(Charlie, chat_client_protocol:encode_map_leave()),
        {map_leave_result, {ok, 1}} = recv(Charlie, 1000),
        send(Alice, chat_client_protocol:encode_map_chat_send(
            <<"leave-barrier-after">>)),
        {map_chat_send_result, {ok, 1}} = recv(Alice)
    after
        ok = sys:resume(Worker)
    end,
    expect_map_chat_push(
        Alice, 1, AliceId, <<"alice">>, <<"leave-barrier-tail">>),
    expect_map_chat_push(
        Bob, 1, AliceId, <<"alice">>, <<"leave-barrier-tail">>),
    expect_map_chat_push(
        Alice, 1, AliceId, <<"alice">>, <<"leave-barrier-after">>),
    expect_map_chat_push(
        Bob, 1, AliceId, <<"alice">>, <<"leave-barrier-after">>),
    expect_map_chat_push(
        Charlie, 1, AliceId, <<"alice">>, <<"leave-barrier-tail">>),
    {error, timeout} = gen_tcp:recv(Charlie, 0, 300),
    send(Charlie, chat_client_protocol:encode_map_join(1)),
    {map_join_result, {ok, 1, JoinPosition}} = recv(Charlie),
    true = map_router:valid_position(JoinPosition),
    gen_tcp:close(Charlie).

worker_for(RoleId) ->
    WorkerIndex = erlang:phash2(
        RoleId, map_broadcast_worker:worker_count()) + 1,
    whereis(map_broadcast_worker_name(WorkerIndex)).

map_broadcast_worker_name(1) -> map_broadcast_worker_1_1;
map_broadcast_worker_name(2) -> map_broadcast_worker_1_2;
map_broadcast_worker_name(3) -> map_broadcast_worker_1_3;
map_broadcast_worker_name(4) -> map_broadcast_worker_1_4;
map_broadcast_worker_name(5) -> map_broadcast_worker_1_5;
map_broadcast_worker_name(6) -> map_broadcast_worker_1_6;
map_broadcast_worker_name(7) -> map_broadcast_worker_1_7;
map_broadcast_worker_name(8) -> map_broadcast_worker_1_8.

check_map_broadcast_worker_recovery(Alice, AliceId, Bob) ->
    WorkerIndex = erlang:phash2(
        AliceId, map_broadcast_worker:worker_count()) + 1,
    WorkerId = {map_broadcast_worker, 1, WorkerIndex},
    Channel = whereis(map_channel_server_1),
    ok = supervisor:terminate_child(channel_sup, WorkerId),
    send(Alice, chat_client_protocol:encode_map_chat_send(
        <<"map-worker-down">>)),
    {map_chat_send_result, {error, map_unavailable, 1}} = recv(Alice),
    send(Alice, chat_client_protocol:encode_map_leave()),
    {map_leave_result, {error, map_unavailable, 1}} = recv(Alice),
    Channel = whereis(map_channel_server_1),
    {ok, {1, _Position}} = map_router:location(role_pid(<<"alice">>)),

    {ok, _WorkerPid} = supervisor:restart_child(channel_sup, WorkerId),
    send(Alice, chat_client_protocol:encode_map_chat_send(
        <<"map-worker-up">>)),
    {map_chat_send_result, {ok, 1}} = recv(Alice),
    expect_map_chat_push(
        Alice, 1, AliceId, <<"alice">>, <<"map-worker-up">>),
    expect_map_chat_push(
        Bob, 1, AliceId, <<"alice">>, <<"map-worker-up">>).

check_relocate_waits_for_map_worker(Alice) ->
    RolePid = role_pid(<<"alice">>),
    Worker = whereis(map_worker_1),
    ok = sys:suspend(Worker),
    try
        send(Alice, chat_client_protocol:encode_teleport(3, 3)),
        {error, timeout} = gen_tcp:recv(Alice, 0, 100),
        {ok, {1, {0, 0}}} = map_router:location(RolePid),
        [] = ets:lookup(map_cells_1, {1, 3, 3})
    after
        ok = sys:resume(Worker)
    end,
    {teleport_result, {ok, {3, 3}}} = recv(Alice),
    {ok, {1, {3, 3}}} = map_router:location(RolePid),
    [{{1, 3, 3}, {RolePid, _Writer}}] =
        ets:lookup(map_cells_1, {1, 3, 3}),
    false = lists:member(
        RolePid,
        [Pid || {{1, 0, 0}, {Pid, _}} <-
                    ets:lookup(map_cells_1, {1, 0, 0})]),
    teleport_to(Alice, {0, 0}).

check_map_worker_timeout() ->
    Worker = whereis(map_worker_3),
    RolePid = spawn(fun() -> receive stop -> ok end end),
    RoleId = 999999,
    ok = sys:suspend(Worker),
    _ = spawn(fun() ->
        timer:sleep(4100),
        ok = sys:resume(Worker)
    end),
    {error, map_unavailable} = map_router:join(
        RoleId, RolePid, 3, {0, 0}),
    {error, not_in_map} = map_router:location(RolePid),
    [] = ets:lookup(map_cells_3, {3, 0, 0}),
    #channel_state{members = Members} = sys:get_state(map_channel_server_3),
    false = maps:is_key(RoleId, Members),
    RolePid ! stop.

check_pending_join_recovery() ->
    Worker = whereis(map_worker_3),
    RolePid = spawn(fun() -> receive stop -> ok end end),
    RoleId = 999998,
    true = ets:insert(
        map_role_positions, {RolePid, {pending, 3, {0, 0}, RoleId}}),
    {ok, {map, 3}} = channel_server:join_map(3, RoleId, RolePid),
    exit(Worker, kill),
    wait_until(fun() -> changed(map_worker_3, Worker) end),
    {error, not_in_map} = map_router:location(RolePid),
    [] = ets:lookup(map_role_positions, RolePid),
    #channel_state{members = Members} = sys:get_state(map_channel_server_3),
    false = maps:is_key(RoleId, Members),
    RolePid ! stop.

check_move(Socket, Direction, Position) ->
    send(Socket, chat_client_protocol:encode_move(Direction)),
    {move_result, {ok, Position}} = recv(Socket).

teleport_to(Socket, {X, Y} = Position) ->
    send(Socket, chat_client_protocol:encode_teleport(X, Y)),
    {teleport_result, {ok, Position}} = recv(Socket).

check_map_cleanup(BobRolePid) ->
    wait_until(fun() -> map_position(BobRolePid) =:= error end),
    AliceRolePid = role_pid(<<"alice">>),
    {ok, [{AliceRolePid, _AliceWriter}]} = map_router:nearby({1, {0, 0}}),
    [{{1, 0, 0}, {AliceRolePid, _}}] =
        ets:lookup(map_cells_1, {1, 0, 0}),
    ok.

check_channel_flow(Alice, AliceId, AliceChannels, Bob, BobId) ->
    send(Alice, chat_client_protocol:encode_channel_list()),
    {channel_list_result, {ok, Channels}} = recv(Alice),
    10 = length(Channels),

    NewChannel = hd(lists:seq(2, 10) -- AliceChannels),
    send(Alice, chat_client_protocol:encode_channel_join(NewChannel)),
    {channel_join_result, {ok, NewChannel}} = recv(Alice),
    send(Alice, chat_client_protocol:encode_channel_leave(NewChannel)),
    {channel_leave_result, {ok, NewChannel}} = recv(Alice),
    send(Alice, chat_client_protocol:encode_channel_send(
        NewChannel, <<"not-joined">>)),
    {channel_send_result, {error, not_joined, NewChannel}} = recv(Alice),

    send(Alice, chat_client_protocol:encode_channel_leave(1)),
    {channel_leave_result, {error, cannot_leave_main, 1}} = recv(Alice),
    send(Alice, chat_client_protocol:encode_channel_join(999)),
    {channel_join_result, {error, invalid_channel, 999}} = recv(Alice),
    send(Alice, chat_client_protocol:encode_channel_send(999, <<"bad">>)),
    {channel_send_result, {error, invalid_channel, 999}} = recv(Alice),

    ensure_joined(Alice, 2),
    ensure_joined(Bob, 2),
    send(Alice, chat_client_protocol:encode_channel_join(2)),
    {channel_join_result, {error, already_joined, 2}} = recv(Alice),
    send(Alice, chat_client_protocol:encode_channel_send(2, <<"public">>)),
    {channel_send_result, {ok, 2}} = recv(Alice),
    expect_channel_push(Alice, 2, AliceId, <<"alice">>, <<"public">>),
    expect_channel_push(Bob, 2, AliceId, <<"alice">>, <<"public">>),

    send(Bob, chat_client_protocol:encode_channel_leave(2)),
    {channel_leave_result, {ok, 2}} = recv(Bob),
    send(Bob, chat_client_protocol:encode_channel_send(2, <<"blocked">>)),
    {channel_send_result, {error, not_joined, 2}} = recv(Bob),
    {error, timeout} = gen_tcp:recv(Alice, 0, 100),

    send(Alice, chat_client_protocol:encode_channel_send(1, <<"world">>)),
    {channel_send_result, {ok, 1}} = recv(Alice),
    expect_channel_push(Alice, 1, AliceId, <<"alice">>, <<"world">>),
    expect_channel_push(Bob, 1, AliceId, <<"alice">>, <<"world">>),
    true = BobId =/= AliceId.

check_channel_batch_boundaries(Alice, AliceId, Bob) ->
    send(Alice, chat_client_protocol:encode_channel_send(
        2, <<"before-join">>)),
    {channel_send_result, {ok, 2}} = recv(Alice),
    send(Bob, chat_client_protocol:encode_channel_join(2)),
    {channel_join_result, {ok, 2}} = recv(Bob),
    expect_channel_push(
        Alice, 2, AliceId, <<"alice">>, <<"before-join">>),
    {error, timeout} = gen_tcp:recv(Bob, 0, 100),

    send(Alice, chat_client_protocol:encode_channel_send(2, <<"order-1">>)),
    send(Alice, chat_client_protocol:encode_channel_send(2, <<"order-2">>)),
    {channel_send_result, {ok, 2}} = recv(Alice),
    {channel_send_result, {ok, 2}} = recv(Alice),
    {channel_push_batch, AliceMessages} = recv(Alice),
    {channel_push_batch, BobMessages} = recv(Bob),
    [<<"order-1">>, <<"order-2">>] =
        channel_message_contents(AliceMessages, 2, AliceId),
    [<<"order-1">>, <<"order-2">>] =
        channel_message_contents(BobMessages, 2, AliceId),

    send(Alice, chat_client_protocol:encode_channel_send(
        2, <<"before-leave">>)),
    {channel_send_result, {ok, 2}} = recv(Alice),
    send(Bob, chat_client_protocol:encode_channel_leave(2)),
    recv_channel_leave(Bob, 2, AliceId),
    expect_channel_push(
        Alice, 2, AliceId, <<"alice">>, <<"before-leave">>),

    send(Alice, chat_client_protocol:encode_channel_send(
        2, <<"after-leave">>)),
    {channel_send_result, {ok, 2}} = recv(Alice),
    expect_channel_push(
        Alice, 2, AliceId, <<"alice">>, <<"after-leave">>),
    {error, timeout} = gen_tcp:recv(Bob, 0, 100).

recv_channel_leave(Socket, ChannelId, SenderId) ->
    recv_channel_leave(Socket, ChannelId, SenderId, false, false).

recv_channel_leave(_Socket, _ChannelId, _SenderId, true, true) ->
    ok;
recv_channel_leave(Socket, ChannelId, SenderId, SawResult, SawBatch) ->
    case recv(Socket) of
        {channel_leave_result, {ok, ChannelId}} ->
            recv_channel_leave(Socket, ChannelId, SenderId, true, SawBatch);
        {channel_push_batch, Messages} ->
            [<<"before-leave">>] =
                channel_message_contents(Messages, ChannelId, SenderId),
            recv_channel_leave(Socket, ChannelId, SenderId, SawResult, true)
    end.

check_channel_full_batch(Alice, AliceId) ->
    PrefixPacket = chat_server_protocol:encode_channel_push(
        2, AliceId, <<"alice">>, <<"full-prefix">>),
    _ = sys:replace_state(
        public_channel_server_1,
        fun(State) ->
            State#channel_state{
                packets = lists:duplicate(255, PrefixPacket),
                batch_size = 255,
                flush_ref = undefined
            }
        end),
    {ok, 2} = channel_server:send_channel(
        2, AliceId, <<"alice">>, <<"full-last">>),
    {channel_push_batch, Messages} = recv(Alice),
    256 = length(Messages),
    #{content := <<"full-last">>} = lists:last(Messages).

check_channel_timeout(Alice, AliceId, Bob, BobId) ->
    OldChannel = whereis(public_channel_server_1),
    ok = sys:suspend(OldChannel),
    send(Bob, chat_client_protocol:encode_channel_join(2)),
    {channel_join_result, {error, channel_unavailable, 2}} =
        recv(Bob, ?TIMEOUT * 2),
    ok = sys:resume(OldChannel),
    OldChannel = whereis(public_channel_server_1),
    wait_until(fun() ->
        case catch sys:get_state(public_channel_server_1) of
            #channel_state{members = Members} ->
                maps:is_key(AliceId, Members) andalso
                not maps:is_key(BobId, Members);
            _ ->
                false
        end
    end),
    true = is_process_alive(role_pid(<<"alice">>)),
    true = is_process_alive(role_pid(<<"bob">>)),
    send(Alice, chat_client_protocol:encode_channel_send(
        2, <<"after-timeout">>)),
    {channel_send_result, {ok, 2}} = recv(Alice),
    expect_channel_push(
        Alice, 2, AliceId, <<"alice">>, <<"after-timeout">>),
    {error, timeout} = gen_tcp:recv(Bob, 0, 100).

check_public_channel_unavailable(Alice, AliceId, Bob, BobId) ->
    AliceRolePid = role_pid(<<"alice">>),
    BobRolePid = role_pid(<<"bob">>),
    ChildId = {channel_server, 2},
    ok = supervisor:terminate_child(channel_sup, ChildId),
    undefined = whereis(public_channel_server_1),

    send(Alice, chat_client_protocol:encode_channel_send(
        2, <<"unavailable">>)),
    {channel_send_result, {error, channel_unavailable, 2}} = recv(Alice),
    send(Alice, chat_client_protocol:encode_channel_leave(2)),
    {channel_leave_result, {error, channel_unavailable, 2}} = recv(Alice),
    send(Bob, chat_client_protocol:encode_channel_join(2)),
    {channel_join_result, {error, channel_unavailable, 2}} = recv(Bob),
    true = is_process_alive(AliceRolePid),
    true = is_process_alive(BobRolePid),

    {ok, _ChannelPid} = supervisor:restart_child(channel_sup, ChildId),
    wait_until(fun() ->
        case catch sys:get_state(public_channel_server_1) of
            #channel_state{members = Members} ->
                maps:is_key(AliceId, Members) andalso
                not maps:is_key(BobId, Members);
            _ ->
                false
        end
    end).

check_public_channel_recovery(Alice, AliceId, Bob, BobId) ->
    OldChannel = whereis(public_channel_server_1),
    exit(OldChannel, kill),
    wait_until(fun() -> changed(public_channel_server_1, OldChannel) end),
    wait_until(fun() ->
        case catch sys:get_state(public_channel_server_1) of
            #channel_state{members = Members} ->
                maps:is_key(AliceId, Members) andalso
                not maps:is_key(BobId, Members);
            _ ->
                false
        end
    end),
    send(Alice, chat_client_protocol:encode_channel_send(
        2, <<"public-after-restart">>)),
    {channel_send_result, {ok, 2}} = recv(Alice),
    expect_channel_push(
        Alice, 2, AliceId, <<"alice">>, <<"public-after-restart">>),
    {error, timeout} = gen_tcp:recv(Bob, 0, 100).

check_private_flow(Alice, AliceId, Bob, _BobId) ->
    send(Alice, chat_client_protocol:encode_private_send(
        <<"bob">>, <<"private">>)),
    {private_send_result, {ok, <<"bob">>}} = recv(Alice),
    {private_push, #{sender_role_id := AliceId,
                     sender_role_name := <<"alice">>,
                     content := <<"private">>}} = recv(Bob),
    send(Alice, chat_client_protocol:encode_private_send(
        <<"missing">>, <<"private">>)),
    {private_send_result,
     {error, target_offline, <<"missing">>}} = recv(Alice).

check_role_packet_batch(Alice, AliceId) ->
    [#online_role{role_pid = AliceRolePid}] =
        ets:lookup(online_roles, <<"alice">>),
    Packets = [
        chat_server_protocol:encode_channel_push(
            1, AliceId, <<"alice">>, <<"batch-1">>),
        chat_server_protocol:encode_channel_push(
            1, AliceId, <<"alice">>, <<"batch-2">>)
    ],
    BatchPacket = chat_server_protocol:encode_channel_push_batch(Packets),
    gen_server:cast(AliceRolePid, {push_batch, BatchPacket}),
    {channel_push_batch,
     [#{channel_id := 1,
        sender_role_id := AliceId,
        sender_role_name := <<"alice">>,
        content := <<"batch-1">>},
      #{channel_id := 1,
        sender_role_id := AliceId,
        sender_role_name := <<"alice">>,
        content := <<"batch-2">>}]} = recv(Alice),
    {error, timeout} = gen_tcp:recv(Alice, 0, 100),
    {error, invalid_packet} = chat_client_protocol:decode_packet(
        <<?PROTO_CHANNEL_PUSH_BATCH:16, 1:16, 8:32, 1:8>>),
    MapPackets = [
        chat_server_protocol:encode_map_chat_push(
            1, AliceId, <<"alice">>, <<"map-batch-1">>),
        chat_server_protocol:encode_map_chat_push(
            1, AliceId, <<"alice">>, <<"map-batch-2">>)
    ],
    {ok, {map_chat_push_batch, [_, _]}} =
        chat_client_protocol:decode_packet(
            chat_server_protocol:encode_map_chat_push_batch(MapPackets)),
    {error, invalid_packet} = chat_client_protocol:decode_packet(
        chat_server_protocol:encode_map_chat_push_batch(Packets)),
    {error, invalid_packet} = chat_client_protocol:decode_packet(
        <<?PROTO_MAP_CHAT_PUSH_BATCH:16, 1:16, 8:32, 1:8>>),
    NearbyPackets = [
        chat_server_protocol:encode_nearby_push(
            AliceId, <<"alice">>, 1, 2, <<"nearby-batch">>)
    ],
    {ok, {nearby_push_batch, [_]}} =
        chat_client_protocol:decode_packet(
            chat_server_protocol:encode_nearby_push_batch(NearbyPackets)),
    {error, invalid_packet} = chat_client_protocol:decode_packet(
        chat_server_protocol:encode_nearby_push_batch(Packets)),
    {error, invalid_packet} = chat_client_protocol:decode_packet(
        <<?PROTO_NEARBY_PUSH_BATCH:16, 1:16, 8:32, 1:8>>),
    check_compressed_batch_protocol(AliceId).

check_compressed_batch_protocol(AliceId) ->
    ChannelPackets = lists:duplicate(
        40,
        chat_server_protocol:encode_channel_push(
            1, AliceId, <<"alice">>, <<"compressed-channel">>)),
    <<?PROTO_COMPRESSED_PUSH_BATCH:16, _/binary>> =
        CompressedChannel =
            chat_server_protocol:encode_channel_push_batch(ChannelPackets),
    {ok, {channel_push_batch, ChannelMessages}} =
        chat_client_protocol:decode_packet(CompressedChannel),
    40 = length(ChannelMessages),

    MapPackets = lists:duplicate(
        40,
        chat_server_protocol:encode_map_chat_push(
            1, AliceId, <<"alice">>, <<"compressed-map">>)),
    <<?PROTO_COMPRESSED_PUSH_BATCH:16, _/binary>> =
        CompressedMap =
            chat_server_protocol:encode_map_chat_push_batch(MapPackets),
    {ok, {map_chat_push_batch, MapMessages}} =
        chat_client_protocol:decode_packet(CompressedMap),
    40 = length(MapMessages),

    SmallPacket = chat_server_protocol:encode_channel_push(
        1, AliceId, <<>>, <<>>),
    <<?PROTO_CHANNEL_PUSH_BATCH:16, _/binary>> =
        chat_server_protocol:encode_channel_push_batch([SmallPacket]),

    RawChannel = raw_batch(?PROTO_CHANNEL_PUSH_BATCH, ChannelPackets),
    RawNearby = raw_batch(
        ?PROTO_NEARBY_PUSH_BATCH,
        [chat_server_protocol:encode_nearby_push(
             AliceId, <<"alice">>, 1, 2, <<"compressed-nearby">>)]),
    {ok, {nearby_push_batch, [_]}} =
        chat_client_protocol:decode_packet(compressed_batch(RawNearby)),

    <<?PROTO_COMPRESSED_PUSH_BATCH:16, RawLength:32,
      CompressedLength:32, CompressedData/binary>> =
        compressed_batch(RawChannel),
    {error, invalid_packet} = chat_client_protocol:decode_packet(
        <<?PROTO_COMPRESSED_PUSH_BATCH:16, (RawLength + 1):32,
          CompressedLength:32, CompressedData/binary>>),
    {error, invalid_packet} = chat_client_protocol:decode_packet(
        <<?PROTO_COMPRESSED_PUSH_BATCH:16, RawLength:32,
          (CompressedLength + 1):32, CompressedData/binary>>),
    Truncated = binary:part(CompressedData, 0, CompressedLength - 1),
    {error, invalid_packet} = chat_client_protocol:decode_packet(
        <<?PROTO_COMPRESSED_PUSH_BATCH:16, RawLength:32,
          (byte_size(Truncated)):32, Truncated/binary>>),
    WithTrailingByte = <<CompressedData/binary, 0>>,
    {error, invalid_packet} = chat_client_protocol:decode_packet(
        <<?PROTO_COMPRESSED_PUSH_BATCH:16, RawLength:32,
          (byte_size(WithTrailingByte)):32, WithTrailingByte/binary>>),
    Damaged = <<0, CompressedData/binary>>,
    {error, invalid_packet} = chat_client_protocol:decode_packet(
        <<?PROTO_COMPRESSED_PUSH_BATCH:16, RawLength:32,
          (byte_size(Damaged)):32, Damaged/binary>>),
    {error, invalid_packet} = chat_client_protocol:decode_packet(
        compressed_batch(compressed_batch(RawChannel))),
    {error, invalid_packet} = chat_client_protocol:decode_packet(
        compressed_batch(chat_server_protocol:encode_private_push(
            AliceId, <<"alice">>, <<"private">>))),
    {error, invalid_packet} = chat_client_protocol:decode_packet(
        raw_batch(?PROTO_CHANNEL_PUSH_BATCH,
                  lists:duplicate(?MAX_BATCH_MESSAGES + 1, SmallPacket))),
    Bomb = zlib:compress(binary:copy(<<0>>, ?MAX_BATCH_BYTES + 1)),
    {error, invalid_packet} = chat_client_protocol:decode_packet(
        <<?PROTO_COMPRESSED_PUSH_BATCH:16, ?MAX_BATCH_BYTES:32,
          (byte_size(Bomb)):32, Bomb/binary>>),
    {error, invalid_packet} = chat_client_protocol:decode_packet(
        <<?PROTO_COMPRESSED_PUSH_BATCH:16,
          (?MAX_BATCH_BYTES + 1):32, 1:32, 0>>).

raw_batch(ProtoId, Packets) ->
    iolist_to_binary([
        <<ProtoId:16, (length(Packets)):16>>,
        [<<(byte_size(Packet)):32, Packet/binary>> || Packet <- Packets]
    ]).

compressed_batch(Packet) ->
    Compressed = zlib:compress(Packet),
    <<?PROTO_COMPRESSED_PUSH_BATCH:16, (byte_size(Packet)):32,
      (byte_size(Compressed)):32, Compressed/binary>>.

check_nearby_batch_worker() ->
    Packet1 = chat_server_protocol:encode_nearby_push(
        9001, <<"batcher">>, 99, 98, <<"one">>),
    Packet2 = chat_server_protocol:encode_nearby_push(
        9001, <<"batcher">>, 99, 98, <<"two">>),
    ok = nearby_broadcast_worker:send(1, {99, 98}, Packet1, [self()]),
    ok = nearby_broadcast_worker:send(1, {99, 98}, Packet2, [self()]),
    receive
        {'$gen_cast', {push_batch, BatchPacket}} ->
            {ok, {nearby_push_batch, Messages}} =
                chat_client_protocol:decode_packet(BatchPacket),
            [<<"one">>, <<"two">>] =
                [Content || #{content := Content} <- Messages]
    after ?TIMEOUT ->
        error(nearby_batch_timeout)
    end,
    receive
        {'$gen_cast', {push_batch, _UnexpectedPacket}} ->
            error(nearby_batch_split)
    after 100 ->
        ok
    end.

check_nearby_batch_metrics() ->
    Before = nearby_batch_metrics(1),
    Packet = chat_server_protocol:encode_nearby_push(
        9002, <<"metrics">>, 0, 0, <<"batch">>),
    ok = nearby_broadcast_worker:send(1, {0, 0}, Packet, [self()]),
    ok = nearby_broadcast_worker:send(1, {1, 0}, Packet, [self()]),
    receive {'$gen_cast', {push_batch, _}} -> ok after ?TIMEOUT ->
        error(nearby_metrics_timeout)
    end,
    receive {'$gen_cast', {push_batch, _}} -> ok after ?TIMEOUT ->
        error(nearby_metrics_timeout)
    end,
    {Messages, Targets, Flushes, BatchMessages, BatchMax} =
        nearby_batch_metrics(1),
    {BeforeMessages, BeforeTargets, BeforeFlushes,
     BeforeBatchMessages, BeforeBatchMax} = Before,
    true = Messages =:= BeforeMessages + 2,
    true = Targets =:= BeforeTargets + 2,
    true = Flushes =:= BeforeFlushes + 2,
    true = BatchMessages =:= BeforeBatchMessages + 2,
    true = BatchMax =:= erlang:max(BeforeBatchMax, 1).

nearby_batch_metrics(MapId) ->
    lists:foldl(
        fun({{_MetricMapId, _WorkerIndex}, Messages, Targets, Flushes,
             BatchMessages, BatchMax},
            {MessageTotal, TargetTotal, FlushTotal,
             BatchMessageTotal, BatchMaxTotal}) ->
            {MessageTotal + Messages, TargetTotal + Targets,
             FlushTotal + Flushes, BatchMessageTotal + BatchMessages,
             erlang:max(BatchMaxTotal, BatchMax)}
        end,
        {0, 0, 0, 0, 0},
        ets:match_object(nearby_batch_metrics,
                         {{MapId, '_'}, '_', '_', '_', '_', '_'})).

check_world_batch_order(Alice, AliceId, Bob) ->
    send(Alice, chat_client_protocol:encode_channel_send(1, <<"order-1">>)),
    send(Alice, chat_client_protocol:encode_channel_send(1, <<"order-2">>)),
    [<<"order-1">>, <<"order-2">>] =
        recv_world_contents(Alice, AliceId, 2, 2),
    [<<"order-1">>, <<"order-2">>] =
        recv_world_contents(Bob, AliceId, 2, 0),
    {error, timeout} = gen_tcp:recv(Alice, 0, 100),
    {error, timeout} = gen_tcp:recv(Bob, 0, 100).

check_socket_writer_broadcast(Alice, AliceId, Bob) ->
    BobRolePid = role_pid(<<"bob">>),
    {BobServerSocket, BobWriter} = world_member_connection(BobRolePid),
    #channel_state{members = MapMembers} =
        sys:get_state(map_channel_server_1),
    #channel_member{role_pid = BobRolePid,
                    socket = BobServerSocket,
                    writer = BobWriter} =
        maps:get(role_id(<<"bob">>), MapMembers),
    true = erlang:suspend_process(BobWriter),
    try
        send(Alice, chat_client_protocol:encode_channel_send(
            1, <<"direct-world">>)),
        {channel_send_result, {ok, 1}} = recv(Alice),
        expect_channel_push(
            Alice, 1, AliceId, <<"alice">>, <<"direct-world">>),
        {error, timeout} = gen_tcp:recv(Bob, 0, 100),

        send(Alice, chat_client_protocol:encode_map_chat_send(
            <<"direct-map">>)),
        {map_chat_send_result, {ok, 1}} = recv(Alice),
        expect_map_chat_push(
            Alice, 1, AliceId, <<"alice">>, <<"direct-map">>),
        {message_queue_len, 0} =
            process_info(BobRolePid, message_queue_len),
        {message_queue_len, 2} =
            process_info(BobWriter, message_queue_len)
    after
        true = erlang:resume_process(BobWriter)
    end,
    expect_channel_push(
        Bob, 1, AliceId, <<"alice">>, <<"direct-world">>),
    expect_map_chat_push(
        Bob, 1, AliceId, <<"alice">>, <<"direct-map">>).

world_member_connection(RolePid) ->
    [#world_channel_member{role_pid = RolePid,
                           socket = Socket,
                           writer = Writer}] =
        [Member
         || Table <- channel_server:world_member_tables(),
            #world_channel_member{role_pid = MemberPid} = Member <-
                ets:tab2list(Table),
            MemberPid =:= RolePid],
    {Socket, Writer}.

check_worker_recovery(Alice, AliceId) ->
    WorkerIndex = erlang:phash2(
        AliceId, world_broadcast_worker:worker_count()) + 1,
    WorkerId = {world_broadcast_worker, WorkerIndex},
    ok = supervisor:terminate_child(channel_sup, WorkerId),
    send(Alice, chat_client_protocol:encode_channel_send(1, <<"worker-down">>)),
    {channel_send_result, {error, broadcast_failed, 1}} = recv(Alice),
    {ok, _WorkerPid} = supervisor:restart_child(channel_sup, WorkerId),
    send(Alice, chat_client_protocol:encode_channel_send(1, <<"worker-up">>)),
    {channel_send_result, {ok, 1}} = recv(Alice),
    expect_channel_push(Alice, 1, AliceId, <<"alice">>, <<"worker-up">>).

check_nearby_worker_recovery(Alice, AliceId) ->
    OldWorker = whereis(nearby_broadcast_worker_1),
    exit(OldWorker, kill),
    wait_until(fun() -> changed(nearby_broadcast_worker_1, OldWorker) end),
    send(Alice, chat_client_protocol:encode_nearby_send(
        <<"nearby-worker-up">>)),
    {nearby_send_result, {ok, _TargetCount}} = recv(Alice),
    expect_nearby_push(
        Alice, AliceId, <<"alice">>, {0, 0}, <<"nearby-worker-up">>).

check_metrics() ->
    Metrics = chat_metrics:snapshot(),
    1 = maps:get(map_router_count, Metrics),
    3 = maps:get(map_worker_count, Metrics),
    #{1 := MapOneQueue, 2 := MapTwoQueue, 3 := MapThreeQueue} =
        maps:get(map_worker_queues, Metrics),
    #{1 := MapOneRoles, 2 := MapTwoRoles, 3 := MapThreeRoles} =
        maps:get(map_role_counts, Metrics),
    true = lists:all(
        fun erlang:is_integer/1,
        [MapOneQueue, MapTwoQueue, MapThreeQueue]),
    true = MapOneRoles + MapTwoRoles + MapThreeRoles =:=
           maps:get(online_count, Metrics),
    6 = maps:get(nearby_worker_count, Metrics),
    24 = maps:get(map_broadcast_worker_count, Metrics),
    true = maps:get(map_broadcast_worker_queue_total, Metrics) >= 0,
    true = maps:get(nearby_messages, Metrics) > 0,
    true = maps:get(nearby_flushes, Metrics) > 0,
    true = maps:get(nearby_batch_max, Metrics) >= 2,
    true = maps:get(channel_timer_flushes, Metrics) > 0,
    true = maps:get(channel_full_flushes, Metrics) > 0,
    true = maps:get(socket_writer_count, Metrics) > 0,
    true = maps:get(socket_writer_queue_total, Metrics) >= 0,
    true = lists:all(
        fun(Key) -> maps:get(Key, Metrics) > 0 end,
        [world_batch_flushes, world_role_packets, world_payload_bytes,
         map_batch_flushes, map_role_packets, map_payload_bytes,
         nearby_delivery_flushes, nearby_role_packets,
         nearby_payload_bytes]),
    #{join := #{count := JoinCount},
      leave := #{count := LeaveCount},
      relocate := #{count := RelocateCount}} = maps:get(map_operations, Metrics),
    true = JoinCount > 0,
    true = LeaveCount > 0,
    true = RelocateCount > 0.

check_real_client(Port) ->
    {Sink, _SinkId, _SinkChannels} = login(Port, <<"sink">>, <<"pw">>),
    teleport_to(Sink, {0, 0}),
    {ok, ClientSup} = chat_client_sup:start_link(),
    unlink(ClientSup),
    {ok, ClientPid} = chat_client_sup:start_client(
        101, "127.0.0.1", Port, <<"client_101">>, <<"123456">>,
        manual),
    wait_until(fun() -> ets:info(online_roles, size) =:= 3 end),
    State = sys:get_state(ClientPid),
    manual = maps:get(mode, State),
    ClientChannels = maps:get(channel_ids, State),
    check_initial_channels(ClientChannels),
    false = maps:get(feedback, State),
    InitialMapId = maps:get(map_id, State),
    true = lists:member(InitialMapId, map_router:map_ids()),
    SpawnPosition = maps:get(position, State),
    true = map_router:valid_position(SpawnPosition),
    {ok, SpawnPosition} = chat_load_test:position(101),
    {ok, {InitialMapId, SpawnPosition}} = chat_load_test:location(101),
    {error, invalid_feedback} = chat_load_test:set_feedback(101, loud),
    ok = chat_load_test:set_feedback(101, true),
    true = maps:get(feedback, sys:get_state(ClientPid)),
    false = lists:any(
        fun(Key) -> maps:is_key(Key, State) end,
        [status, role_id, password, action_state, client_id, client_range]),
    ClientRolePid = role_pid(<<"client_101">>),
    {ok, {InitialMapId, SpawnPosition}} = map_location(ClientRolePid),
    ok = move_client_to_map(101, 1),
    ok = chat_load_test:teleport(101, 0, 0),
    wait_until(fun() ->
        map_position(ClientRolePid) =:= {ok, {0, 0}} andalso
        chat_load_test:position(101) =:= {ok, {0, 0}}
    end),
    ok = chat_load_test:teleport(101, 100, 0),
    ok = chat_load_test:move(101, down),
    wait_until(fun() ->
        map_position(ClientRolePid) =:= {ok, {1, 0}} andalso
        chat_load_test:position(101) =:= {ok, {1, 0}}
    end),
    ok = chat_load_test:teleport(101, 1, 1),
    wait_until(fun() ->
        map_position(ClientRolePid) =:= {ok, {1, 1}} andalso
        chat_load_test:position(101) =:= {ok, {1, 1}}
    end),
    ok = chat_load_test:set_feedback(101, false),
    false = maps:get(feedback, sys:get_state(ClientPid)),
    {error, invalid_direction} = chat_load_test:move(101, diagonal),
    {error, invalid_position} = chat_load_test:teleport(101, -1, 0),
    ok = chat_load_test:send_nearby(101, <<"manual-nearby">>),
    expect_nearby_push(
        Sink, role_id(<<"client_101">>), <<"client_101">>,
        {1, 1}, <<"manual-nearby">>),
    ok = chat_load_test:send_map(101, <<"manual-map">>),
    expect_map_chat_push(
        Sink, 1, role_id(<<"client_101">>), <<"client_101">>,
        <<"manual-map">>),
    [ClientInfo] = [Info
                    || Info <- chat_metrics:online_clients(),
                       maps:get(role_name, Info) =:= <<"client_101">>],
    1 = maps:get(map_id, ClientInfo),
    {1, 1} = maps:get(position, ClientInfo),
    ok = chat_load_test:leave_map(101),
    wait_until(fun() ->
        chat_load_test:location(101) =:= {error, not_in_map}
    end),
    ok = chat_load_test:join_map(101, 2),
    wait_until(fun() ->
        valid_client_location(101, 2)
    end),
    ok = chat_load_test:leave_map(101),
    ok = chat_load_test:join_map(101, 1),
    wait_until(fun() ->
        valid_client_location(101, 1)
    end),
    check_map_load_client(),
    {ok, ObserverPid} = chat_client_sup:start_client(
        batch_observer, "127.0.0.1", Port,
        <<"batch_observer">>, <<"123456">>, observer),
    wait_until(fun() -> ets:info(online_roles, size) =:= 4 end),
    NewChannel = hd(lists:seq(2, 10) -- ClientChannels),
    gen_server:cast(ClientPid, {join_channel, NewChannel}),
    wait_until(fun() ->
        lists:member(NewChannel,
                     maps:get(channel_ids, sys:get_state(ClientPid)))
    end),
    gen_server:cast(ClientPid, {leave_channel, NewChannel}),
    wait_until(fun() ->
        not lists:member(NewChannel,
                         maps:get(channel_ids, sys:get_state(ClientPid)))
    end),
    ok = chat_load_test:send_channel(101, 1, <<"manual">>),
    expect_push_content(Sink, <<"client_101">>, <<"manual">>),
    wait_until(fun() ->
        ObserverState = sys:get_state(ObserverPid),
        maps:get(observer_received, ObserverState) >= 1 andalso
        maps:get(observer_invalid, ObserverState) =:= 0
    end),
    ok = gen_server:stop(ObserverPid),
    ok = chat_load_test:send_private(101, 999999, <<"offline">>),
    {ok, AutoPid} = chat_client_sup:start_client(
        103, "127.0.0.1", Port, <<"client_103">>, <<"123456">>,
        {normal, 103, 103, 103}),
    wait_until(fun() ->
        maps:get(action_seq, sys:get_state(AutoPid)) >= 2
    end),
    ok = gen_server:stop(AutoPid),
    wait_until(fun() -> ets:info(online_roles, size) =:= 3 end),
    ok = gen_server:stop(ClientPid),
    gen_tcp:close(Sink),
    exit(ClientSup, shutdown),
    wait_until(fun() -> ets:info(online_roles, size) =:= 1 end).

check_map_load_client() ->
    {ok, 1} = chat_load_test:start_map(102, 102),
    [ClientPid] = [Pid
                   || {{chat_client, 102}, Pid, worker, _Modules} <-
                          supervisor:which_children(chat_client_sup)],
    wait_until(fun() ->
        State = sys:get_state(ClientPid),
        maps:get(load_started, State, false) andalso
        map_router:valid_position(maps:get(position, State))
    end),
    {ok, Position} = chat_load_test:position(102),
    {ok, {MapId, Position}} = chat_load_test:location(102),
    true = lists:member(MapId, map_router:map_ids()),
    {ok, Position} = map_position(role_pid(<<"client_102">>)),
    {ok, {MapId, Position}} = map_location(role_pid(<<"client_102">>)),
    ClientPid ! auto_action,
    wait_until(fun() ->
        maps:get(action_seq, sys:get_state(ClientPid)) >= 2
    end),
    ok = gen_server:stop(ClientPid),
    wait_until(fun() -> ets:info(online_roles, size) =:= 3 end).

check_main_restart(Alice) ->
    OldMap = whereis(map_router),
    OldMain = whereis(main_channel_server),
    OldChannelSup = whereis(channel_sup),
    OldRoleSup = whereis(role_sup),
    OldListener = whereis(chat_listener),
    exit(OldMain, kill),
    wait_until(fun() -> changed(main_channel_server, OldMain) end),
    wait_until(fun() -> changed(channel_sup, OldChannelSup) end),
    wait_until(fun() -> changed(role_sup, OldRoleSup) end),
    wait_until(fun() -> changed(chat_listener, OldListener) end),
    OldMap = whereis(map_router),
    wait_socket_closed(Alice, 20),
    wait_until(fun() ->
        Snapshot = chat_metrics:snapshot(),
        maps:get(online_count, Snapshot) =:= 0 andalso
        maps:get(world_member_count, Snapshot) =:= 0 andalso
        maps:get(role_count, Snapshot) =:= 0 andalso
        ets:info(map_role_positions, size) =:= 0 andalso
        maps:get(channel_count, Snapshot) =:= 13 andalso
        maps:get(world_worker_count, Snapshot) =:= 8
    end),
    check_world_member_shards(0).

check_map_restart(Port) ->
    {Socket, _RoleId, _Channels} = login(Port, <<"before_map_restart">>, <<"pw">>),
    OldMap = whereis(map_router),
    OldMain = whereis(main_channel_server),
    OldChannelSup = whereis(channel_sup),
    OldRoleSup = whereis(role_sup),
    OldListener = whereis(chat_listener),
    exit(OldMap, kill),
    wait_until(fun() -> changed(map_router, OldMap) end),
    wait_until(fun() -> changed(main_channel_server, OldMain) end),
    wait_until(fun() -> changed(channel_sup, OldChannelSup) end),
    wait_until(fun() -> changed(role_sup, OldRoleSup) end),
    wait_until(fun() -> changed(chat_listener, OldListener) end),
    wait_socket_closed(Socket, 20),
    NewMap = whereis(map_router),
    NewMap = ets:info(map_cells_1, owner),
    NewMap = ets:info(map_role_positions, owner),
    wait_until(fun() -> ets:info(online_roles, size) =:= 0 end),
    0 = ets:info(map_role_positions, size),
    check_world_member_shards(0).

check_restart_accepts_login(Port) ->
    {Socket, _RoleId, _Channels} = login(Port, <<"after_restart">>, <<"pw">>),
    check_world_member_shards(1),
    1 = ets:info(map_role_positions, size),
    gen_tcp:close(Socket),
    wait_until(fun() -> ets:info(online_roles, size) =:= 0 end),
    wait_until(fun() -> world_member_count() =:= 0 end),
    wait_until(fun() -> ets:info(map_role_positions, size) =:= 0 end).

check_world_member_shards(ExpectedCount) ->
    Tables = channel_server:world_member_tables(),
    8 = length(Tables),
    MainPid = whereis(main_channel_server),
    true = lists:all(
        fun(Table) -> ets:info(Table, owner) =:= MainPid end,
        Tables),
    Members = lists:append([ets:tab2list(Table) || Table <- Tables]),
    ExpectedCount = length(Members),
    RoleIds = [RoleId || #world_channel_member{role_id = RoleId} <- Members],
    ExpectedCount = length(lists:usort(RoleIds)),
    true = lists:all(
        fun(RoleId) ->
            [Table] = [MemberTable || MemberTable <- Tables,
                                      ets:member(MemberTable, RoleId)],
            channel_server:world_member_table(RoleId) =:= Table
        end,
        RoleIds).

world_member_count() ->
    lists:sum([ets:info(Table, size)
               || Table <- channel_server:world_member_tables()]).

recv_world_contents(Socket, SenderId, ContentCount, ResultCount) ->
    recv_world_contents(
        Socket, SenderId, ContentCount, ResultCount, [], 0).

recv_world_contents(_Socket, _SenderId, ContentCount, ResultCount,
                    Contents, ResultCount)
  when length(Contents) =:= ContentCount ->
    Contents;
recv_world_contents(Socket, SenderId, ContentCount, ResultCount,
                    Contents, Results) ->
    case recv(Socket) of
        {channel_send_result, {ok, 1}} ->
            recv_world_contents(
                Socket, SenderId, ContentCount, ResultCount,
                Contents, Results + 1);
        {channel_push, Message} ->
            recv_world_contents(
                Socket, SenderId, ContentCount, ResultCount,
                Contents ++ message_contents([Message], SenderId), Results);
        {channel_push_batch, Messages} ->
            recv_world_contents(
                Socket, SenderId, ContentCount, ResultCount,
                Contents ++ message_contents(Messages, SenderId), Results)
    end.

message_contents(Messages, SenderId) ->
    lists:map(
        fun(#{channel_id := 1,
              sender_role_id := MessageSenderId,
              content := Content}) ->
            SenderId = MessageSenderId,
            Content
        end,
        Messages).

channel_message_contents(Messages, ChannelId, SenderId) ->
    [Content || #{channel_id := MessageChannelId,
                  sender_role_id := MessageSenderId,
                  content := Content} <- Messages,
                ChannelId =:= MessageChannelId,
                SenderId =:= MessageSenderId].

map_message_contents(Messages, MapId, SenderId) ->
    [Content || #{map_id := MessageMapId,
                  sender_role_id := MessageSenderId,
                  content := Content} <- Messages,
                MapId =:= MessageMapId,
                SenderId =:= MessageSenderId].

ensure_joined(Socket, ChannelId) ->
    send(Socket, chat_client_protocol:encode_channel_join(ChannelId)),
    case recv(Socket) of
        {channel_join_result, {ok, ChannelId}} -> ok;
        {channel_join_result, {error, already_joined, ChannelId}} -> ok
    end.

expect_channel_push(Socket, ChannelId, SenderId, SenderName, Content) ->
    case recv(Socket) of
        {channel_push, Message} ->
            check_channel_push(
                Message, ChannelId, SenderId, SenderName, Content);
        {channel_push_batch, [Message]} ->
            check_channel_push(
                Message, ChannelId, SenderId, SenderName, Content)
    end.

check_channel_push(#{channel_id := ChannelId,
                     sender_role_id := SenderId,
                     sender_role_name := SenderName,
                     content := Content},
                   ChannelId, SenderId, SenderName, Content) ->
    ok.

expect_push_content(Socket, SenderName, Content) ->
    case recv(Socket) of
        {channel_push, #{sender_role_name := SenderName,
                         content := Content}} ->
            ok;
        {channel_push_batch, Messages} ->
            case lists:any(
                     fun(#{sender_role_name := MessageSender,
                           content := MessageContent}) ->
                         MessageSender =:= SenderName andalso
                         MessageContent =:= Content
                     end,
                     Messages) of
                true -> ok;
                false -> expect_push_content(Socket, SenderName, Content)
            end;
        _Other ->
            expect_push_content(Socket, SenderName, Content)
    end.

expect_nearby_push(Socket, SenderId, SenderName, Position, Content) ->
    case recv(Socket) of
        {nearby_push, #{sender_role_id := SenderId,
                        sender_role_name := SenderName,
                        position := Position,
                        content := Content}} ->
            ok;
        {nearby_push_batch, Messages} ->
            true = lists:any(
                fun(#{sender_role_id := MessageSenderId,
                      sender_role_name := MessageSenderName,
                      position := MessagePosition,
                      content := MessageContent}) ->
                    MessageSenderId =:= SenderId andalso
                    MessageSenderName =:= SenderName andalso
                    MessagePosition =:= Position andalso
                    MessageContent =:= Content
                end,
                Messages),
            ok;
        _Other ->
            expect_nearby_push(
                Socket, SenderId, SenderName, Position, Content)
    end.

expect_map_chat_push(Socket, MapId, SenderId, SenderName, Content) ->
    case recv(Socket) of
        {map_chat_push, #{map_id := MapId,
                          sender_role_id := SenderId,
                          sender_role_name := SenderName,
                          content := Content}} ->
            ok;
        {map_chat_push_batch, [#{map_id := MapId,
                                 sender_role_id := SenderId,
                                 sender_role_name := SenderName,
                                 content := Content}]} ->
            ok;
        _Other ->
            expect_map_chat_push(
                Socket, MapId, SenderId, SenderName, Content)
    end.

role_pid(RoleName) ->
    [#online_role{role_pid = RolePid}] = ets:lookup(online_roles, RoleName),
    RolePid.

map_position(RolePid) ->
    case map_location(RolePid) of
        {ok, {_MapId, Position}} -> {ok, Position};
        error -> error
    end.

map_location(RolePid) ->
    case ets:lookup(map_role_positions, RolePid) of
        [{RolePid, Location}] -> {ok, Location};
        [] -> error
    end.

role_id(RoleName) ->
    RolePid = role_pid(RoleName),
    [RoleId] = [Id
                || Table <- channel_server:world_member_tables(),
                   #world_channel_member{role_id = Id,
                                         role_pid = MemberPid} <- ets:tab2list(Table),
                   MemberPid =:= RolePid],
    RoleId.

login(Port, RoleName, Password) ->
    Socket = connect(Port),
    send(Socket, chat_client_protocol:encode_login(RoleName, Password)),
    {login_result, {ok, RoleId, MapId, Position, ChannelIds}} = recv(Socket),
    true = lists:member(MapId, map_router:map_ids()),
    true = map_router:valid_position(Position),
    ok = move_socket_to_map(Socket, MapId, 1),
    {Socket, RoleId, ChannelIds}.

move_socket_to_map(_Socket, MapId, MapId) ->
    ok;
move_socket_to_map(Socket, CurrentMapId, TargetMapId) ->
    send(Socket, chat_client_protocol:encode_map_leave()),
    {map_leave_result, {ok, CurrentMapId}} = recv(Socket),
    send(Socket, chat_client_protocol:encode_map_join(TargetMapId)),
    {map_join_result, {ok, TargetMapId, Position}} = recv(Socket),
    true = map_router:valid_position(Position),
    ok.

move_client_to_map(ClientId, MapId) ->
    case chat_load_test:location(ClientId) of
        {ok, {MapId, _Position}} ->
            ok;
        {ok, {_CurrentMapId, _Position}} ->
            ok = chat_load_test:leave_map(ClientId),
            wait_until(fun() ->
                chat_load_test:location(ClientId) =:= {error, not_in_map}
            end),
            ok = chat_load_test:join_map(ClientId, MapId),
            wait_until(fun() -> valid_client_location(ClientId, MapId) end),
            ok
    end.

valid_client_location(ClientId, MapId) ->
    case chat_load_test:location(ClientId) of
        {ok, {MapId, Position}} -> map_router:valid_position(Position);
        _ -> false
    end.

connect(Port) ->
    {ok, Socket} = gen_tcp:connect(
        "127.0.0.1", Port,
        [binary, {packet, 4}, {active, false}], ?TIMEOUT),
    Socket.

send(Socket, Packet) ->
    ok = gen_tcp:send(Socket, Packet).

recv(Socket) ->
    recv(Socket, ?TIMEOUT).

recv(Socket, Timeout) ->
    {ok, Packet} = gen_tcp:recv(Socket, 0, Timeout),
    {ok, Decoded} = chat_client_protocol:decode_packet(Packet),
    Decoded.

free_port() ->
    {ok, ListenSocket} = gen_tcp:listen(
        0, [binary, {packet, 4}, {active, false}, {reuseaddr, true}]),
    {ok, {_Address, Port}} = inet:sockname(ListenSocket),
    ok = gen_tcp:close(ListenSocket),
    Port.

changed(Name, OldPid) ->
    NewPid = whereis(Name),
    is_pid(NewPid) andalso NewPid =/= OldPid.

wait_socket_closed(_Socket, 0) ->
    error(socket_not_closed);
wait_socket_closed(Socket, Attempts) ->
    case gen_tcp:recv(Socket, 0, 250) of
        {error, closed} -> ok;
        {ok, _Packet} -> wait_socket_closed(Socket, Attempts - 1);
        {error, timeout} -> wait_socket_closed(Socket, Attempts - 1)
    end.

wait_until(Fun) ->
    wait_until(Fun, 100).

wait_until(_Fun, 0) ->
    error(wait_timeout);
wait_until(Fun, Attempts) ->
    case catch Fun() of
        true -> ok;
        _ ->
            timer:sleep(50),
            wait_until(Fun, Attempts - 1)
    end.
