-module(chat_metrics_tests).

-include_lib("eunit/include/eunit.hrl").

map_queue_metric_contract_test() ->
    EmptySnapshot = chat_metrics:snapshot(),
    ?assertEqual(0, maps:get(map_server_queue_max, EmptySnapshot)),
    assert_no_queue_totals(EmptySnapshot),

    undefined = whereis(map_server_1),
    undefined = whereis(map_server_2),
    MapPid1 = spawn(fun map_server_stub/0),
    MapPid2 = spawn(fun map_server_stub/0),
    try
        true = register(map_server_1, MapPid1),
        true = register(map_server_2, MapPid2),
        send_queue_markers(MapPid1, 2),
        send_queue_markers(MapPid2, 5),

        Snapshot = chat_metrics:snapshot(),
        MapQueues = maps:get(map_server_queues, Snapshot),
        ?assertEqual(2, maps:get(1, MapQueues)),
        ?assertEqual(5, maps:get(2, MapQueues)),
        ?assertEqual(undefined, maps:get(3, MapQueues)),
        ?assertEqual(5, maps:get(map_server_queue_max, Snapshot)),
        assert_no_queue_totals(Snapshot)
    after
        stop_map_server_stub(map_server_1, MapPid1),
        stop_map_server_stub(map_server_2, MapPid2)
    end.

assert_no_queue_totals(Snapshot) ->
    ?assertNot(maps:is_key(role_queue_total, Snapshot)),
    ?assertNot(maps:is_key(channel_queue_total, Snapshot)),
    ?assertNot(maps:is_key(map_server_queue_total, Snapshot)),
    ?assertNot(maps:is_key(world_worker_queue_total, Snapshot)).

send_queue_markers(_Pid, 0) ->
    ok;
send_queue_markers(Pid, Count) ->
    Pid ! {queue_marker, Count},
    send_queue_markers(Pid, Count - 1).

map_server_stub() ->
    receive
        {'$gen_call', From, operation_stats} ->
            ok = gen_server:reply(From, #{}),
            map_server_stub()
    end.

stop_map_server_stub(Name, Pid) ->
    case whereis(Name) of
        Pid -> true = unregister(Name);
        _ -> ok
    end,
    exit(Pid, kill).

world_broadcast_delivery_metrics_test_() ->
    {setup,
     fun create_broadcast_delivery_metrics/0,
     fun delete_broadcast_delivery_metrics/1,
     fun(_Table) ->
         fun() ->
             ?assertEqual(
                 ok,
                 chat_metrics:record_world_broadcast_delivery(3, 120)),
             ?assertEqual(
                 [{world, 1, 3, 120}],
                 ets:lookup(broadcast_delivery_metrics, world)),

             ?assertEqual(
                 ok,
                 chat_metrics:record_world_broadcast_delivery(5, 250)),
             ?assertEqual(
                 [{world, 2, 8, 370}],
                 ets:lookup(broadcast_delivery_metrics, world)),

             Snapshot = chat_metrics:snapshot(),
             ?assertEqual(2, maps:get(world_batch_flushes, Snapshot)),
             ?assertEqual(8, maps:get(world_role_packets, Snapshot)),
             ?assertEqual(370, maps:get(world_payload_bytes, Snapshot)),

             true = ets:delete(broadcast_delivery_metrics),
             ?assertEqual(
                 ok,
                 chat_metrics:record_world_broadcast_delivery(1, 1))
         end
     end}.

create_broadcast_delivery_metrics() ->
    undefined = ets:whereis(broadcast_delivery_metrics),
    broadcast_delivery_metrics = ets:new(
        broadcast_delivery_metrics, [named_table, public, set]),
    true = ets:insert(broadcast_delivery_metrics, {world, 0, 0, 0}),
    broadcast_delivery_metrics.

delete_broadcast_delivery_metrics(_Table) ->
    case ets:whereis(broadcast_delivery_metrics) of
        undefined -> ok;
        _TableId ->
            true = ets:delete(broadcast_delivery_metrics),
            ok
    end.
