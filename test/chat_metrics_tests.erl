-module(chat_metrics_tests).

-include_lib("eunit/include/eunit.hrl").

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
