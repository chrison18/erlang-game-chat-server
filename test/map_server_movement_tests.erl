-module(map_server_movement_tests).

-include_lib("eunit/include/eunit.hrl").

move_and_teleport_are_distinct_test_() ->
    {setup,
     fun start_map/0,
     fun stop_map/1,
     fun(MapPid) ->
         fun() ->
             RolePid = self(),
             ?assertEqual({ok, {10, {10, 10}}},
                          map_server:join(MapPid, 1, RolePid, {10, 10})),
             ?assertEqual(ok, map_server:move(MapPid, RolePid, down)),
             ?assertEqual({ok, {11, 10}}, receive_result(MapPid, move)),
             ?assertEqual(ok,
                          map_server:teleport(MapPid, RolePid, {80, 90})),
             ?assertEqual({ok, {80, 90}}, receive_result(MapPid, teleport)),
             ?assertEqual(ok,
                          map_server:send_nearby(
                              MapPid, 1, RolePid, <<"alice">>, <<"nearby">>)),
             ?assertEqual({ok, 1}, receive_result(MapPid, send_nearby)),
             assert_nearby_push(1, <<"alice">>, {80, 90}, <<"nearby">>),
             ?assertEqual(ok,
                          map_server:teleport(MapPid, RolePid, {100, 90})),
             ?assertEqual({error, invalid_position, {80, 90}},
                          receive_result(MapPid, teleport)),
             Stats = map_server:operation_stats(10),
             ?assertMatch(#{move := #{count := 1},
                            teleport := #{count := 2}},
                          Stats),
             ?assertEqual(false, maps:is_key(relocate, Stats))
         end
     end}.

start_map() ->
    {ok, MapPid} = map_server:start_link(10),
    MapPid.

stop_map(MapPid) ->
    gen_server:stop(MapPid).

receive_result(MapPid, Operation) ->
    receive
        {'$gen_cast', {map_result, MapPid, Operation, Result}} -> Result
    after 500 ->
        timeout
    end.

assert_nearby_push(RoleId, RoleName, Position, Content) ->
    receive
        {'$gen_cast', {push_batch, Packet}} ->
            ?assertEqual(
                {ok, {nearby_push,
                      #{sender_role_id => RoleId,
                        sender_role_name => RoleName,
                        position => Position,
                        content => Content}}},
                chat_client_protocol:decode_packet(Packet))
    after 500 ->
        ?assert(false)
    end.
