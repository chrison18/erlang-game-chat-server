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
             ?assertEqual({ok, {11, 10}},
                          map_server:move(MapPid, RolePid, {11, 10})),
             ?assertEqual({ok, {80, 90}},
                          map_server:teleport(MapPid, RolePid, {80, 90})),
             ?assertEqual({error, invalid_position},
                          map_server:teleport(MapPid, RolePid, {100, 90})),
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
