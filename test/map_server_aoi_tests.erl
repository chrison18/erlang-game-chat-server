-module(map_server_aoi_tests).

-include_lib("eunit/include/eunit.hrl").

join_and_leave_notify_both_roles_test_() ->
    {setup,
     fun start_map/0,
     fun stop_map/1,
     fun(MapPid) ->
         fun() ->
             Role1 = start_role_proxy(role1),
             Role2 = start_role_proxy(role2),
             try
                 ?assertEqual({ok, {10, {10, 10}}},
                              map_server:join(
                                  MapPid, 1, Role1, {10, 10})),
                 ?assertEqual({ok, {10, {11, 10}}},
                              map_server:join(
                                  MapPid, 2, Role2, {11, 10})),
                 assert_aoi(role1, MapPid, enter, 2),
                 assert_aoi(role2, MapPid, enter, 1),
                 ?assertEqual({ok, 10}, map_server:leave(MapPid, 2)),
                 assert_aoi(role1, MapPid, leave, 2),
                 assert_aoi(role2, MapPid, leave, 1),
                 assert_no_aoi([role1, role2])
             after
                 stop_role_proxies([Role1, Role2])
             end
         end
     end}.

role_down_notifies_only_surviving_roles_test_() ->
    {setup,
     fun start_map/0,
     fun stop_map/1,
     fun(MapPid) ->
         fun() ->
             Role1 = start_role_proxy(role1),
             Role2 = start_role_proxy(role2),
             try
                 join_role(MapPid, 1, Role1, {10, 10}),
                 join_role(MapPid, 2, Role2, {11, 10}),
                 assert_aoi(role1, MapPid, enter, 2),
                 assert_aoi(role2, MapPid, enter, 1),
                 exit(Role2, kill),
                 assert_aoi(role1, MapPid, leave, 2),
                 wait_for_role_removed(MapPid, 2, 50),
                 assert_no_aoi([role1])
             after
                 stop_role_proxies([Role1, Role2])
             end
         end
     end}.

move_crossing_updates_only_direction_edges_test_() ->
    [move_case(Direction, OldPosition, LeavePosition,
               EnterPosition, SharedPosition)
     || {Direction, OldPosition, LeavePosition,
         EnterPosition, SharedPosition} <-
            [{up, {12, 10}, {14, 10}, {8, 10}, {10, 10}},
             {down, {11, 10}, {8, 10}, {14, 10}, {10, 10}},
             {left, {10, 12}, {10, 15}, {10, 6}, {10, 10}},
             {right, {10, 11}, {10, 6}, {10, 15}, {10, 10}}]].

move_within_shard_sends_no_aoi_test_() ->
    {setup,
     fun start_map/0,
     fun stop_map/1,
     fun(MapPid) ->
         fun() ->
             Mover = start_role_proxy(mover),
             Neighbor = start_role_proxy(neighbor),
             try
                 join_role(MapPid, 1, Mover, {10, 10}),
                 join_role(MapPid, 2, Neighbor, {10, 12}),
                 drain_messages(),
                 ?assertEqual(ok, map_server:move(MapPid, 1, right)),
                 assert_result(mover, MapPid, move, {ok, {10, 11}}),
                 assert_no_aoi([mover, neighbor])
             after
                 stop_role_proxies([Mover, Neighbor])
             end
         end
     end}.

move_at_map_edge_clips_invalid_shards_test_() ->
    {setup,
     fun start_map/0,
     fun stop_map/1,
     fun(MapPid) ->
         fun() ->
             Mover = start_role_proxy(mover),
             Enter1 = start_role_proxy(enter1),
             Enter2 = start_role_proxy(enter2),
             try
                 join_role(MapPid, 1, Mover, {0, 2}),
                 join_role(MapPid, 2, Enter1, {0, 6}),
                 join_role(MapPid, 3, Enter2, {2, 6}),
                 drain_messages(),
                 ?assertEqual(ok, map_server:move(MapPid, 1, right)),
                 assert_aoi(mover, MapPid, enter, 2),
                 assert_aoi(mover, MapPid, enter, 3),
                 assert_result(mover, MapPid, move, {ok, {0, 3}}),
                 assert_aoi(enter1, MapPid, enter, 1),
                 assert_aoi(enter2, MapPid, enter, 1),
                 assert_no_aoi([mover, enter1, enter2])
             after
                 stop_role_proxies([Mover, Enter1, Enter2])
             end
         end
     end}.

teleport_uses_overlapping_aoi_difference_test_() ->
    {setup,
     fun start_map/0,
     fun stop_map/1,
     fun(MapPid) ->
         fun() ->
             assert_teleport_case(
                 MapPid, {10, 10}, {10, 12},
                 {10, 6}, {10, 15}, {10, 10})
         end
     end}.

teleport_between_separate_aois_notifies_both_sides_test_() ->
    {setup,
     fun start_map/0,
     fun stop_map/1,
     fun(MapPid) ->
         fun() ->
             Mover = start_role_proxy(mover),
             Leave = start_role_proxy(leave_role),
             Enter = start_role_proxy(enter_role),
             try
                 join_role(MapPid, 1, Mover, {10, 10}),
                 join_role(MapPid, 2, Leave, {10, 12}),
                 join_role(MapPid, 3, Enter, {80, 90}),
                 drain_messages(),
                 ?assertEqual(
                     ok, map_server:teleport(MapPid, 1, {80, 90})),
                 assert_aoi(mover, MapPid, leave, 2),
                 assert_aoi(mover, MapPid, enter, 3),
                 assert_result(
                     mover, MapPid, teleport, {ok, {80, 90}}),
                 assert_aoi(leave_role, MapPid, leave, 1),
                 assert_aoi(enter_role, MapPid, enter, 1),
                 assert_no_aoi([mover, leave_role, enter_role])
             after
                 stop_role_proxies([Mover, Leave, Enter])
             end
         end
     end}.

failed_operations_send_no_aoi_test_() ->
    {setup,
     fun start_map/0,
     fun stop_map/1,
     fun(MapPid) ->
         fun() ->
             Role1 = start_role_proxy(role1),
             Role2 = start_role_proxy(role2),
             try
                 join_role(MapPid, 1, Role1, {0, 0}),
                 join_role(MapPid, 2, Role2, {0, 1}),
                 drain_messages(),
                 ?assertEqual(ok, map_server:move(MapPid, 1, up)),
                 assert_result(
                     role1, MapPid, move,
                     {error, out_of_bounds, {0, 0}}),
                 ?assertEqual(
                     ok, map_server:teleport(MapPid, 1, {100, 0})),
                 assert_result(
                     role1, MapPid, teleport,
                     {error, invalid_position, {0, 0}}),
                 ?assertEqual(
                     {error, {already_in_map, 10}},
                     map_server:join(MapPid, 1, Role1, {20, 20})),
                 ?assertEqual(
                     {error, not_in_map}, map_server:leave(MapPid, 999)),
                 assert_no_aoi([role1, role2])
             after
                 stop_role_proxies([Role1, Role2])
             end
         end
     end}.

move_case(Direction, OldPosition, LeavePosition,
          EnterPosition, SharedPosition) ->
    {atom_to_list(Direction),
     {setup,
      fun start_map/0,
      fun stop_map/1,
      fun(MapPid) ->
          fun() ->
              assert_move_case(
                  MapPid, Direction, OldPosition, LeavePosition,
                  EnterPosition, SharedPosition)
          end
      end}}.

assert_move_case(MapPid, Direction, OldPosition, LeavePosition,
                 EnterPosition, SharedPosition) ->
    Mover = start_role_proxy(mover),
    Leave = start_role_proxy(leave_role),
    Enter = start_role_proxy(enter_role),
    Shared = start_role_proxy(shared_role),
    try
        join_role(MapPid, 1, Mover, OldPosition),
        join_role(MapPid, 2, Leave, LeavePosition),
        join_role(MapPid, 3, Enter, EnterPosition),
        join_role(MapPid, 4, Shared, SharedPosition),
        drain_messages(),
        ?assertEqual(ok, map_server:move(MapPid, 1, Direction)),
        assert_aoi(mover, MapPid, leave, 2),
        assert_aoi(mover, MapPid, enter, 3),
        assert_result(
            mover, MapPid, move,
            {ok, move_target(Direction, OldPosition)}),
        assert_aoi(leave_role, MapPid, leave, 1),
        assert_aoi(enter_role, MapPid, enter, 1),
        assert_no_aoi([mover, leave_role, enter_role, shared_role])
    after
        stop_role_proxies([Mover, Leave, Enter, Shared])
    end.

assert_teleport_case(MapPid, OldPosition, NewPosition,
                     LeavePosition, EnterPosition, SharedPosition) ->
    Mover = start_role_proxy(mover),
    Leave = start_role_proxy(leave_role),
    Enter = start_role_proxy(enter_role),
    Shared = start_role_proxy(shared_role),
    try
        join_role(MapPid, 1, Mover, OldPosition),
        join_role(MapPid, 2, Leave, LeavePosition),
        join_role(MapPid, 3, Enter, EnterPosition),
        join_role(MapPid, 4, Shared, SharedPosition),
        drain_messages(),
        ?assertEqual(ok, map_server:teleport(MapPid, 1, NewPosition)),
        assert_aoi(mover, MapPid, leave, 2),
        assert_aoi(mover, MapPid, enter, 3),
        assert_result(mover, MapPid, teleport, {ok, NewPosition}),
        assert_aoi(leave_role, MapPid, leave, 1),
        assert_aoi(enter_role, MapPid, enter, 1),
        assert_no_aoi([mover, leave_role, enter_role, shared_role])
    after
        stop_role_proxies([Mover, Leave, Enter, Shared])
    end.

move_target(up, {X, Y}) -> {X - 1, Y};
move_target(down, {X, Y}) -> {X + 1, Y};
move_target(left, {X, Y}) -> {X, Y - 1};
move_target(right, {X, Y}) -> {X, Y + 1}.

start_map() ->
    {ok, MapPid} = map_server:start_link(10),
    MapPid.

stop_map(MapPid) ->
    gen_server:stop(MapPid).

start_role_proxy(Label) ->
    Parent = self(),
    spawn(fun() -> role_proxy_loop(Parent, Label) end).

role_proxy_loop(Parent, Label) ->
    receive
        Message ->
            Parent ! {Label, Message},
            role_proxy_loop(Parent, Label)
    end.

stop_role_proxies(RolePids) ->
    lists:foreach(fun(RolePid) -> exit(RolePid, kill) end, RolePids).

join_role(MapPid, RoleId, RolePid, Position) ->
    ?assertEqual(
        {ok, {10, Position}},
        map_server:join(MapPid, RoleId, RolePid, Position)).

assert_aoi(Label, MapPid, Event, RoleId) ->
    ?assertEqual(
        {aoi_event, MapPid, Event, RoleId},
        receive_aoi(Label, 500)).

receive_aoi(Label, Timeout) ->
    receive
        {Label, {'$gen_cast',
                 {aoi_event, _MapPid, _Event, _RoleId} = AoiEvent}} ->
            AoiEvent
    after Timeout ->
        timeout
    end.

assert_result(Label, MapPid, Operation, Result) ->
    receive
        {Label, {'$gen_cast',
                 {map_result, MapPid, Operation, Result}}} ->
            ok
    after 500 ->
        ?assert(false)
    end.

assert_no_aoi(Labels) ->
    lists:foreach(
        fun(Label) -> ?assertEqual(timeout, receive_aoi(Label, 50)) end,
        Labels).

drain_messages() ->
    receive
        {_Label, {'$gen_cast', _Message}} -> drain_messages()
    after 20 ->
        ok
    end.

wait_for_role_removed(_MapPid, _RoleId, 0) ->
    ?assert(false);
wait_for_role_removed(MapPid, RoleId, Attempts) ->
    case map_server:debug_role(MapPid, RoleId) of
        undefined ->
            ok;
        _RoleInfo ->
            receive after 10 -> ok end,
            wait_for_role_removed(MapPid, RoleId, Attempts - 1)
    end.
