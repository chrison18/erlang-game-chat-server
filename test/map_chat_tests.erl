-module(map_chat_tests).

-include_lib("eunit/include/eunit.hrl").

map_chat_broadcasts_only_to_current_map_test_() ->
    {setup,
     fun start_maps/0,
     fun stop_maps/1,
     fun({Map9, Map10}) ->
         fun() ->
             Role1 = start_role_proxy(role1),
             Role2 = start_role_proxy(role2),
             Role3 = start_role_proxy(role3),
             try
                 ?assertEqual({ok, {9, {10, 10}}},
                              map_server:join(Map9, 100, Role1, {10, 10})),
                 ?assertEqual({ok, {9, {11, 10}}},
                              map_server:join(Map9, 2, Role2, {11, 10})),
                 ?assertEqual({ok, {10, {10, 10}}},
                              map_server:join(Map10, 3, Role3, {10, 10})),
                 ?assertEqual(ok,
                              map_server:send_map(
                                  Map9, Role1, <<"alice">>, <<"hello">>)),
                 assert_map_result(role1, Map9, send_map, {ok, 9}),
                 assert_map_chat(role1, 9, 100, <<"alice">>, <<"hello">>),
                 assert_map_chat(role2, 9, 100, <<"alice">>, <<"hello">>),
                 ?assertEqual(timeout, receive_push(role3, 100)),
                 ?assertMatch(#{send_map := #{count := 1}},
                              map_server:operation_stats(9))
             after
                 stop_role_proxies([Role1, Role2, Role3])
             end
         end
     end}.

map_chat_rejects_non_member_test_() ->
    {setup,
     fun start_map/0,
     fun stop_map/1,
     fun(MapPid) ->
         fun() ->
             RolePid = start_role_proxy(non_member),
             try
                 ?assertEqual(
                     ok,
                     map_server:send_map(
                         MapPid, RolePid, <<"unknown">>, <<"hello">>)),
                 assert_map_result(
                     non_member, MapPid, send_map, {error, not_in_map}),
                 ?assertEqual(timeout, receive_push(non_member, 100)),
                 ?assertEqual(
                     {error, map_unavailable},
                     map_server:send_map(
                         undefined, RolePid, <<"unknown">>, <<"hello">>))
             after
                 stop_role_proxies([RolePid])
             end
         end
     end}.

map_chat_stops_after_leave_test_() ->
    {setup,
     fun start_map/0,
     fun stop_map/1,
     fun(MapPid) ->
         fun() ->
             Role1 = start_role_proxy(role1),
             Role2 = start_role_proxy(role2),
             try
                 ?assertEqual({ok, {10, {10, 10}}},
                              map_server:join(MapPid, 1, Role1, {10, 10})),
                 ?assertEqual({ok, {10, {11, 10}}},
                              map_server:join(MapPid, 2, Role2, {11, 10})),
                 ?assertEqual({ok, 10},
                              map_server:leave(MapPid, Role2)),
                 ?assertEqual(ok,
                              map_server:send_map(
                                  MapPid, Role2, <<"bob">>, <<"left">>)),
                 assert_map_result(
                     role2, MapPid, send_map, {error, not_in_map}),
                 ?assertEqual(ok,
                              map_server:send_map(
                                  MapPid, Role1, <<"alice">>, <<"still here">>)),
                 assert_map_result(role1, MapPid, send_map, {ok, 10}),
                 assert_map_chat(
                     role1, 10, 1, <<"alice">>, <<"still here">>),
                 ?assertEqual(timeout, receive_push(role2, 100))
             after
                 stop_role_proxies([Role1, Role2])
             end
         end
     end}.

map_chat_batches_packets_in_order_test_() ->
    {setup,
     fun start_map/0,
     fun stop_map/1,
     fun(MapPid) ->
         fun() ->
             RolePid = start_role_proxy(role1),
             try
                 ?assertEqual({ok, {10, {10, 10}}},
                              map_server:join(
                                  MapPid, 1, RolePid, {10, 10})),
                 ?assertEqual(ok,
                              map_server:send_map(
                                  MapPid, RolePid,
                                  <<"alice">>, <<"first">>)),
                 ?assertEqual(ok,
                              map_server:send_map(
                                  MapPid, RolePid,
                                  <<"alice">>, <<"second">>)),
                 assert_map_result(role1, MapPid, send_map, {ok, 10}),
                 assert_map_result(role1, MapPid, send_map, {ok, 10}),
                 {ok, [FirstPacket, SecondPacket]} =
                     receive_push_packets(role1, 500),
                 assert_map_chat_packet(
                     FirstPacket, 10, 1, <<"alice">>, <<"first">>),
                 assert_map_chat_packet(
                     SecondPacket, 10, 1, <<"alice">>, <<"second">>)
             after
                 stop_role_proxies([RolePid])
             end
         end
     end}.

role_down_removes_member_and_cell_test_() ->
    {setup,
     fun start_map/0,
     fun stop_map/1,
     fun(MapPid) ->
         fun() ->
             Role1 = start_role_proxy(role1),
             Role2 = start_role_proxy(role2),
             try
                 ?assertEqual({ok, {10, {10, 10}}},
                              map_server:join(MapPid, 1, Role1, {10, 10})),
                 ?assertEqual({ok, {10, {11, 10}}},
                              map_server:join(MapPid, 2, Role2, {11, 10})),
                 exit(Role2, kill),
                 State = wait_for_role_removed(MapPid, Role2, 50),
                 Members = maps:get(members, State),
                 Cells = maps:get(cells, State),
                 ?assertEqual(false, maps:is_key(Role2, Members)),
                 ?assertEqual(false, maps:is_key({11, 10}, Cells)),
                 ?assertEqual(ok,
                              map_server:send_nearby(
                                  MapPid, Role1, <<"alice">>, <<"nearby">>)),
                 assert_map_result(role1, MapPid, send_nearby, {ok, 1})
             after
                 stop_role_proxies([Role1, Role2])
             end
         end
     end}.

map_chat_protocol_roundtrip_test() ->
    Content = <<"hello">>,
    ?assertEqual(
        {ok, {send_map, Content}},
        chat_server_protocol:decode_request(
            chat_client_protocol:encode_map_chat_send(Content))),
    ?assertEqual(
        {ok, {map_chat_send_result, {ok, 9}}},
        chat_client_protocol:decode_packet(
            chat_server_protocol:encode_map_chat_send_result({ok, 9}))),
    ?assertEqual(
        {ok, {map_chat_send_result, {error, not_in_map}}},
        chat_client_protocol:decode_packet(
            chat_server_protocol:encode_map_chat_send_result(
                {error, not_in_map}))).

start_map() ->
    {ok, MapPid} = map_server:start_link(10),
    MapPid.

stop_map(MapPid) ->
    gen_server:stop(MapPid).

start_maps() ->
    {ok, Map9} = map_server:start_link(9),
    {ok, Map10} = map_server:start_link(10),
    {Map9, Map10}.

stop_maps({Map9, Map10}) ->
    gen_server:stop(Map9),
    gen_server:stop(Map10).

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

assert_map_chat(Label, MapId, RoleId, RoleName, Content) ->
    {ok, Packet} = receive_push(Label, 500),
    assert_map_chat_packet(Packet, MapId, RoleId, RoleName, Content).

assert_map_chat_packet(Packet, MapId, RoleId, RoleName, Content) ->
    ?assertEqual(
        {ok, {map_chat_push,
              #{map_id => MapId,
                sender_role_id => RoleId,
                sender_role_name => RoleName,
                content => Content}}},
        chat_client_protocol:decode_packet(Packet)).

assert_map_result(Label, MapPid, Operation, Expected) ->
    receive
        {Label, {'$gen_cast',
                 {map_result, MapPid, Operation, Expected}}} ->
            ok
    after 500 ->
        ?assert(false)
    end.

receive_push(Label, Timeout) ->
    case receive_push_packets(Label, Timeout) of
        {ok, [Packet]} -> {ok, Packet};
        timeout -> timeout
    end.

receive_push_packets(Label, Timeout) ->
    receive
        {Label, {'$gen_cast', {push_packets, Packets}}} ->
            {ok, Packets}
    after Timeout ->
        timeout
    end.

wait_for_role_removed(_MapPid, _RolePid, 0) ->
    ?assert(false);
wait_for_role_removed(MapPid, RolePid, Attempts) ->
    State = sys:get_state(MapPid),
    Members = maps:get(members, State),
    case maps:is_key(RolePid, Members) of
        false ->
            State;
        true ->
            receive after 10 -> ok end,
            wait_for_role_removed(MapPid, RolePid, Attempts - 1)
    end.
