-module(role_server_aoi_tests).

-include_lib("eunit/include/eunit.hrl").

current_map_aoi_is_forwarded_and_stale_map_is_ignored_test() ->
    {ListenSocket, ServerSocket, ClientSocket} = open_socket_pair(),
    CurrentMapPid = self(),
    StaleMapPid = spawn(fun wait/0),
    State = #{socket => ServerSocket},
    try
        put(map_pid, CurrentMapPid),
        ?assertEqual(
            {noreply, State},
            role_server:handle_cast(
                {aoi_event, CurrentMapPid, enter, 101}, State)),
        ?assertEqual(
            {ok, {aoi_event, #{event => enter, role_id => 101}}},
            receive_packet(ClientSocket)),

        ?assertEqual(
            {noreply, State},
            role_server:handle_cast(
                {aoi_event, StaleMapPid, leave, 102}, State)),
        ?assertEqual({error, timeout}, gen_tcp:recv(ClientSocket, 0, 100)),

        put(aoi_leave_source, StaleMapPid),
        ?assertEqual(
            {noreply, State},
            role_server:handle_cast(
                {aoi_event, StaleMapPid, leave, 102}, State)),
        ?assertEqual(
            {ok, {aoi_event, #{event => leave, role_id => 102}}},
            receive_packet(ClientSocket)),
        ?assertEqual(
            {noreply, State},
            role_server:handle_cast(
                {clear_aoi_leave_source, StaleMapPid}, State)),
        ?assertEqual(undefined, get(aoi_leave_source))
    after
        erase(map_pid),
        erase(aoi_leave_source),
        exit(StaleMapPid, kill),
        gen_tcp:close(ServerSocket),
        gen_tcp:close(ClientSocket),
        gen_tcp:close(ListenSocket)
    end.

map_join_aoi_reaches_both_tcp_clients_test() ->
    {ok, MapPid} = map_server:start_link(10),
    Role1Sockets = open_socket_pair(),
    Role2Sockets = open_socket_pair(),
    {_Listen1, Server1, Client1} = Role1Sockets,
    {_Listen2, Server2, Client2} = Role2Sockets,
    Role1 = start_role_proxy(MapPid, Server1),
    Role2 = start_role_proxy(MapPid, Server2),
    try
        ?assertEqual(
            {ok, {10, {10, 10}}},
            map_server:join(MapPid, 1, Role1, {10, 10})),
        ?assertEqual(
            {ok, {10, {11, 10}}},
            map_server:join(MapPid, 2, Role2, {11, 10})),
        ?assertEqual(
            {ok, {aoi_event, #{event => enter, role_id => 2}}},
            receive_packet(Client1)),
        ?assertEqual(
            {ok, {aoi_event, #{event => enter, role_id => 1}}},
            receive_packet(Client2))
    after
        stop_role_proxy(Role1),
        stop_role_proxy(Role2),
        close_socket_pair(Role1Sockets),
        close_socket_pair(Role2Sockets),
        gen_server:stop(MapPid)
    end.

open_socket_pair() ->
    Options = [binary, {packet, 4}, {active, false}, {reuseaddr, true}],
    {ok, ListenSocket} = gen_tcp:listen(0, Options),
    {ok, {_Address, Port}} = inet:sockname(ListenSocket),
    {ok, ClientSocket} = gen_tcp:connect("127.0.0.1", Port, Options),
    {ok, ServerSocket} = gen_tcp:accept(ListenSocket),
    {ListenSocket, ServerSocket, ClientSocket}.

receive_packet(Socket) ->
    {ok, Packet} = gen_tcp:recv(Socket, 0, 500),
    chat_client_protocol:decode_packet(Packet).

start_role_proxy(MapPid, Socket) ->
    Parent = self(),
    RolePid = spawn(fun() -> role_proxy_init(Parent, MapPid, Socket) end),
    ok = gen_tcp:controlling_process(Socket, RolePid),
    RolePid ! socket_ready,
    receive
        {role_ready, RolePid} -> RolePid
    after 500 ->
        error(role_proxy_start_timeout)
    end.

role_proxy_init(Parent, MapPid, Socket) ->
    receive
        socket_ready ->
            put(map_pid, MapPid),
            Parent ! {role_ready, self()},
            role_proxy_loop(#{socket => Socket})
    end.

role_proxy_loop(State) ->
    receive
        {'$gen_cast', Message} ->
            {noreply, State} = role_server:handle_cast(Message, State),
            role_proxy_loop(State);
        stop ->
            ok
    end.

stop_role_proxy(RolePid) ->
    RolePid ! stop,
    ok.

close_socket_pair({ListenSocket, ServerSocket, ClientSocket}) ->
    gen_tcp:close(ServerSocket),
    gen_tcp:close(ClientSocket),
    gen_tcp:close(ListenSocket).

wait() ->
    receive
        stop -> ok
    end.
