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
    check_channel_flow(Alice, AliceId, AliceChannels, Bob, BobId),
    check_role_packet_batch(Alice, AliceId),
    check_private_flow(Alice, AliceId, Bob, BobId),
    check_worker_recovery(Alice, AliceId),
    gen_tcp:close(Bob),
    wait_until(fun() -> ets:info(online_roles, size) =:= 1 end),
    wait_until(fun() -> ets:info(world_channel_members, size) =:= 1 end),
    send(Alice, chat_client_protocol:encode_private_send(
        <<"bob">>, <<"after-close">>)),
    {private_send_result, {error, target_offline, <<"bob">>}} = recv(Alice),
    check_real_client(Port),
    check_main_restart(Alice),
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
    send(Socket, <<?PROTO_CHANNEL_JOIN_REQUEST:16, 1:8>>),
    {server_error, ?PROTO_CHANNEL_JOIN_REQUEST, invalid_packet} = recv(Socket),
    send(Socket, <<7777:16>>),
    {server_error, 7777, unknown_proto} = recv(Socket),
    gen_tcp:close(Socket).

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
    gen_server:cast(AliceRolePid, {push_channel_batch, BatchPacket}),
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
        <<?PROTO_CHANNEL_PUSH_BATCH:16, 1:16, 8:32, 1:8>>).

check_worker_recovery(Alice, AliceId) ->
    WorkerId = {world_broadcast_worker, 1},
    ok = supervisor:terminate_child(channel_sup, WorkerId),
    send(Alice, chat_client_protocol:encode_channel_send(1, <<"worker-down">>)),
    {channel_send_result, {error, broadcast_failed, 1}} = recv(Alice),
    {ok, _WorkerPid} = supervisor:restart_child(channel_sup, WorkerId),
    send(Alice, chat_client_protocol:encode_channel_send(1, <<"worker-up">>)),
    {channel_send_result, {ok, 1}} = recv(Alice),
    expect_channel_push(Alice, 1, AliceId, <<"alice">>, <<"worker-up">>).

check_real_client(Port) ->
    {Sink, _SinkId, _SinkChannels} = login(Port, <<"sink">>, <<"pw">>),
    {ok, ClientSup} = chat_client_sup:start_link(),
    unlink(ClientSup),
    {ok, ClientPid} = chat_client_sup:start_client(
        101, "127.0.0.1", Port, <<"client_101">>, <<"123456">>,
        {normal, 101, 101, 101}),
    wait_until(fun() -> ets:info(online_roles, size) =:= 3 end),
    State = sys:get_state(ClientPid),
    101 = maps:get(client_id, State),
    {101, 101} = maps:get(client_range, State),
    ClientChannels = maps:get(channel_ids, State),
    check_initial_channels(ClientChannels),
    false = lists:any(
        fun(Key) -> maps:is_key(Key, State) end,
        [status, role_id, password, action_state]),
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
    wait_until(fun() ->
        maps:get(action_seq, sys:get_state(ClientPid)) >= 2
    end),
    ok = gen_server:stop(ClientPid),
    gen_tcp:close(Sink),
    exit(ClientSup, shutdown),
    wait_until(fun() -> ets:info(online_roles, size) =:= 1 end).

check_main_restart(Alice) ->
    OldMain = whereis(main_channel_server),
    OldChannelSup = whereis(channel_sup),
    OldRoleSup = whereis(role_sup),
    OldListener = whereis(chat_listener),
    exit(OldMain, kill),
    wait_until(fun() -> changed(main_channel_server, OldMain) end),
    wait_until(fun() -> changed(channel_sup, OldChannelSup) end),
    wait_until(fun() -> changed(role_sup, OldRoleSup) end),
    wait_until(fun() -> changed(chat_listener, OldListener) end),
    wait_socket_closed(Alice, 20),
    wait_until(fun() ->
        Snapshot = chat_metrics:snapshot(),
        maps:get(online_count, Snapshot) =:= 0 andalso
        maps:get(world_member_count, Snapshot) =:= 0 andalso
        maps:get(role_count, Snapshot) =:= 0 andalso
        maps:get(channel_count, Snapshot) =:= 10 andalso
        maps:get(world_worker_count, Snapshot) =:= 8
    end),
    true = whereis(main_channel_server) =:=
        ets:info(world_channel_members, owner).

check_restart_accepts_login(Port) ->
    {Socket, _RoleId, _Channels} = login(Port, <<"after_restart">>, <<"pw">>),
    gen_tcp:close(Socket),
    wait_until(fun() -> ets:info(online_roles, size) =:= 0 end).

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

login(Port, RoleName, Password) ->
    Socket = connect(Port),
    send(Socket, chat_client_protocol:encode_login(RoleName, Password)),
    {login_result, {ok, RoleId, ChannelIds}} = recv(Socket),
    {Socket, RoleId, ChannelIds}.

connect(Port) ->
    {ok, Socket} = gen_tcp:connect(
        "127.0.0.1", Port,
        [binary, {packet, 4}, {active, false}], ?TIMEOUT),
    Socket.

send(Socket, Packet) ->
    ok = gen_tcp:send(Socket, Packet).

recv(Socket) ->
    {ok, Packet} = gen_tcp:recv(Socket, 0, ?TIMEOUT),
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
