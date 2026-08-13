-module(channel_server).
-behaviour(gen_server).

%% 公共频道和地图内部频道共用的成员与批量广播进程。
%% main 频道只维护成员分片，实际广播由 world_broadcast_worker 完成。

-include("chat_protocol.hrl").
-include("chat_record.hrl").

-export([child_spec/1,
         map_child_spec/1,
         channels/0,
         channel/1,
         world_member_tables/0,
         world_member_table/1,
         start_link/2,
         join/3,
         leave/2,
         send_channel/4,
         join_map/3,
         join_map/4,
         leave_map/2,
         leave_map/3,
         send_map/4]).
-export([init/1, handle_call/3, handle_cast/2, handle_continue/2,
         handle_info/2]).

-define(BATCH_WINDOW_MS, 120).
-define(BATCH_MAX_MESSAGES, 256).
-define(REQUEST_DEADLINE_MS, 4000).
-define(CALL_TIMEOUT_MS, 4500).

child_spec(ChannelId) ->
    {ok, ChannelType, _ChannelName} = channel(ChannelId),
    #{id => {channel_server, ChannelId},
      start => {channel_server, start_link,
                [ChannelId, ChannelType]}}.

map_child_spec(MapId) ->
    MapChannelId = {map, MapId},
    #{id => {map_channel_server, MapId},
      start => {channel_server, start_link,
                [MapChannelId, ?CHANNEL_TYPE_PUBLIC]}}.

channels() ->
    [channel_tuple(ChannelId) || ChannelId <- lists:seq(1, 10)].

world_member_tables() ->
    [world_channel_members_1,
     world_channel_members_2,
     world_channel_members_3,
     world_channel_members_4,
     world_channel_members_5,
     world_channel_members_6,
     world_channel_members_7,
     world_channel_members_8].

world_member_table(RoleId) ->
    Tables = world_member_tables(),
    lists:nth(erlang:phash2(RoleId, length(Tables)) + 1, Tables).

channel(1) -> {ok, ?CHANNEL_TYPE_MAIN, <<"main">>};
channel(2) -> {ok, ?CHANNEL_TYPE_PUBLIC, <<"public_1">>};
channel(3) -> {ok, ?CHANNEL_TYPE_PUBLIC, <<"public_2">>};
channel(4) -> {ok, ?CHANNEL_TYPE_PUBLIC, <<"public_3">>};
channel(5) -> {ok, ?CHANNEL_TYPE_PUBLIC, <<"public_4">>};
channel(6) -> {ok, ?CHANNEL_TYPE_PUBLIC, <<"public_5">>};
channel(7) -> {ok, ?CHANNEL_TYPE_PUBLIC, <<"public_6">>};
channel(8) -> {ok, ?CHANNEL_TYPE_PUBLIC, <<"public_7">>};
channel(9) -> {ok, ?CHANNEL_TYPE_PUBLIC, <<"public_8">>};
channel(10) -> {ok, ?CHANNEL_TYPE_PUBLIC, <<"public_9">>};
channel(_ChannelId) -> error.

start_link(ChannelId, Type) ->
    gen_server:start_link(
        {local, server_name(ChannelId)}, ?MODULE, [ChannelId, Type], []).

join(ChannelId, RoleId, RolePid) ->
    case channel(ChannelId) of
        {ok, _ChannelType, _ChannelName} ->
            channel_call(ChannelId, {join, RoleId, RolePid});
        error ->
            {error, invalid_channel}
    end.

leave(1, _RoleId) ->
    {error, cannot_leave_main};
leave(ChannelId, RoleId) ->
    case channel(ChannelId) of
        {ok, _ChannelType, _ChannelName} ->
            channel_call(ChannelId, {leave, RoleId});
        error ->
            {error, invalid_channel}
    end.

send_channel(1, RoleId, RoleName, Content) ->
    world_broadcast_worker:send(RoleId, RoleName, Content);
send_channel(ChannelId, RoleId, RoleName, Content) ->
    case channel(ChannelId) of
        {ok, ?CHANNEL_TYPE_PUBLIC, _ChannelName} ->
            channel_call(
                ChannelId, {send_channel, RoleId, RoleName, Content});
        error ->
            {error, invalid_channel}
    end.

join_map(MapId, RoleId, RolePid) ->
    channel_call({map, MapId}, {join, RoleId, RolePid}).

join_map(MapId, RoleId, RolePid, Deadline) ->
    channel_call({map, MapId}, {join, RoleId, RolePid}, Deadline).

leave_map(MapId, RoleId) ->
    channel_call({map, MapId}, {leave, RoleId}).

leave_map(MapId, RoleId, Deadline) ->
    channel_call({map, MapId}, {leave, RoleId}, Deadline).

send_map(MapId, RoleId, RoleName, Content) ->
    channel_call(
        {map, MapId}, {send_channel, RoleId, RoleName, Content}).

init([ChannelId, Type]) ->
    ok = create_world_members(Type),
    State = #channel_state{
        channel_id = ChannelId,
        channel_type = Type
    },
    case Type of
        ?CHANNEL_TYPE_MAIN ->
            {ok, State};
        ?CHANNEL_TYPE_PUBLIC ->
            %% 普通/地图频道重启后由在线 Role 的本地状态反向恢复成员。
            {ok, State, {continue, recover_members}}
    end.

handle_continue(recover_members,
                #channel_state{channel_id = ChannelId} = State) ->
    lists:foreach(
        fun(#online_role{role_pid = RolePid}) ->
            gen_server:cast(RolePid, {rejoin_channel, ChannelId})
        end,
        ets:tab2list(online_roles)),
    {noreply, State};
handle_continue(flush_batch, State) ->
    {noreply, flush_batch(full, State)}.

handle_call({channel_request, Deadline, Request}, From, State) ->
    %% caller 超时不会撤销请求，出队前检查 deadline 以拒绝迟到副作用。
    case erlang:monotonic_time(millisecond) < Deadline of
        true -> handle_call(Request, From, State);
        false -> {reply, {error, channel_unavailable}, State}
    end;
handle_call({join, RoleId, RolePid}, _From,
            #channel_state{channel_id = ChannelId,
                           channel_type = ChannelType,
                           members = Members,
                           member_monitors = MemberMonitors,
                           batch_generation = BatchGeneration,
                           batch_size = BatchSize} = State) ->
    case maps:is_key(RoleId, Members) of
        true ->
            {reply, {error, already_joined}, State};
        false ->
            %% 新成员从当前 batch_size 开始，只接收加入之后进入本批的消息。
            MonitorRef = erlang:monitor(process, RolePid),
            Member = #channel_member{
                role_pid = RolePid,
                monitor_ref = MonitorRef,
                batch_generation = BatchGeneration,
                batch_start = BatchSize
            },
            ok = add_world_member(ChannelType, RoleId, RolePid),
            {reply, {ok, ChannelId},
             State#channel_state{
                 members = Members#{RoleId => Member},
                 member_monitors = MemberMonitors#{MonitorRef => RoleId}
             }}
    end;
handle_call({leave, RoleId}, _From,
            #channel_state{channel_id = ChannelId,
                           channel_type = ChannelType,
                           members = Members,
                           member_monitors = MemberMonitors} = State) ->
    case maps:take(RoleId, Members) of
        {#channel_member{monitor_ref = MonitorRef} = Member,
         RemainingMembers} ->
            %% 只补发离开者应收的当前批次后缀，不打断其他成员的合批。
            ok = send_pending(State, Member),
            true = erlang:demonitor(MonitorRef, [flush]),
            ok = remove_world_member(ChannelType, RoleId),
            {reply, {ok, ChannelId},
             State#channel_state{
                 members = RemainingMembers,
                 member_monitors = maps:remove(MonitorRef, MemberMonitors)
             }};
        error ->
            {reply, {error, not_joined}, State}
    end;
handle_call({send_channel, RoleId, RoleName, Content}, _From,
            #channel_state{channel_id = ChannelId, members = Members} = State) ->
    case maps:is_key(RoleId, Members) of
        false ->
            {reply, {error, not_joined}, State};
        true ->
            Packet = encode_push(ChannelId, RoleId, RoleName, Content),
            {NewState, BatchFull} = enqueue(Packet, State),
            case BatchFull of
                true ->
                    {reply, {ok, ChannelId}, NewState,
                     {continue, flush_batch}};
                false ->
                    {reply, {ok, ChannelId}, NewState}
            end
    end;
handle_call(Request, _From, State) ->
    {reply, {error, {unsupported_call, Request}}, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({'DOWN', MonitorRef, process, _RolePid, _Reason},
            #channel_state{channel_type = ChannelType,
                           members = Members,
                           member_monitors = MemberMonitors} = State) ->
    case maps:take(MonitorRef, MemberMonitors) of
        {RoleId, RemainingMonitors} ->
            ok = remove_world_member(ChannelType, RoleId),
            {noreply, State#channel_state{
                members = maps:remove(RoleId, Members),
                member_monitors = RemainingMonitors
            }};
        error ->
            {noreply, State}
    end;
handle_info({timeout, Ref, flush_batch},
            #channel_state{flush_ref = Ref} = State) ->
    {noreply, flush_batch(timer, State)};
handle_info(_Info, State) ->
    {noreply, State}.

create_world_members(?CHANNEL_TYPE_MAIN) ->
    lists:foreach(
        fun(Table) ->
            Table = ets:new(Table, [
                named_table,
                set,
                protected,
                {keypos, #world_channel_member.role_id},
                {read_concurrency, true}
            ])
        end,
        world_member_tables()),
    ok;
create_world_members(?CHANNEL_TYPE_PUBLIC) ->
    ok.

add_world_member(?CHANNEL_TYPE_MAIN, RoleId, RolePid) ->
    true = ets:insert(world_member_table(RoleId), #world_channel_member{
        role_id = RoleId,
        role_pid = RolePid
    }),
    ok;
add_world_member(?CHANNEL_TYPE_PUBLIC, _RoleId, _RolePid) ->
    ok.

remove_world_member(?CHANNEL_TYPE_MAIN, RoleId) ->
    true = ets:delete(world_member_table(RoleId), RoleId),
    ok;
remove_world_member(?CHANNEL_TYPE_PUBLIC, _RoleId) ->
    ok.

encode_push({map, MapId}, RoleId, RoleName, Content) ->
    chat_server_protocol:encode_map_chat_push(
        MapId, RoleId, RoleName, Content);
encode_push(ChannelId, RoleId, RoleName, Content) ->
    chat_server_protocol:encode_channel_push(
        ChannelId, RoleId, RoleName, Content).

enqueue(Packet, #channel_state{packets = Packets,
                               batch_size = BatchSize,
                               flush_ref = undefined} = State) ->
    %% 每批首条消息启动 timer；满 256 条时由 continue 立即刷批。
    Ref = erlang:start_timer(?BATCH_WINDOW_MS, self(), flush_batch),
    NewSize = BatchSize + 1,
    {State#channel_state{packets = [Packet | Packets],
                         batch_size = NewSize,
                         flush_ref = Ref},
     NewSize >= ?BATCH_MAX_MESSAGES};
enqueue(Packet, #channel_state{packets = Packets,
                               batch_size = BatchSize} = State) ->
    NewSize = BatchSize + 1,
    {State#channel_state{packets = [Packet | Packets],
                         batch_size = NewSize},
     NewSize >= ?BATCH_MAX_MESSAGES}.

flush_batch(_Reason, #channel_state{packets = []} = State) ->
    cancel_flush_timer(State),
    State#channel_state{batch_size = 0, flush_ref = undefined};
flush_batch(Reason,
            #channel_state{channel_id = ChannelId,
                           members = Members,
                           packets = Packets,
                           batch_size = BatchSize,
                           batch_generation = BatchGeneration} = State) ->
    cancel_flush_timer(State),
    %% generation 每次全批发送后递增，使跨批加入/离开的偏移不会串批。
    OrderedPackets = lists:reverse(Packets),
    {_, RolePackets, PayloadBytes} = maps:fold(
        fun(_RoleId, Member, BatchAcc) ->
            send_batch(ChannelId, BatchGeneration, BatchSize,
                       OrderedPackets, Member, BatchAcc)
        end,
        {#{}, 0, 0},
        Members),
    record_channel_batch(ChannelId, Reason, BatchSize),
    record_map_delivery(ChannelId, 1, RolePackets, PayloadBytes),
    State#channel_state{packets = [],
                        batch_size = 0,
                        batch_generation = BatchGeneration + 1,
                        flush_ref = undefined}.

send_batch(_ChannelId, BatchGeneration, BatchSize, _OrderedPackets,
           #channel_member{batch_generation = BatchGeneration,
                           batch_start = BatchStart}, BatchAcc)
  when BatchStart >= BatchSize ->
    BatchAcc;
send_batch(ChannelId, BatchGeneration, _BatchSize, OrderedPackets,
           #channel_member{role_pid = RolePid,
                           batch_generation = MemberGeneration,
                           batch_start = MemberStart},
           {BatchPackets, RolePackets, PayloadBytes}) ->
    %% 稳定成员从 0 开始；本批中途加入者只取自己 batch_start 后的消息。
    BatchStart = case MemberGeneration =:= BatchGeneration of
        true -> MemberStart;
        false -> 0
    end,
    {BatchPacket, NewBatchPackets} = batch_packet(
        ChannelId, BatchStart, OrderedPackets, BatchPackets),
    gen_server:cast(RolePid, {push_batch, BatchPacket}),
    {NewBatchPackets, RolePackets + 1,
     PayloadBytes + byte_size(BatchPacket)}.

send_pending(#channel_state{channel_id = ChannelId,
                            packets = Packets,
                            batch_size = BatchSize,
                            batch_generation = BatchGeneration},
             #channel_member{role_pid = RolePid,
                             batch_generation = MemberGeneration,
                             batch_start = MemberStart}) ->
    BatchStart = case MemberGeneration =:= BatchGeneration of
        true -> MemberStart;
        false -> 0
    end,
    send_pending_batch(
        ChannelId, RolePid, BatchStart, BatchSize, Packets).

send_pending_batch(ChannelId, RolePid, BatchStart, BatchSize, Packets)
  when BatchStart < BatchSize ->
    BatchPacket = encode_batch(
        ChannelId, lists:nthtail(BatchStart, lists:reverse(Packets))),
    gen_server:cast(RolePid, {push_batch, BatchPacket}),
    record_channel_batch(ChannelId, member_change, BatchSize - BatchStart),
    record_map_delivery(ChannelId, 0, 1, byte_size(BatchPacket)),
    ok;
send_pending_batch(_ChannelId, _RolePid, _BatchStart, _BatchSize, _Packets) ->
    ok.

batch_packet(ChannelId, BatchStart, OrderedPackets, BatchPackets) ->
    case maps:find(BatchStart, BatchPackets) of
        {ok, BatchPacket} ->
            {BatchPacket, BatchPackets};
        error ->
            BatchPacket = encode_batch(
                ChannelId, lists:nthtail(BatchStart, OrderedPackets)),
            {BatchPacket, BatchPackets#{BatchStart => BatchPacket}}
    end.

record_channel_batch(ChannelId, Reason, Size) ->
    Key = {ChannelId, Reason},
    case ets:lookup(channel_batch_metrics, Key) of
        [{Key, Count, Messages, Max}] ->
            true = ets:insert(
                channel_batch_metrics,
                {Key, Count + 1, Messages + Size, erlang:max(Max, Size)});
        [] ->
            true = ets:insert(channel_batch_metrics, {Key, 1, Size, Size})
    end.

record_map_delivery({map, _MapId}, Flushes, RolePackets, PayloadBytes) ->
    chat_metrics:record_broadcast_delivery(
        map, Flushes, RolePackets, PayloadBytes);
record_map_delivery(_ChannelId, _Flushes, _RolePackets, _PayloadBytes) ->
    ok.

cancel_flush_timer(#channel_state{flush_ref = undefined}) ->
    ok;
cancel_flush_timer(#channel_state{flush_ref = Ref}) ->
    _ = erlang:cancel_timer(Ref, [{async, true}, {info, false}]),
    ok.

encode_batch({map, _MapId}, Packets) ->
    chat_server_protocol:encode_map_chat_push_batch(Packets);
encode_batch(_ChannelId, Packets) ->
    chat_server_protocol:encode_channel_push_batch(Packets).

channel_tuple(ChannelId) ->
    {ok, ChannelType, ChannelName} = channel(ChannelId),
    {ChannelId, ChannelType, ChannelName}.

channel_call(ChannelId, Request) ->
    Deadline = erlang:monotonic_time(millisecond) + ?REQUEST_DEADLINE_MS,
    channel_call(ChannelId, Request, Deadline).

channel_call(ChannelId, Request, Deadline) ->
    case whereis(server_name(ChannelId)) of
        undefined ->
            {error, channel_unavailable};
        ChannelPid ->
            channel_call_pid(ChannelPid, Request, Deadline)
    end.

channel_call_pid(ChannelPid, Request, Deadline) ->
    %% 调用固定在开始时解析的 PID，重启后的新实例不会接到这条旧请求。
    RemainingMs = erlang:max(
        0, Deadline - erlang:monotonic_time(millisecond)),
    try gen_server:call(
            ChannelPid, {channel_request, Deadline, Request},
            erlang:min(?CALL_TIMEOUT_MS, RemainingMs + 500)) of
        Reply ->
            Reply
    catch
        exit:{timeout, _Location} ->
            {error, channel_unavailable};
        exit:_Reason ->
            {error, channel_unavailable}
    end.

server_name(1) -> main_channel_server;
server_name(2) -> public_channel_server_1;
server_name(3) -> public_channel_server_2;
server_name(4) -> public_channel_server_3;
server_name(5) -> public_channel_server_4;
server_name(6) -> public_channel_server_5;
server_name(7) -> public_channel_server_6;
server_name(8) -> public_channel_server_7;
server_name(9) -> public_channel_server_8;
server_name(10) -> public_channel_server_9;
server_name({map, 1}) -> map_channel_server_1;
server_name({map, 2}) -> map_channel_server_2;
server_name({map, 3}) -> map_channel_server_3.
