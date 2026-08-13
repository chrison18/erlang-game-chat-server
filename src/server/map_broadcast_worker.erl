-module(map_broadcast_worker).
-behaviour(gen_server).

-include("chat_record.hrl").

-export([start_link/2, worker_count/0, available/1, join/3, leave/7,
         remove/3, broadcast/6]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

start_link(MapId, WorkerIndex) ->
    gen_server:start_link({local, worker_name(MapId, WorkerIndex)}, ?MODULE,
                          [MapId, WorkerIndex], []).

worker_count() -> 8.

available({map, MapId}) ->
    lists:all(fun erlang:is_pid/1,
              [whereis(worker_name(MapId, Index))
               || Index <- lists:seq(1, worker_count())]);
available(_) -> true.

join({map, MapId}, RoleId, Member) ->
    call(MapId, RoleId, {join, RoleId, Member}).

leave({map, MapId}, RoleId, Packets, BatchSize, Generation, Owner, _Deadline) ->
    cast(MapId, RoleId,
         {leave, RoleId, Packets, BatchSize, Generation, Owner}).

remove({map, MapId}, RoleId, Owner) ->
    cast(MapId, RoleId, {remove, RoleId, Owner}).

broadcast({map, MapId}, Generation, BatchSize, Packets, BatchPacket, Owner) ->
    case available({map, MapId}) of
        true ->
            lists:foreach(fun(Index) -> gen_server:cast(
                worker_name(MapId, Index),
                {broadcast, Generation, BatchSize, Packets, BatchPacket, Owner})
            end, lists:seq(1, worker_count())), ok;
        false -> {error, channel_unavailable}
    end.

init([MapId, WorkerIndex]) ->
    Table = lists:nth(WorkerIndex, channel_server:map_member_tables(MapId)),
    Members = maps:from_list([
        {RoleId, Member}
     || #map_channel_member{role_id = RoleId} = Member <- ets:tab2list(Table)]),
    {ok, #{map_id => MapId, worker_index => WorkerIndex, members => Members}}.

handle_call({join, RoleId, Member}, _From, #{members := Members} = State) ->
    {reply, ok, State#{members := Members#{RoleId => Member}}};
handle_call(_Request, _From, State) -> {reply, {error, unsupported_call}, State}.

handle_cast({broadcast, Generation, BatchSize, Packets, BatchPacket, Owner},
            #{members := Members} = State) ->
    broadcast_members(
        Members, Generation, BatchSize, Packets, BatchPacket, Owner),
    {noreply, State};
handle_cast({leave, RoleId, Packets, BatchSize, Generation, Owner},
            #{map_id := MapId, members := Members} = State) ->
    case maps:take(RoleId, Members) of
        {#map_channel_member{owner_pid = Owner} = Member, RemainingMembers} ->
            send_pending(MapId, Member, Packets, BatchSize, Generation),
            {noreply, State#{members := RemainingMembers}};
        {Member, _} ->
            {noreply, State#{members := Members#{RoleId => Member}}};
        error ->
            {noreply, State}
    end;
handle_cast({remove, RoleId, Owner}, #{members := Members} = State) ->
    case maps:find(RoleId, Members) of
        {ok, #map_channel_member{owner_pid = Owner}} ->
            {noreply, State#{members := maps:remove(RoleId, Members)}};
        _ ->
            {noreply, State}
    end;
handle_cast(_Request, State) -> {noreply, State}.

handle_info(_Info, State) -> {noreply, State}.

call(MapId, RoleId, Request) ->
    case whereis(worker_name(MapId, shard(RoleId))) of
        undefined -> {error, channel_unavailable};
        Pid ->
            try gen_server:call(Pid, Request) of Reply -> Reply
            catch exit:_ -> {error, channel_unavailable}
            end
    end.

cast(MapId, RoleId, Request) ->
    case whereis(worker_name(MapId, shard(RoleId))) of
        undefined -> {error, channel_unavailable};
        Pid -> gen_server:cast(Pid, Request), ok
    end.

shard(RoleId) -> erlang:phash2(RoleId, worker_count()) + 1.

broadcast_members(Members, Generation, BatchSize, Packets, BatchPacket, Owner) ->
    {LogicalSize, WireSize} = chat_server_protocol:batch_sizes(BatchPacket),
    Initial = {#{0 => {BatchPacket, LogicalSize, WireSize}}, 0, 0, 0},
    {_, Count, LogicalBytes, WireBytes} =
        maps:fold(
            fun(_RoleId, Member, Acc) ->
                send_batch(
                    Member, Owner, Generation, BatchSize, Packets, Acc)
            end,
            Initial,
            Members),
    chat_metrics:record_broadcast_delivery(
        map, 0, Count, LogicalBytes, WireBytes),
    ok.

send_batch(#map_channel_member{owner_pid = MemberOwner}, Owner,
           _Generation, _BatchSize, _Packets, Acc)
  when MemberOwner =/= Owner ->
    Acc;
send_batch(#map_channel_member{batch_generation = MemberGeneration}, _Owner,
           Generation, _BatchSize, _Packets, Acc)
  when MemberGeneration > Generation ->
    Acc;
send_batch(#map_channel_member{batch_generation = Generation,
                               batch_start = BatchStart}, _Owner,
           Generation, BatchSize, _Packets, Acc)
  when BatchStart >= BatchSize ->
    Acc;
send_batch(#map_channel_member{role_pid = RolePid,
                               writer = Writer,
                               batch_generation = MemberGeneration,
                               batch_start = MemberStart}, _Owner,
           Generation, _BatchSize, Packets,
           {BatchPackets, Count, LogicalBytes, WireBytes}) ->
    BatchStart = case MemberGeneration =:= Generation of
        true -> MemberStart;
        false -> 0
    end,
    {{Packet, LogicalSize, WireSize}, NewBatchPackets} =
        batch_packet(BatchStart, Packets, BatchPackets),
    ok = role_server:send_push(RolePid, Writer, Packet, map),
    {NewBatchPackets, Count + 1,
     LogicalBytes + LogicalSize, WireBytes + WireSize}.

batch_packet(BatchStart, Packets, BatchPackets) ->
    case maps:find(BatchStart, BatchPackets) of
        {ok, BatchInfo} ->
            {BatchInfo, BatchPackets};
        error ->
            Packet = chat_server_protocol:encode_map_chat_push_batch(
                lists:nthtail(BatchStart, Packets)),
            {LogicalSize, WireSize} =
                chat_server_protocol:batch_sizes(Packet),
            BatchInfo = {Packet, LogicalSize, WireSize},
            {BatchInfo, BatchPackets#{BatchStart => BatchInfo}}
    end.

send_pending(_MapId, Member, Packets, BatchSize, Generation) ->
    case packets_for(Member, Generation, BatchSize, lists:reverse(Packets)) of
        [] -> ok;
        MemberPackets ->
            Packet = chat_server_protocol:encode_map_chat_push_batch(MemberPackets),
            ok = role_server:send_push(Member#map_channel_member.role_pid,
                                       Member#map_channel_member.writer, Packet, map),
            {LogicalSize, WireSize} = chat_server_protocol:batch_sizes(Packet),
            chat_metrics:record_broadcast_delivery(
                map, 0, 1, LogicalSize, WireSize)
    end.

packets_for(#map_channel_member{batch_generation = MemberGeneration}, Generation,
            _BatchSize, _Packets) when MemberGeneration > Generation -> [];
packets_for(#map_channel_member{batch_generation = Generation,
                                batch_start = Start}, Generation, BatchSize, Packets)
  when Start < BatchSize -> lists:nthtail(Start, Packets);
packets_for(#map_channel_member{batch_generation = Generation}, Generation,
            _BatchSize, _Packets) -> [];
packets_for(_Member, _Generation, _BatchSize, Packets) -> Packets.

worker_name(1, 1) -> map_broadcast_worker_1_1;
worker_name(1, 2) -> map_broadcast_worker_1_2;
worker_name(1, 3) -> map_broadcast_worker_1_3;
worker_name(1, 4) -> map_broadcast_worker_1_4;
worker_name(1, 5) -> map_broadcast_worker_1_5;
worker_name(1, 6) -> map_broadcast_worker_1_6;
worker_name(1, 7) -> map_broadcast_worker_1_7;
worker_name(1, 8) -> map_broadcast_worker_1_8;
worker_name(2, 1) -> map_broadcast_worker_2_1;
worker_name(2, 2) -> map_broadcast_worker_2_2;
worker_name(2, 3) -> map_broadcast_worker_2_3;
worker_name(2, 4) -> map_broadcast_worker_2_4;
worker_name(2, 5) -> map_broadcast_worker_2_5;
worker_name(2, 6) -> map_broadcast_worker_2_6;
worker_name(2, 7) -> map_broadcast_worker_2_7;
worker_name(2, 8) -> map_broadcast_worker_2_8;
worker_name(3, 1) -> map_broadcast_worker_3_1;
worker_name(3, 2) -> map_broadcast_worker_3_2;
worker_name(3, 3) -> map_broadcast_worker_3_3;
worker_name(3, 4) -> map_broadcast_worker_3_4;
worker_name(3, 5) -> map_broadcast_worker_3_5;
worker_name(3, 6) -> map_broadcast_worker_3_6;
worker_name(3, 7) -> map_broadcast_worker_3_7;
worker_name(3, 8) -> map_broadcast_worker_3_8.
