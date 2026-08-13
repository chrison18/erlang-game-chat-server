-module(nearby_broadcast_worker).
-behaviour(gen_server).

-export([start_link/2, send/4, worker_count/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_continue/2,
         handle_info/2]).

-define(BATCH_WINDOW_MS, 80).
-define(BATCH_MAX_MESSAGES, 256).
-define(WORKER_COUNT, 2).

%% ponytail: two workers per map, sharded by source position.

start_link(MapId, WorkerIndex) ->
    gen_server:start_link(
        {local, worker_name(MapId, WorkerIndex)},
        ?MODULE, [MapId, WorkerIndex], []).

worker_count() ->
    ?WORKER_COUNT.

send(MapId, Position, Packet, Targets) ->
    WorkerIndex = erlang:phash2(Position, worker_count()) + 1,
    case whereis(worker_name(MapId, WorkerIndex)) of
        undefined ->
            BatchPacket = chat_server_protocol:encode_nearby_push_batch([Packet]),
            lists:foreach(
                fun(Target) ->
                    {TargetPid, Writer} = target_connection(Target),
                    role_server:send_push(
                        TargetPid, Writer,
                        BatchPacket, nearby)
                end,
                Targets),
            RolePackets = length(Targets),
            {LogicalSize, WireSize} =
                chat_server_protocol:batch_sizes(BatchPacket),
            chat_metrics:record_broadcast_delivery(
                nearby, 0, RolePackets,
                RolePackets * LogicalSize, RolePackets * WireSize),
            ok;
        WorkerPid ->
            gen_server:cast(
                WorkerPid, {broadcast, Position, Packet, Targets})
    end.

init([MapId, WorkerIndex]) ->
    MetricsKey = {MapId, WorkerIndex},
    _ = ets:insert_new(
        nearby_batch_metrics, {MetricsKey, 0, 0, 0, 0, 0}),
    {ok, #{map_id => MapId,
           metrics_key => MetricsKey,
           batches => #{}}}.

handle_call(Request, _From, State) ->
    {reply, {error, {unsupported_call, Request}}, State}.

handle_cast({broadcast, Position, Packet, Targets},
            #{metrics_key := MetricsKey, batches := Batches} = State) ->
    {NewBatches, BatchFull} = enqueue(
        Position, {Packet, Targets}, Batches),
    record_message(MetricsKey, length(Targets)),
    NewState = State#{batches := NewBatches},
    case BatchFull of
        true ->
            {noreply, NewState, {continue, {flush_batch, Position}}};
        false ->
            {noreply, NewState}
    end;
handle_cast(_Request, State) ->
    {noreply, State}.

handle_continue({flush_batch, Position}, State) ->
    {noreply, flush_batch(Position, State)}.

handle_info({flush_batch, Position, Ref}, #{batches := Batches} = State) ->
    case maps:find(Position, Batches) of
        {ok, #{ref := Ref}} ->
            {noreply, flush_batch(Position, State)};
        _ ->
            {noreply, State}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

enqueue(Position, Item, Batches) ->
    case maps:find(Position, Batches) of
        error ->
            Ref = make_ref(),
            _ = erlang:send_after(
                ?BATCH_WINDOW_MS, self(), {flush_batch, Position, Ref}),
            {Batches#{Position => #{items => [Item], size => 1, ref => Ref}},
             false};
        {ok, #{items := Items, size := Size} = Batch} ->
            NewSize = Size + 1,
            {Batches#{Position := Batch#{items := [Item | Items],
                                         size := NewSize}},
             NewSize >= ?BATCH_MAX_MESSAGES}
    end.

flush_batch(Position,
            #{metrics_key := MetricsKey, batches := Batches} = State) ->
    case maps:take(Position, Batches) of
        {#{items := Items, size := Size}, RemainingBatches} ->
            {RolePackets, LogicalBytes, WireBytes} =
                broadcast(lists:reverse(Items)),
            record_flush(MetricsKey, Size),
            chat_metrics:record_broadcast_delivery(
                nearby, 1, RolePackets, LogicalBytes, WireBytes),
            State#{batches := RemainingBatches};
        error ->
            State
    end.

broadcast(Items) ->
    TargetPackets = lists:foldl(
        fun({Packet, Targets}, Acc) ->
            lists:foldl(
                fun(Target, TargetAcc) ->
                    {TargetPid, Writer} = target_connection(Target),
                    TargetKey = {TargetPid, Writer},
                    maps:update_with(
                        TargetKey, fun(Packets) -> [Packet | Packets] end,
                        [Packet], TargetAcc)
                end,
                Acc,
                Targets)
        end,
        #{},
        Items),
    {_, RolePackets, LogicalBytes, WireBytes} = maps:fold(
        fun({TargetPid, Writer}, ReversedPackets,
            {BatchPackets, PacketCount, LogicalCount, WireCount}) ->
            Packets = lists:reverse(ReversedPackets),
            {{BatchPacket, LogicalSize, WireSize}, NewBatchPackets} =
                batch_packet(
                    Packets, BatchPackets),
            role_server:send_push(
                TargetPid, Writer,
                BatchPacket, nearby),
            {NewBatchPackets, PacketCount + 1,
             LogicalCount + LogicalSize, WireCount + WireSize}
        end,
        {#{}, 0, 0, 0},
        TargetPackets),
    {RolePackets, LogicalBytes, WireBytes}.

target_connection({RolePid, Writer}) when is_pid(RolePid) ->
    {RolePid, Writer};
target_connection(RolePid) when is_pid(RolePid) ->
    {RolePid, role_server:writer(RolePid)}.

batch_packet(Packets, BatchPackets) ->
    case maps:find(Packets, BatchPackets) of
        {ok, BatchInfo} ->
            {BatchInfo, BatchPackets};
        error ->
            BatchPacket = chat_server_protocol:encode_nearby_push_batch(Packets),
            {LogicalSize, WireSize} =
                chat_server_protocol:batch_sizes(BatchPacket),
            BatchInfo = {BatchPacket, LogicalSize, WireSize},
            {BatchInfo, BatchPackets#{Packets => BatchInfo}}
    end.

record_message(MetricsKey, TargetCount) ->
    [{MetricsKey, Messages, Targets, Flushes, BatchMessages, BatchMax}] =
        ets:lookup(nearby_batch_metrics, MetricsKey),
    true = ets:insert(
        nearby_batch_metrics,
        {MetricsKey, Messages + 1, Targets + TargetCount,
         Flushes, BatchMessages, BatchMax}).

record_flush(MetricsKey, Size) ->
    [{MetricsKey, Messages, Targets, Flushes, BatchMessages, BatchMax}] =
        ets:lookup(nearby_batch_metrics, MetricsKey),
    true = ets:insert(
        nearby_batch_metrics,
        {MetricsKey, Messages, Targets, Flushes + 1,
         BatchMessages + Size, erlang:max(BatchMax, Size)}).

worker_name(1, 1) -> nearby_broadcast_worker_1;
worker_name(1, 2) -> nearby_broadcast_worker_1_2;
worker_name(2, 1) -> nearby_broadcast_worker_2;
worker_name(2, 2) -> nearby_broadcast_worker_2_2;
worker_name(3, 1) -> nearby_broadcast_worker_3;
worker_name(3, 2) -> nearby_broadcast_worker_3_2.
