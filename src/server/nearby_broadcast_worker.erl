-module(nearby_broadcast_worker).
-behaviour(gen_server).

-export([start_link/1, send/4]).
-export([init/1, handle_call/3, handle_cast/2, handle_continue/2,
         handle_info/2]).

-define(BATCH_WINDOW_MS, 80).
-define(BATCH_MAX_MESSAGES, 256).

%% ponytail: one worker per map; shard by source cell if its mailbox grows.

start_link(MapId) ->
    gen_server:start_link({local, worker_name(MapId)}, ?MODULE, [MapId], []).

send(MapId, Position, Packet, Targets) ->
    case whereis(worker_name(MapId)) of
        undefined ->
            BatchPacket = chat_server_protocol:encode_nearby_push_batch([Packet]),
            lists:foreach(
                fun(TargetPid) ->
                    gen_server:cast(TargetPid, {push_batch, BatchPacket})
                end,
                Targets),
            RolePackets = length(Targets),
            chat_metrics:record_broadcast_delivery(
                nearby, 0, RolePackets,
                RolePackets * byte_size(BatchPacket)),
            ok;
        WorkerPid ->
            gen_server:cast(
                WorkerPid, {broadcast, Position, Packet, Targets})
    end.

init([MapId]) ->
    _ = ets:insert_new(
        nearby_batch_metrics, {MapId, 0, 0, 0, 0, 0}),
    {ok, #{map_id => MapId, batches => #{}}}.

handle_call(Request, _From, State) ->
    {reply, {error, {unsupported_call, Request}}, State}.

handle_cast({broadcast, Position, Packet, Targets},
            #{map_id := MapId, batches := Batches} = State) ->
    {NewBatches, BatchFull} = enqueue(
        Position, {Packet, Targets}, Batches),
    record_message(MapId, length(Targets)),
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

flush_batch(Position, #{map_id := MapId, batches := Batches} = State) ->
    case maps:take(Position, Batches) of
        {#{items := Items, size := Size}, RemainingBatches} ->
            {RolePackets, PayloadBytes} = broadcast(lists:reverse(Items)),
            record_flush(MapId, Size),
            chat_metrics:record_broadcast_delivery(
                nearby, 1, RolePackets, PayloadBytes),
            State#{batches := RemainingBatches};
        error ->
            State
    end.

broadcast(Items) ->
    TargetPackets = lists:foldl(
        fun({Packet, Targets}, Acc) ->
            lists:foldl(
                fun(TargetPid, TargetAcc) ->
                    maps:update_with(
                        TargetPid, fun(Packets) -> [Packet | Packets] end,
                        [Packet], TargetAcc)
                end,
                Acc,
                Targets)
        end,
        #{},
        Items),
    {_, RolePackets, PayloadBytes} = maps:fold(
        fun(TargetPid, ReversedPackets,
            {BatchPackets, PacketCount, ByteCount}) ->
            Packets = lists:reverse(ReversedPackets),
            {BatchPacket, NewBatchPackets} = batch_packet(
                Packets, BatchPackets),
            gen_server:cast(TargetPid, {push_batch, BatchPacket}),
            {NewBatchPackets, PacketCount + 1,
             ByteCount + byte_size(BatchPacket)}
        end,
        {#{}, 0, 0},
        TargetPackets),
    {RolePackets, PayloadBytes}.

batch_packet(Packets, BatchPackets) ->
    case maps:find(Packets, BatchPackets) of
        {ok, BatchPacket} ->
            {BatchPacket, BatchPackets};
        error ->
            BatchPacket = chat_server_protocol:encode_nearby_push_batch(Packets),
            {BatchPacket, BatchPackets#{Packets => BatchPacket}}
    end.

record_message(MapId, TargetCount) ->
    [{MapId, Messages, Targets, Flushes, BatchMessages, BatchMax}] =
        ets:lookup(nearby_batch_metrics, MapId),
    true = ets:insert(
        nearby_batch_metrics,
        {MapId, Messages + 1, Targets + TargetCount,
         Flushes, BatchMessages, BatchMax}).

record_flush(MapId, Size) ->
    [{MapId, Messages, Targets, Flushes, BatchMessages, BatchMax}] =
        ets:lookup(nearby_batch_metrics, MapId),
    true = ets:insert(
        nearby_batch_metrics,
        {MapId, Messages, Targets, Flushes + 1,
         BatchMessages + Size, erlang:max(BatchMax, Size)}).

worker_name(1) -> nearby_broadcast_worker_1;
worker_name(2) -> nearby_broadcast_worker_2;
worker_name(3) -> nearby_broadcast_worker_3.
