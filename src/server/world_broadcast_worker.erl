-module(world_broadcast_worker).
-behaviour(gen_server).

-include("chat_record.hrl").

-export([start_link/1, send/3, worker_count/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_continue/2, handle_info/2]).

-define(WORLD_CHANNEL_ID, 1).
-define(BATCH_WINDOW_MS, 120).
-define(BATCH_MAX_MESSAGES, 256).

start_link(WorkerIndex) ->
    gen_server:start_link(
        {local, worker_name(WorkerIndex)}, ?MODULE, [WorkerIndex], []).

send(RoleId, RoleName, Content) ->
    case ets:member(channel_server:world_member_table(RoleId), RoleId) of
        false ->
            {error, not_joined};
        true ->
            case worker_pids() of
                {ok, Workers} ->
                    Packet = chat_server_protocol:encode_channel_push(
                        ?WORLD_CHANNEL_ID, RoleId, RoleName, Content),
                    lists:foreach(
                        fun(Worker) ->
                            gen_server:cast(Worker, {broadcast, Packet})
                        end,
                        Workers),
                    {ok, ?WORLD_CHANNEL_ID};
                error ->
                    {error, broadcast_failed}
            end
    end.

worker_count() ->
    length(channel_server:world_member_tables()).

init([WorkerIndex]) ->
    {ok, #{worker_index => WorkerIndex,
           packets => [],
           batch_size => 0,
           flush_ref => undefined}}.

handle_call(Request, _From, State) ->
    {reply, {error, {unsupported_call, Request}}, State}.

handle_cast({broadcast, Packet}, State) ->
    {NewState, BatchFull} = enqueue(Packet, State),
    case BatchFull of
        true ->
            {noreply, NewState, {continue, flush_batch}};
        false ->
            {noreply, NewState}
    end;
handle_cast(_Request, State) ->
    {noreply, State}.

handle_continue(flush_batch, State) ->
    {noreply, flush_batch(State)}.

handle_info({flush_batch, Ref}, #{flush_ref := Ref} = State) ->
    {noreply, flush_batch(State)};
handle_info(_Info, State) ->
    {noreply, State}.

worker_pids() ->
    Workers = [whereis(worker_name(WorkerIndex))
               || WorkerIndex <- lists:seq(1, worker_count())],
    case lists:all(fun erlang:is_pid/1, Workers) of
        true -> {ok, Workers};
        false -> error
    end.

enqueue(Packet, #{packets := Packets,
                  batch_size := BatchSize,
                  flush_ref := undefined} = State) ->
    Ref = make_ref(),
    _ = erlang:send_after(?BATCH_WINDOW_MS, self(), {flush_batch, Ref}),
    NewSize = BatchSize + 1,
    {State#{packets := [Packet | Packets],
            batch_size := NewSize,
            flush_ref := Ref},
     NewSize >= ?BATCH_MAX_MESSAGES};
enqueue(Packet, #{packets := Packets, batch_size := BatchSize} = State) ->
    NewSize = BatchSize + 1,
    {State#{packets := [Packet | Packets], batch_size := NewSize},
     NewSize >= ?BATCH_MAX_MESSAGES}.

flush_batch(#{worker_index := WorkerIndex, packets := Packets} = State) ->
    broadcast(lists:reverse(Packets), WorkerIndex),
    State#{packets := [], batch_size := 0, flush_ref := undefined}.

broadcast([], _WorkerIndex) ->
    ok;
broadcast(Packets, WorkerIndex) ->
    BatchPacket = chat_server_protocol:encode_channel_push_batch(Packets),
    MemberTable = lists:nth(
        WorkerIndex, channel_server:world_member_tables()),
    RolePackets = ets:foldl(
        fun(#world_channel_member{role_pid = RolePid}, Count) ->
            gen_server:cast(RolePid, {push_batch, BatchPacket}),
            Count + 1
        end,
        0,
        MemberTable),
    chat_metrics:record_broadcast_delivery(
        world, 1, RolePackets, RolePackets * byte_size(BatchPacket)).

worker_name(1) -> world_broadcast_worker_1;
worker_name(2) -> world_broadcast_worker_2;
worker_name(3) -> world_broadcast_worker_3;
worker_name(4) -> world_broadcast_worker_4;
worker_name(5) -> world_broadcast_worker_5;
worker_name(6) -> world_broadcast_worker_6;
worker_name(7) -> world_broadcast_worker_7;
worker_name(8) -> world_broadcast_worker_8.
