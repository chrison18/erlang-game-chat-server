-module(world_broadcast_worker).
-behaviour(gen_server).

-include("chat_record.hrl").

-export([start_link/1, send/3, worker_count/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_continue/2, handle_info/2]).

-define(WORLD_CHANNEL_ID, 1).
-define(WORKER_COUNT, 8).
-define(COLLECTOR_WORKER_INDEX, 1).
-define(DELIVERY_WORKER_COUNT, ?WORKER_COUNT - 1).
-define(BATCH_WINDOW_MS, 10).
-define(BATCH_MAX_MESSAGES, 32).

start_link(WorkerIndex) ->
    gen_server:start_link(
        {local, worker_name(WorkerIndex)}, ?MODULE, [], []).

send(RoleId, RoleName, Content) ->
    call_worker(
        worker_name(?COLLECTOR_WORKER_INDEX),
        {broadcast, RoleId, RoleName, Content}).

worker_count() ->
    ?WORKER_COUNT.

init([]) ->
    {ok, #{packets => [], batch_size => 0, flush_ref => undefined}}.

handle_call({broadcast, RoleId, RoleName, Content}, _From, State) ->
    case ets:member(world_channel_members, RoleId) of
        false ->
            {reply, {error, not_joined}, State};
        true ->
            Packet = chat_server_protocol:encode_channel_push(
                ?WORLD_CHANNEL_ID, RoleId, RoleName, Content),
            {NewState, BatchFull} = enqueue(Packet, State),
            case BatchFull of
                true ->
                    {reply, {ok, ?WORLD_CHANNEL_ID}, NewState,
                     {continue, flush_batch}};
                false ->
                    {reply, {ok, ?WORLD_CHANNEL_ID}, NewState}
            end
    end;
handle_call(Request, _From, State) ->
    {reply, {error, {unsupported_call, Request}}, State}.

handle_cast({deliver_batch, BatchPacket, RolePids}, State) ->
    lists:foreach(
        fun(RolePid) ->
            gen_server:cast(RolePid, {push_channel_batch, BatchPacket})
        end,
        RolePids),
    {noreply, State};
handle_cast(_Request, State) ->
    {noreply, State}.

handle_continue(flush_batch, State) ->
    {noreply, flush_batch(State)}.

handle_info({flush_batch, Ref}, #{flush_ref := Ref} = State) ->
    {noreply, flush_batch(State)};
handle_info(_Info, State) ->
    {noreply, State}.

call_worker(Worker, Request) ->
    try gen_server:call(Worker, Request) of
        Reply -> Reply
    catch
        exit:_Reason -> {error, broadcast_failed}
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

flush_batch(#{packets := Packets} = State) ->
    broadcast(lists:reverse(Packets)),
    State#{packets := [], batch_size := 0, flush_ref := undefined}.

broadcast([]) ->
    ok;
broadcast(Packets) ->
    BatchPacket = chat_server_protocol:encode_channel_push_batch(Packets),
    RecipientGroups = ets:foldl(
        fun(#world_channel_member{role_id = RoleId,
                                  role_pid = RolePid}, Groups) ->
            GroupIndex = erlang:phash2(RoleId, ?DELIVERY_WORKER_COUNT) + 1,
            RolePids = element(GroupIndex, Groups),
            setelement(GroupIndex, Groups, [RolePid | RolePids])
        end,
        erlang:make_tuple(?DELIVERY_WORKER_COUNT, []),
        world_channel_members),
    deliver_groups(BatchPacket, RecipientGroups, 1).

deliver_groups(_BatchPacket, _RecipientGroups, GroupIndex)
  when GroupIndex > ?DELIVERY_WORKER_COUNT ->
    ok;
deliver_groups(BatchPacket, RecipientGroups, GroupIndex) ->
    WorkerIndex = GroupIndex + ?COLLECTOR_WORKER_INDEX,
    gen_server:cast(
        worker_name(WorkerIndex),
        {deliver_batch, BatchPacket, element(GroupIndex, RecipientGroups)}),
    deliver_groups(BatchPacket, RecipientGroups, GroupIndex + 1).

worker_name(1) -> world_broadcast_worker_1;
worker_name(2) -> world_broadcast_worker_2;
worker_name(3) -> world_broadcast_worker_3;
worker_name(4) -> world_broadcast_worker_4;
worker_name(5) -> world_broadcast_worker_5;
worker_name(6) -> world_broadcast_worker_6;
worker_name(7) -> world_broadcast_worker_7;
worker_name(8) -> world_broadcast_worker_8.
