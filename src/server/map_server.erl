-module(map_server).
-behaviour(gen_server).

%% 每张地图一个进程，独占维护地图状态和地图消息。

-include("chat_protocol.hrl").

-export([child_spec/1,
         start_link/1,
         map_ids/0,
         default_map_id/0,
         random_position/0,
         valid_map/1,
         valid_position/1,
         server_name/1,
         pid/1,
         join/4,
         leave/2,
         move/3,
         teleport/3,
         send_nearby/4,
         send_map/4,
         debug_state/1,
         debug_role/2,
         operation_stats/0,
         operation_stats/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-define(CALL_TIMEOUT_MS, 5000).
-define(MAP_CHAT_BATCH_WINDOW_MS, 120).
-define(SHARD_X_CELLS, 2).
-define(SHARD_Y_CELLS, 3).

child_spec(MapId) ->
    #{id => {map_server, MapId},
      start => {?MODULE, start_link, [MapId]}}.

start_link(MapId) ->
    gen_server:start_link({local, server_name(MapId)}, ?MODULE, [MapId], []).

map_ids() ->
    ?MAP_IDS.

default_map_id() ->
    ?DEFAULT_MAP_ID.

random_position() ->
    {rand:uniform(?MAP_SIZE) - 1, rand:uniform(?MAP_SIZE) - 1}.

valid_map(MapId) ->
    lists:member(MapId, ?MAP_IDS).

valid_position({X, Y}) ->
    is_integer(X) andalso X >= 0 andalso X < ?MAP_SIZE andalso
    is_integer(Y) andalso Y >= 0 andalso Y < ?MAP_SIZE;
valid_position(_Position) ->
    false.

server_name(MapId) when is_integer(MapId), MapId >= 1, MapId =< 10 ->
    list_to_atom("map_server_" ++ integer_to_list(MapId)).

pid(MapId) ->
    case valid_map(MapId) of
        true -> whereis(server_name(MapId));
        false -> undefined
    end.

join(MapPid, RoleId, RolePid, Position) ->
    call(MapPid, {join, RoleId, RolePid, Position}).

leave(MapPid, RoleId) ->
    call(MapPid, {leave, RoleId}).

move(MapPid, RoleId, Direction) ->
    cast(MapPid, {move, RoleId, Direction}).

teleport(MapPid, RoleId, NewPosition) ->
    cast(MapPid, {teleport, RoleId, NewPosition}).

send_nearby(MapPid, RoleId, RoleName, Content) ->
    cast(MapPid, {send_nearby, RoleId, RoleName, Content}).

send_map(MapPid, RoleId, RoleName, Content) ->
    cast(MapPid, {send_map, RoleId, RoleName, Content}).

debug_state(MapPid) ->
    call(MapPid, debug_state).

debug_role(MapPid, RoleId) ->
    call(MapPid, {debug_role, RoleId}).

operation_stats() ->
    lists:foldl(fun merge_map_operations/2, #{}, map_ids()).

merge_map_operations(MapId, Operations) ->
    maps:fold(fun merge_operation/3,
              Operations,
              operation_stats(MapId)).

merge_operation(Operation,
                #{count := Count, total_us := TotalUs, max_us := MaxUs},
                Operations) ->
    case maps:find(Operation, Operations) of
        {ok, #{count := OldCount,
               total_us := OldTotalUs,
               max_us := OldMaxUs}} ->
            Operations#{Operation =>
                            #{count => OldCount + Count,
                              total_us => OldTotalUs + TotalUs,
                              max_us => erlang:max(OldMaxUs, MaxUs)}};
        error ->
            Operations#{Operation =>
                            #{count => Count,
                              total_us => TotalUs,
                              max_us => MaxUs}}
    end.

operation_stats(MapId) ->
    case pid(MapId) of
        undefined -> #{};
        MapPid ->
            try gen_server:call(MapPid, operation_stats, ?CALL_TIMEOUT_MS) of
                Stats -> Stats
            catch
                exit:_Reason -> #{}
            end
    end.

init([MapId]) ->
    set_map_id(MapId),
    set_role_ids([]),
    set_map_packets([]),
    set_map_flush_ref(undefined),
    set_operations(#{}),
    {ok, map_server_state}.

handle_call({join, RoleId, RolePid, Position}, _From, _State) ->
    MapId = get_map_id(),
    StartedAt = erlang:monotonic_time(microsecond),
    Reply = case {valid_position(Position), get_role_pid(RoleId)} of
        {false, _} ->
            {error, invalid_position};
        {true, ExistingRolePid} when ExistingRolePid =/= undefined ->
            {error, {already_in_map, MapId}};
        {true, undefined} ->
            MonitorRef = erlang:monitor(process, RolePid),
            Shard = position_shard(Position),
            set_role_pid(RoleId, RolePid),
            set_role_id_by_monitor_ref(MonitorRef, RoleId),
            set_role_position(RoleId, Position),
            set_role_shard(RoleId, Shard),
            set_role_monitor_ref(RoleId, MonitorRef),
            set_role_ids([RoleId | get_role_ids()]),
            add_role_id_to_cell(Position, RoleId),
            add_role_id_to_shard(Shard, RoleId),
            {ok, {MapId, Position}}
    end,
    operation_reply(join, StartedAt, Reply);
handle_call({leave, RoleId}, _From, _State) ->
    MapId = get_map_id(),
    StartedAt = erlang:monotonic_time(microsecond),
    Reply = case get_role_pid(RoleId) of
        RolePid when is_pid(RolePid) ->
            Position = get_role_position(RoleId),
            Shard = get_role_shard(RoleId),
            MonitorRef = get_role_monitor_ref(RoleId),
            true = erlang:demonitor(MonitorRef, [flush]),
            delete_role(RoleId),
            remove_role_id_from_cell(Position, RoleId),
            remove_role_id_from_shard(Shard, RoleId),
            {ok, MapId};
        _ ->
            {error, not_in_map}
    end,
    operation_reply(leave, StartedAt, Reply);
handle_call(operation_stats, _From, _State) ->
    {reply, get_operations(), map_server_state};
handle_call(debug_state, _From, _State) ->
    {reply, debug_state(), map_server_state};
handle_call({debug_role, RoleId}, _From, _State) ->
    {reply, debug_role(RoleId), map_server_state};
handle_call(_Request, _From, _State) ->
    {reply, {error, map_unavailable}, map_server_state}.

handle_cast({move, RoleId, Direction}, _State) ->
    StartedAt = erlang:monotonic_time(microsecond),
    Result = move_member(RoleId, Direction),
    send_result(RoleId, move, Result),
    operation_noreply(move, StartedAt);
handle_cast({teleport, RoleId, NewPosition}, _State) ->
    StartedAt = erlang:monotonic_time(microsecond),
    Result = teleport_member(RoleId, NewPosition),
    send_result(RoleId, teleport, Result),
    operation_noreply(teleport, StartedAt);
handle_cast({send_nearby, RoleId, RoleName, Content},
            _State) ->
    StartedAt = erlang:monotonic_time(microsecond),
    do_send_nearby(RoleId, RoleName, Content),
    operation_noreply(send_nearby, StartedAt);
handle_cast({send_map, RoleId, RoleName, Content},
            _State) ->
    StartedAt = erlang:monotonic_time(microsecond),
    do_send_map(RoleId, RoleName, Content),
    operation_noreply(send_map, StartedAt);
handle_cast(_Request, _State) ->
    {noreply, map_server_state}.

do_send_nearby(RoleId, RoleName, Content) ->
    case get_role_position(RoleId) of
        Position when Position =/= undefined ->
            Shard = get_role_shard(RoleId),
            {X, Y} = Position,
            Packet = chat_server_protocol:encode_nearby_push(
                RoleId, RoleName, X, Y, Content),
            Targets = nearby_targets(Shard),
            send_result(RoleId, send_nearby, {ok, length(Targets)}),
            push(Targets, Packet),
            ok;
        _ ->
            send_result(RoleId, send_nearby, {error, not_in_map})
    end.

do_send_map(RoleId, RoleName, Content) ->
    MapId = get_map_id(),
    case get_role_pid(RoleId) of
        RolePid when is_pid(RolePid) ->
            Packet = chat_server_protocol:encode_map_chat_push(
                MapId, RoleId, RoleName, Content),
            enqueue_map_packet(Packet),
            send_result(RoleId, send_map, {ok, MapId});
        _ ->
            send_result(RoleId, send_map, {error, not_in_map})
    end.

handle_info({timeout, Ref, flush_map_chat},
            _State) ->
    case get_map_flush_ref() of
        Ref ->
            flush_map_chat(),
            {noreply, map_server_state};
        _Other ->
            {noreply, map_server_state}
    end;
handle_info({'DOWN', MonitorRef, process, _RolePid, _Reason},
            _State) ->
    case get_role_id_by_monitor_ref(MonitorRef) of
        RoleId when RoleId =/= undefined ->
            Position = get_role_position(RoleId),
            Shard = get_role_shard(RoleId),
            delete_role(RoleId),
            remove_role_id_from_cell(Position, RoleId),
            remove_role_id_from_shard(Shard, RoleId),
            {noreply, map_server_state};
        undefined ->
            {noreply, map_server_state}
    end;
handle_info(_Info, _State) ->
    {noreply, map_server_state}.

call(undefined, _Request) ->
    {error, map_unavailable};
call(MapPid, Request) when is_pid(MapPid) ->
    try gen_server:call(MapPid, Request, ?CALL_TIMEOUT_MS) of
        Reply -> Reply
    catch
        exit:_Reason -> {error, map_unavailable}
    end.

cast(undefined, _Request) ->
    {error, map_unavailable};
cast(MapPid, Request) when is_pid(MapPid) ->
    gen_server:cast(MapPid, Request).

change_position(_RoleId, NewPosition, NewPosition) ->
    {ok, NewPosition};
change_position(RoleId, NewPosition, OldPosition) ->
    NewShard = position_shard(NewPosition),
    OldShard = get_role_shard(RoleId),
    remove_role_id_from_cell(OldPosition, RoleId),
    add_role_id_to_cell(NewPosition, RoleId),
    change_shard(RoleId, OldShard, NewShard),
    set_role_position(RoleId, NewPosition),
    set_role_shard(RoleId, NewShard),
    {ok, NewPosition}.

change_shard(_RoleId, Shard, Shard) ->
    ok;
change_shard(RoleId, OldShard, NewShard) ->
    remove_role_id_from_shard(OldShard, RoleId),
    add_role_id_to_shard(NewShard, RoleId).

move_member(RoleId, Direction) ->
    case get_role_position(RoleId) of
        undefined ->
            {error, not_in_map};
        Position ->
            case move_target(Direction, Position) of
                invalid ->
                    {error, invalid_direction, Position};
                Target ->
                    case valid_position(Target) of
                        true -> change_position(RoleId, Target, Position);
                        false -> {error, out_of_bounds, Position}
                    end
            end
    end.

move_target(up, {X, Y}) -> {X - 1, Y};
move_target(down, {X, Y}) -> {X + 1, Y};
move_target(left, {X, Y}) -> {X, Y - 1};
move_target(right, {X, Y}) -> {X, Y + 1};
move_target(_Direction, _Position) -> invalid.

teleport_member(RoleId, NewPosition) ->
    case get_role_position(RoleId) of
        undefined ->
            {error, not_in_map};
        Position ->
            case valid_position(NewPosition) of
                true -> change_position(RoleId, NewPosition, Position);
                false -> {error, invalid_position, Position}
            end
    end.

position_shard({X, Y}) ->
    {X div ?SHARD_X_CELLS, Y div ?SHARD_Y_CELLS}.

nearby_targets({ShardX, ShardY}) ->
    MaxShardX = (?MAP_SIZE - 1) div ?SHARD_X_CELLS,
    MaxShardY = (?MAP_SIZE - 1) div ?SHARD_Y_CELLS,
    NearbyShards =
        [{NearbyX, NearbyY}
         || NearbyX <- lists:seq(erlang:max(0, ShardX - 1),
                                 erlang:min(MaxShardX, ShardX + 1)),
            NearbyY <- lists:seq(erlang:max(0, ShardY - 1),
                                 erlang:min(MaxShardY, ShardY + 1))],
    nearby_targets(NearbyShards, []).

nearby_targets([], Acc) ->
    lists:reverse(Acc);
nearby_targets([Shard | Remaining], Acc) ->
    RoleIds = get_role_ids_by_shard(Shard),
    RolePids = [RolePid || RoleId <- RoleIds,
                           RolePid <- [get_role_pid(RoleId)],
                           is_pid(RolePid)],
    nearby_targets(Remaining, lists:reverse(RolePids, Acc)).

add_role_id_to_cell(Cell, RoleId) ->
    set_role_ids_by_cell(Cell, [RoleId | get_role_ids_by_cell(Cell)]).

remove_role_id_from_cell(Cell, RoleId) ->
    case lists:delete(RoleId, get_role_ids_by_cell(Cell)) of
        [] -> delete_role_ids_by_cell(Cell);
        Remaining -> set_role_ids_by_cell(Cell, Remaining)
    end.

add_role_id_to_shard(Shard, RoleId) ->
    set_role_ids_by_shard(Shard, [RoleId | get_role_ids_by_shard(Shard)]).

remove_role_id_from_shard(Shard, RoleId) ->
    case lists:delete(RoleId, get_role_ids_by_shard(Shard)) of
        [] -> delete_role_ids_by_shard(Shard);
        Remaining -> set_role_ids_by_shard(Shard, Remaining)
    end.

get_role_ids_by_cell(Cell) ->
    case erlang:get({cell, Cell}) of
        undefined -> [];
        RoleIds -> RoleIds
    end.

set_role_ids_by_cell(Cell, RoleIds) ->
    erlang:put({cell, Cell}, RoleIds),
    ok.

delete_role_ids_by_cell(Cell) ->
    erlang:erase({cell, Cell}),
    ok.

get_role_ids_by_shard(Shard) ->
    case erlang:get({shard, Shard}) of
        undefined -> [];
        RoleIds -> RoleIds
    end.

set_role_ids_by_shard(Shard, RoleIds) ->
    erlang:put({shard, Shard}, RoleIds),
    ok.

delete_role_ids_by_shard(Shard) ->
    erlang:erase({shard, Shard}),
    ok.

get_role_id_by_monitor_ref(MonitorRef) ->
    erlang:get({role_id, MonitorRef}).

set_role_id_by_monitor_ref(MonitorRef, RoleId) ->
    erlang:put({role_id, MonitorRef}, RoleId),
    ok.

get_role_pid(RoleId) ->
    erlang:get({role_pid, RoleId}).

set_role_pid(RoleId, RolePid) ->
    erlang:put({role_pid, RoleId}, RolePid),
    ok.

get_role_position(RoleId) ->
    erlang:get({position, RoleId}).

set_role_position(RoleId, Position) ->
    erlang:put({position, RoleId}, Position),
    ok.

get_role_shard(RoleId) ->
    erlang:get({role_shard, RoleId}).

set_role_shard(RoleId, Shard) ->
    erlang:put({role_shard, RoleId}, Shard),
    ok.

get_role_monitor_ref(RoleId) ->
    erlang:get({monitor_ref, RoleId}).

set_role_monitor_ref(RoleId, MonitorRef) ->
    erlang:put({monitor_ref, RoleId}, MonitorRef),
    ok.

delete_role(RoleId) ->
    MonitorRef = get_role_monitor_ref(RoleId),
    erlang:erase({role_pid, RoleId}),
    erlang:erase({role_id, MonitorRef}),
    erlang:erase({position, RoleId}),
    erlang:erase({role_shard, RoleId}),
    erlang:erase({monitor_ref, RoleId}),
    set_role_ids(lists:delete(RoleId, get_role_ids())),
    ok.

get_role_ids() ->
    erlang:get(role_ids).

set_role_ids(RoleIds) ->
    erlang:put(role_ids, RoleIds),
    ok.

debug_role(RoleId) ->
    case get_role_pid(RoleId) of
        RolePid when is_pid(RolePid) ->
            #{role_id => RoleId,
              position => get_role_position(RoleId),
              shard => get_role_shard(RoleId),
              monitor_ref => get_role_monitor_ref(RoleId)};
        _ ->
            undefined
    end.

push(Targets, Packet) ->
    lists:foreach(
        fun(RolePid) -> gen_server:cast(RolePid, {push_batch, Packet}) end,
        Targets),
    ok.

enqueue_map_packet(Packet) ->
    Packets = get_map_packets(),
    set_map_packets([Packet | Packets]),
    case get_map_flush_ref() of
        undefined ->
            Ref = erlang:start_timer(
                ?MAP_CHAT_BATCH_WINDOW_MS, self(), flush_map_chat),
            set_map_flush_ref(Ref);
        _Ref ->
            ok
    end.

flush_map_chat() ->
    MapId = get_map_id(),
    Packets = get_map_packets(),
    RolePids = [RolePid || RoleId <- get_role_ids(),
                           RolePid <- [get_role_pid(RoleId)],
                           is_pid(RolePid)],
    StartedAt = erlang:monotonic_time(microsecond),
    OrderedPackets = lists:reverse(Packets),
    lists:foreach(
        fun(RolePid) ->
            gen_server:cast(RolePid, {push_packets, OrderedPackets})
        end,
        RolePids),
    record_map_batch(MapId, length(Packets), length(RolePids),
                     erlang:monotonic_time(microsecond) - StartedAt),
    set_map_packets([]),
    set_map_flush_ref(undefined),
    ok.

send_result(RoleId, Operation, Result) ->
    case get_role_pid(RoleId) of
        RolePid when is_pid(RolePid) ->
            gen_server:cast(RolePid, {map_result, self(), Operation, Result});
        _ ->
            ok
    end.

operation_reply(Operation, StartedAt, Reply) ->
    record_operation(Operation, StartedAt),
    {reply, Reply, map_server_state}.

operation_noreply(Operation, StartedAt) ->
    record_operation(Operation, StartedAt),
    {noreply, map_server_state}.

record_operation(Operation, StartedAt) ->
    MapId = get_map_id(),
    ElapsedUs = erlang:monotonic_time(microsecond) - StartedAt,
    record_map_operation(MapId, Operation, ElapsedUs),
    Operations = get_operations(),
    Previous = maps:get(Operation, Operations,
                        #{count => 0, total_us => 0, max_us => 0}),
    Updated = Previous#{count := maps:get(count, Previous) + 1,
                        total_us := maps:get(total_us, Previous) + ElapsedUs,
                        max_us := erlang:max(maps:get(max_us, Previous),
                                             ElapsedUs)},
    set_operations(Operations#{Operation => Updated}),
    ok.

debug_state() ->
    #{map_id => get_map_id(),
      cells => debug_index(cell),
      shards => debug_index(shard),
      map_packets => get_map_packets(),
      map_flush_ref => get_map_flush_ref(),
      operations => get_operations()}.

debug_index(Type) ->
    maps:from_list(
        [{Key, RoleIds}
         || {{IndexType, Key}, RoleIds} <- erlang:get(),
            IndexType =:= Type]).

get_map_id() ->
    erlang:get(map_id).

set_map_id(MapId) ->
    erlang:put(map_id, MapId),
    ok.

get_map_packets() ->
    erlang:get(map_packets).

set_map_packets(Packets) ->
    erlang:put(map_packets, Packets),
    ok.

get_map_flush_ref() ->
    erlang:get(map_flush_ref).

set_map_flush_ref(Ref) ->
    erlang:put(map_flush_ref, Ref),
    ok.

get_operations() ->
    erlang:get(operations).

set_operations(Operations) ->
    erlang:put(operations, Operations),
    ok.

record_map_operation(MapId, Operation, ElapsedUs) ->
    Key = {MapId, Operation},
    try ets:update_counter(
            map_operation_metrics, Key,
            [{2, 1}, {3, ElapsedUs}],
            {Key, 0, 0}) of
        _ -> ok
    catch
        error:badarg -> ok
    end.

record_map_batch(MapId, BatchSize, RoleCount, ElapsedUs) ->
    try ets:lookup(map_batch_metrics, MapId) of
        [{MapId, Flushes, Messages, MaxBatch, RoleCasts, TotalUs, MaxUs}] ->
            true = ets:insert(
                map_batch_metrics,
                {MapId,
                 Flushes + 1,
                 Messages + BatchSize,
                 erlang:max(MaxBatch, BatchSize),
                 RoleCasts + RoleCount,
                 TotalUs + ElapsedUs,
                 erlang:max(MaxUs, ElapsedUs)}),
            ok;
        [] ->
            true = ets:insert(
                map_batch_metrics,
                {MapId, 1, BatchSize, BatchSize,
                 RoleCount, ElapsedUs, ElapsedUs}),
            ok
    catch
        error:badarg -> ok
    end.
