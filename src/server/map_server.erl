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

leave(MapPid, RolePid) ->
    call(MapPid, {leave, RolePid}).

move(MapPid, RolePid, Direction) ->
    cast(MapPid, {move, RolePid, Direction}).

teleport(MapPid, RolePid, NewPosition) ->
    cast(MapPid, {teleport, RolePid, NewPosition}).

send_nearby(MapPid, RolePid, RoleName, Content) ->
    cast(MapPid, {send_nearby, RolePid, RoleName, Content}).

send_map(MapPid, RolePid, RoleName, Content) ->
    cast(MapPid, {send_map, RolePid, RoleName, Content}).

debug_state(MapPid) ->
    call(MapPid, debug_state).

debug_role(MapPid, RolePid) ->
    call(MapPid, {debug_role, RolePid}).

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
    put(map_id, MapId),
    put(cells, #{}),
    put(shards, #{}),
    put(map_packets, []),
    put(map_flush_ref, undefined),
    put(operations, #{}),
    {ok, map_server_state}.

handle_call({join, RoleId, RolePid, Position}, _From, _State) ->
    MapId = get(map_id),
    Cells = get(cells),
    Shards = get(shards),
    StartedAt = erlang:monotonic_time(microsecond),
    Reply = case {valid_position(Position), get({role, RolePid})} of
        {false, _} ->
            {error, invalid_position};
        {true, RoleInfo} when RoleInfo =/= undefined ->
            {error, {already_in_map, MapId}};
        {true, undefined} ->
            MonitorRef = erlang:monitor(process, RolePid),
            Shard = position_shard(Position),
            RoleInfo = #{role_id => RoleId,
                         position => Position,
                         shard => Shard,
                         monitor_ref => MonitorRef},
            put({role, RolePid}, RoleInfo),
            put(cells, add_index(Position, RolePid, Cells)),
            put(shards, add_index(Shard, RolePid, Shards)),
            {ok, {MapId, Position}}
    end,
    operation_reply(join, StartedAt, Reply);
handle_call({leave, RolePid}, _From, _State) ->
    MapId = get(map_id),
    Cells = get(cells),
    Shards = get(shards),
    StartedAt = erlang:monotonic_time(microsecond),
    Reply = case get({role, RolePid}) of
        #{position := Position, shard := Shard,
          monitor_ref := MonitorRef} ->
            true = erlang:demonitor(MonitorRef, [flush]),
            erase({role, RolePid}),
            put(cells, remove_index(Position, RolePid, Cells)),
            put(shards, remove_index(Shard, RolePid, Shards)),
            {ok, MapId};
        undefined ->
            {error, not_in_map}
    end,
    operation_reply(leave, StartedAt, Reply);
handle_call(operation_stats, _From, _State) ->
    {reply, get(operations), map_server_state};
handle_call(debug_state, _From, _State) ->
    {reply, debug_state(), map_server_state};
handle_call({debug_role, RolePid}, _From, _State) ->
    {reply, get({role, RolePid}), map_server_state};
handle_call(_Request, _From, _State) ->
    {reply, {error, map_unavailable}, map_server_state}.

handle_cast({move, RolePid, Direction}, _State) ->
    StartedAt = erlang:monotonic_time(microsecond),
    Result = move_member(RolePid, Direction),
    send_result(RolePid, move, Result),
    operation_noreply(move, StartedAt);
handle_cast({teleport, RolePid, NewPosition}, _State) ->
    StartedAt = erlang:monotonic_time(microsecond),
    Result = teleport_member(RolePid, NewPosition),
    send_result(RolePid, teleport, Result),
    operation_noreply(teleport, StartedAt);
handle_cast({send_nearby, RolePid, RoleName, Content},
            _State) ->
    StartedAt = erlang:monotonic_time(microsecond),
    do_send_nearby(RolePid, RoleName, Content),
    operation_noreply(send_nearby, StartedAt);
handle_cast({send_map, RolePid, RoleName, Content},
            _State) ->
    StartedAt = erlang:monotonic_time(microsecond),
    do_send_map(RolePid, RoleName, Content),
    operation_noreply(send_map, StartedAt);
handle_cast(_Request, _State) ->
    {noreply, map_server_state}.

do_send_nearby(RolePid, RoleName, Content) ->
    case get({role, RolePid}) of
        #{role_id := MemberRoleId, position := Position,
          shard := Shard} ->
            {X, Y} = Position,
            Packet = chat_server_protocol:encode_nearby_push(
                MemberRoleId, RoleName, X, Y, Content),
            Targets = nearby_targets(Shard, get(shards)),
            send_result(RolePid, send_nearby, {ok, length(Targets)}),
            push(Targets, Packet),
            ok;
        _ ->
            send_result(RolePid, send_nearby, {error, not_in_map})
    end.

do_send_map(RolePid, RoleName, Content) ->
    MapId = get(map_id),
    case get({role, RolePid}) of
        #{role_id := MemberRoleId} ->
            Packet = chat_server_protocol:encode_map_chat_push(
                MapId, MemberRoleId, RoleName, Content),
            enqueue_map_packet(Packet),
            send_result(RolePid, send_map, {ok, MapId});
        _ ->
            send_result(RolePid, send_map, {error, not_in_map})
    end.

handle_info({timeout, Ref, flush_map_chat},
            _State) ->
    case get(map_flush_ref) of
        Ref ->
            flush_map_chat(),
            {noreply, map_server_state};
        _Other ->
            {noreply, map_server_state}
    end;
handle_info({'DOWN', MonitorRef, process, RolePid, _Reason},
            _State) ->
    case get({role, RolePid}) of
        #{position := Position, shard := Shard,
          monitor_ref := MonitorRef} ->
            erase({role, RolePid}),
            put(cells, remove_index(Position, RolePid, get(cells))),
            put(shards, remove_index(Shard, RolePid, get(shards))),
            {noreply, map_server_state};
        _ ->
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

change_position(_RolePid, NewPosition, #{position := NewPosition}) ->
    {ok, NewPosition};
change_position(RolePid, NewPosition,
                #{position := OldPosition, shard := OldShard} = RoleInfo) ->
    Cells = get(cells),
    NewShard = position_shard(NewPosition),
    NewCells = add_index(
        NewPosition, RolePid,
        remove_index(OldPosition, RolePid, Cells)),
    NewShards = change_shard(RolePid, OldShard, NewShard, get(shards)),
    put({role, RolePid},
        RoleInfo#{position := NewPosition, shard := NewShard}),
    put(cells, NewCells),
    put(shards, NewShards),
    {ok, NewPosition}.

change_shard(_RolePid, Shard, Shard, Shards) ->
    Shards;
change_shard(RolePid, OldShard, NewShard, Shards) ->
    add_index(NewShard, RolePid,
              remove_index(OldShard, RolePid, Shards)).

move_member(RolePid, Direction) ->
    case get({role, RolePid}) of
        undefined ->
            {error, not_in_map};
        #{position := Position} = Member ->
            case move_target(Direction, Position) of
                invalid ->
                    {error, invalid_direction, Position};
                Target ->
                    case valid_position(Target) of
                        true -> change_position(RolePid, Target, Member);
                        false -> {error, out_of_bounds, Position}
                    end
            end
    end.

move_target(up, {X, Y}) -> {X - 1, Y};
move_target(down, {X, Y}) -> {X + 1, Y};
move_target(left, {X, Y}) -> {X, Y - 1};
move_target(right, {X, Y}) -> {X, Y + 1};
move_target(_Direction, _Position) -> invalid.

teleport_member(RolePid, NewPosition) ->
    case get({role, RolePid}) of
        undefined ->
            {error, not_in_map};
        #{position := Position} = Member ->
            case valid_position(NewPosition) of
                true -> change_position(RolePid, NewPosition, Member);
                false -> {error, invalid_position, Position}
            end
    end.

position_shard({X, Y}) ->
    {X div ?SHARD_X_CELLS, Y div ?SHARD_Y_CELLS}.

nearby_targets({ShardX, ShardY}, Shards) ->
    MaxShardX = (?MAP_SIZE - 1) div ?SHARD_X_CELLS,
    MaxShardY = (?MAP_SIZE - 1) div ?SHARD_Y_CELLS,
    NearbyShards =
        [{NearbyX, NearbyY}
         || NearbyX <- lists:seq(erlang:max(0, ShardX - 1),
                                 erlang:min(MaxShardX, ShardX + 1)),
            NearbyY <- lists:seq(erlang:max(0, ShardY - 1),
                                 erlang:min(MaxShardY, ShardY + 1))],
    nearby_targets(NearbyShards, Shards, []).

nearby_targets([], _Shards, Acc) ->
    lists:reverse(Acc);
nearby_targets([Shard | Remaining], Shards, Acc) ->
    RolePids = maps:get(Shard, Shards, []),
    nearby_targets(Remaining, Shards, lists:reverse(RolePids, Acc)).

add_index(Key, RolePid, Index) ->
    case maps:find(Key, Index) of
        {ok, RolePids} ->
            Index#{Key => [RolePid | RolePids]};
        error ->
            Index#{Key => [RolePid]}
    end.

remove_index(Key, RolePid, Index) ->
    case maps:find(Key, Index) of
        {ok, RolePids} ->
            case lists:delete(RolePid, RolePids) of
                [] -> maps:remove(Key, Index);
                Remaining -> Index#{Key => Remaining}
            end;
        error ->
            Index
    end.

push(Targets, Packet) ->
    lists:foreach(
        fun(RolePid) -> gen_server:cast(RolePid, {push_batch, Packet}) end,
        Targets),
    ok.

enqueue_map_packet(Packet) ->
    Packets = get(map_packets),
    put(map_packets, [Packet | Packets]),
    case get(map_flush_ref) of
        undefined ->
            Ref = erlang:start_timer(
                ?MAP_CHAT_BATCH_WINDOW_MS, self(), flush_map_chat),
            put(map_flush_ref, Ref);
        _Ref ->
            ok
    end.

flush_map_chat() ->
    MapId = get(map_id),
    Packets = get(map_packets),
    RolePids = map_role_pids(get(cells)),
    StartedAt = erlang:monotonic_time(microsecond),
    OrderedPackets = lists:reverse(Packets),
    lists:foreach(
        fun(RolePid) ->
            gen_server:cast(RolePid, {push_packets, OrderedPackets})
        end,
        RolePids),
    record_map_batch(MapId, length(Packets), length(RolePids),
                     erlang:monotonic_time(microsecond) - StartedAt),
    put(map_packets, []),
    put(map_flush_ref, undefined),
    ok.

map_role_pids(Cells) ->
    maps:fold(fun(_Position, RolePids, Acc) ->
                      lists:reverse(RolePids, Acc)
              end, [], Cells).

send_result(RolePid, Operation, Result) ->
    gen_server:cast(RolePid, {map_result, self(), Operation, Result}).

operation_reply(Operation, StartedAt, Reply) ->
    record_operation(Operation, StartedAt),
    {reply, Reply, map_server_state}.

operation_noreply(Operation, StartedAt) ->
    record_operation(Operation, StartedAt),
    {noreply, map_server_state}.

record_operation(Operation, StartedAt) ->
    MapId = get(map_id),
    ElapsedUs = erlang:monotonic_time(microsecond) - StartedAt,
    record_map_operation(MapId, Operation, ElapsedUs),
    Operations = get(operations),
    Previous = maps:get(Operation, Operations,
                        #{count => 0, total_us => 0, max_us => 0}),
    Updated = Previous#{count := maps:get(count, Previous) + 1,
                        total_us := maps:get(total_us, Previous) + ElapsedUs,
                        max_us := erlang:max(maps:get(max_us, Previous),
                                             ElapsedUs)},
    put(operations, Operations#{Operation => Updated}),
    ok.

debug_state() ->
    #{map_id => get(map_id),
      cells => get(cells),
      shards => get(shards),
      map_packets => get(map_packets),
      map_flush_ref => get(map_flush_ref),
      operations => get(operations)}.

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
