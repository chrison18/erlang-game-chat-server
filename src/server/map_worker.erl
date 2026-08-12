-module(map_worker).
-behaviour(gen_server).

-export([child_spec/1,
         start_link/1,
         join/4,
         leave/3,
         relocate/3,
         nearby/2,
         operation_stats/1,
         server_name/1,
         cell_table/1,
         metrics_table/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-define(POSITION_TABLE, map_role_positions).
-define(REQUEST_DEADLINE_MS, 4000).
-define(CALL_TIMEOUT_MS, 5000).

child_spec(MapId) ->
    #{id => {map_worker, MapId},
      start => {?MODULE, start_link, [MapId]}}.

start_link(MapId) ->
    gen_server:start_link({local, server_name(MapId)}, ?MODULE, [MapId], []).

join(MapId, RoleId, RolePid, Position) ->
    worker_call(MapId, {join, RoleId, RolePid, Position}).

leave(MapId, RoleId, RolePid) ->
    worker_call(MapId, {leave, RoleId, RolePid}).

relocate(MapId, RolePid, NewPosition) ->
    worker_call(MapId, {relocate, RolePid, NewPosition}).

nearby(MapId, {X, Y}) ->
    Table = cell_table(MapId),
    Coordinates = [{MapId, NearbyX, NearbyY}
                   || NearbyX <- lists:seq(erlang:max(0, X - 1),
                                           erlang:min(99, X + 1)),
                      NearbyY <- lists:seq(erlang:max(0, Y - 1),
                                           erlang:min(99, Y + 1))],
    lists:usort([RolePid
                 || Coordinate <- Coordinates,
                    {_StoredCoordinate, RolePid} <- ets:lookup(
                        Table, Coordinate)]).

operation_stats(MapId) ->
    maps:from_list([
        {Operation, #{count => Count,
                      total_us => TotalUs,
                      max_us => MaxUs}}
     || {Operation, Count, TotalUs, MaxUs} <- table_rows(
            metrics_table(MapId))
    ]).

init([MapId]) ->
    CellTable = cell_table(MapId),
    true = ets:match_delete(CellTable, {{MapId, '_', '_'}, '_'}),
    Monitors = recover_roles(MapId, CellTable),
    {ok, #{map_id => MapId, monitors => Monitors}}.

handle_call({map_request, Deadline, {join, RoleId, RolePid, Position}},
            From, State) ->
    case erlang:monotonic_time(millisecond) < Deadline of
        true ->
            handle_call(
                {join, RoleId, RolePid, Position, Deadline}, From, State);
        false -> {reply, {error, map_unavailable}, State}
    end;
handle_call({map_request, Deadline, {leave, RoleId, RolePid}}, From, State) ->
    case erlang:monotonic_time(millisecond) < Deadline of
        true -> handle_call({leave, RoleId, RolePid, Deadline}, From, State);
        false -> {reply, {error, map_unavailable}, State}
    end;
handle_call({map_request, Deadline, Request}, From, State) ->
    case erlang:monotonic_time(millisecond) < Deadline of
        true -> handle_call(Request, From, State);
        false -> {reply, {error, map_unavailable}, State}
    end;

handle_call({join, RoleId, RolePid, Position, Deadline}, _From,
            #{map_id := MapId, monitors := Monitors} = State) ->
    StartedAt = erlang:monotonic_time(microsecond),
    Location = {MapId, Position},
    case map_router:valid_position(Position) of
        false ->
            operation_reply(
                MapId, join, StartedAt, {error, invalid_position}, State);
        true ->
            Pending = {pending, MapId, Position, RoleId},
            case ets:insert_new(?POSITION_TABLE, {RolePid, Pending}) of
                false ->
                    case ets:lookup(?POSITION_TABLE, RolePid) of
                        [{RolePid, {CurrentMapId, _CurrentPosition}}] ->
                            operation_reply(
                                MapId, join, StartedAt,
                                {error, {already_in_map, CurrentMapId}},
                                State);
                        [{RolePid, {pending, _CurrentMapId,
                                    _CurrentPosition, _CurrentRoleId}}] ->
                            operation_reply(
                                MapId, join, StartedAt,
                                {error, map_unavailable}, State)
                    end;
                true ->
                    case channel_server:join_map(
                             MapId, RoleId, RolePid, Deadline) of
                        {ok, {map, MapId}} ->
                            true = ets:insert(
                                ?POSITION_TABLE, {RolePid, Location}),
                            true = ets:insert(
                                cell_table(MapId),
                                {cell_key(Location), RolePid}),
                            {NewMonitors, _MonitorRef} =
                                ensure_monitor(RolePid, Monitors),
                            operation_reply(
                                MapId, join, StartedAt, {ok, Location},
                                State#{monitors := NewMonitors});
                        {error, channel_unavailable} ->
                            true = ets:delete(?POSITION_TABLE, RolePid),
                            operation_reply(
                                MapId, join, StartedAt,
                                {error, map_unavailable}, State)
                    end
            end
    end;
handle_call({leave, RoleId, RolePid, Deadline}, _From,
            #{map_id := MapId, monitors := Monitors} = State) ->
    StartedAt = erlang:monotonic_time(microsecond),
    case ets:lookup(?POSITION_TABLE, RolePid) of
        [] ->
            operation_reply(
                MapId, leave, StartedAt, {error, not_in_map}, State);
        [{RolePid, {MapId, _Position} = Location}] ->
            case channel_server:leave_map(MapId, RoleId, Deadline) of
                {ok, {map, MapId}} ->
                    ok = remove_location(MapId, RolePid, Location),
                    operation_reply(
                        MapId, leave, StartedAt, {ok, MapId},
                        State#{monitors := drop_monitor(RolePid, Monitors)});
                {error, not_joined} ->
                    ok = remove_location(MapId, RolePid, Location),
                    operation_reply(
                        MapId, leave, StartedAt, {ok, MapId},
                        State#{monitors := drop_monitor(RolePid, Monitors)});
                {error, channel_unavailable} ->
                    operation_reply(
                        MapId, leave, StartedAt,
                        {error, {map_unavailable, MapId}}, State)
            end
    end;
handle_call({relocate, RolePid, NewPosition}, _From,
            #{map_id := MapId} = State) ->
    StartedAt = erlang:monotonic_time(microsecond),
    case {map_router:valid_position(NewPosition),
          ets:lookup(?POSITION_TABLE, RolePid)} of
        {false, _} ->
            operation_reply(
                MapId, relocate, StartedAt,
                {error, invalid_position}, State);
        {true, []} ->
            operation_reply(
                MapId, relocate, StartedAt, {error, not_in_map}, State);
        {true, [{RolePid, {MapId, NewPosition}}]} ->
            operation_reply(
                MapId, relocate, StartedAt, {ok, NewPosition}, State);
        {true, [{RolePid, {MapId, _OldPosition} = OldLocation}]} ->
            NewLocation = {MapId, NewPosition},
            true = ets:delete_object(
                cell_table(MapId), {cell_key(OldLocation), RolePid}),
            true = ets:insert(?POSITION_TABLE, {RolePid, NewLocation}),
            true = ets:insert(
                cell_table(MapId), {cell_key(NewLocation), RolePid}),
            operation_reply(
                MapId, relocate, StartedAt, {ok, NewPosition}, State)
    end;
handle_call(Request, _From, State) ->
    {reply, {error, {unsupported_call, Request}}, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({'DOWN', MonitorRef, process, RolePid, _Reason},
            #{map_id := MapId, monitors := Monitors} = State) ->
    case maps:take(RolePid, Monitors) of
        {MonitorRef, RemainingMonitors} ->
            remove_role(MapId, RolePid),
            {noreply, State#{monitors := RemainingMonitors}};
        {_OtherRef, _RemainingMonitors} ->
            {noreply, State};
        error ->
            {noreply, State}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

ensure_monitor(RolePid, Monitors) ->
    case maps:find(RolePid, Monitors) of
        {ok, MonitorRef} ->
            {Monitors, MonitorRef};
        error ->
            MonitorRef = erlang:monitor(process, RolePid),
            {Monitors#{RolePid => MonitorRef}, MonitorRef}
    end.

drop_monitor(RolePid, Monitors) ->
    case maps:take(RolePid, Monitors) of
        {MonitorRef, RemainingMonitors} ->
            true = erlang:demonitor(MonitorRef, [flush]),
            RemainingMonitors;
        error ->
            Monitors
    end.

remove_role(MapId, RolePid) ->
    true = ets:match_delete(
        cell_table(MapId), {{MapId, '_', '_'}, RolePid}),
    case ets:lookup(?POSITION_TABLE, RolePid) of
        [{RolePid, {MapId, _Position}}] ->
            true = ets:delete(?POSITION_TABLE, RolePid);
        _ ->
            ok
    end,
    ok.

remove_location(MapId, RolePid, Location) ->
    true = ets:delete(?POSITION_TABLE, RolePid),
    true = ets:delete_object(
        cell_table(MapId), {cell_key(Location), RolePid}),
    ok.

cell_key({MapId, {X, Y}}) ->
    {MapId, X, Y}.

operation_reply(MapId, Operation, StartedAt, Reply, State) ->
    ElapsedUs = erlang:monotonic_time(microsecond) - StartedAt,
    Table = metrics_table(MapId),
    case ets:lookup(Table, Operation) of
        [{Operation, Count, TotalUs, MaxUs}] ->
            true = ets:insert(
                Table,
                {Operation, Count + 1, TotalUs + ElapsedUs,
                 erlang:max(MaxUs, ElapsedUs)});
        [] ->
            true = ets:insert(
                Table, {Operation, 1, ElapsedUs, ElapsedUs})
    end,
    {reply, Reply, State}.

table_rows(Table) ->
    case ets:info(Table) of
        undefined -> [];
        _ -> ets:tab2list(Table)
    end.

worker_call(MapId, Request) ->
    case whereis(server_name(MapId)) of
        undefined ->
            {error, map_unavailable};
        WorkerPid ->
            Deadline = erlang:monotonic_time(millisecond) +
                       ?REQUEST_DEADLINE_MS,
            try gen_server:call(
                    WorkerPid, {map_request, Deadline, Request},
                    ?CALL_TIMEOUT_MS) of
                Reply ->
                    Reply
            catch
                exit:_Reason ->
                    {error, map_unavailable}
            end
    end.

recover_roles(MapId, CellTable) ->
    lists:foldl(
        fun({RolePid, {RoleMapId, _Position} = Location}, Monitors)
              when RoleMapId =:= MapId ->
            true = ets:insert(CellTable, {cell_key(Location), RolePid}),
            gen_server:cast(RolePid, {rejoin_channel, {map, MapId}}),
            {NewMonitors, _MonitorRef} = ensure_monitor(RolePid, Monitors),
            NewMonitors;
           ({RolePid, {pending, PendingMapId, _Position, RoleId}}, Monitors)
              when PendingMapId =:= MapId ->
            _ = channel_server:leave_map(MapId, RoleId),
            true = ets:delete(?POSITION_TABLE, RolePid),
            Monitors;
           ({_RolePid, _OtherLocation}, Monitors) ->
            Monitors
        end,
        #{},
        ets:tab2list(?POSITION_TABLE)).

server_name(1) -> map_worker_1;
server_name(2) -> map_worker_2;
server_name(3) -> map_worker_3.

cell_table(1) -> map_cells_1;
cell_table(2) -> map_cells_2;
cell_table(3) -> map_cells_3.

metrics_table(1) -> map_operation_metrics_1;
metrics_table(2) -> map_operation_metrics_2;
metrics_table(3) -> map_operation_metrics_3.
