-module(map_router).
-behaviour(gen_server).

-include("chat_protocol.hrl").

-export([start_link/0,
         map_ids/0,
         default_map_id/0,
         join/4,
         leave/2,
         relocate/2,
         nearby/1,
         location/1,
         operation_stats/0,
         random_position/0,
         valid_map/1,
         valid_position/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-define(POSITION_TABLE, map_role_positions).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

map_ids() ->
    ?MAP_IDS.

default_map_id() ->
    ?DEFAULT_MAP_ID.

join(RoleId, RolePid, MapId, Position) ->
    case valid_map(MapId) of
        true ->
            recover_join_result(
                MapId, Position, RolePid,
                map_worker:join(MapId, RoleId, RolePid, Position));
        false -> {error, invalid_map}
    end.

leave(RoleId, RolePid) ->
    case location(RolePid) of
        {ok, {MapId, _Position}} ->
            recover_leave_result(
                MapId, RolePid, map_worker:leave(MapId, RoleId, RolePid));
        {error, not_in_map} = Error -> Error
    end.

relocate(RolePid, NewPosition) ->
    case location(RolePid) of
        {ok, {MapId, _Position}} ->
            recover_relocate_result(
                MapId, NewPosition, RolePid,
                map_worker:relocate(MapId, RolePid, NewPosition));
        {error, not_in_map} = Error ->
            Error
    end.

nearby({MapId, Position}) ->
    case valid_map(MapId) andalso valid_position(Position) of
        true -> {ok, map_worker:nearby(MapId, Position)};
        false -> {error, not_in_map}
    end;
nearby(_Location) ->
    {error, not_in_map}.

location(RolePid) ->
    case ets:lookup(?POSITION_TABLE, RolePid) of
        [{RolePid, {pending, _MapId, _Position, _RoleId}}] ->
            {error, not_in_map};
        [{RolePid, Location}] -> {ok, Location};
        [] -> {error, not_in_map}
    end.

operation_stats() ->
    lists:foldl(
        fun merge_operation_stats/2,
        #{},
        [map_worker:operation_stats(MapId) || MapId <- map_ids()]).

random_position() ->
    {rand:uniform(?MAP_SIZE) - 1, rand:uniform(?MAP_SIZE) - 1}.

valid_map(MapId) ->
    lists:member(MapId, ?MAP_IDS).

valid_position({X, Y}) ->
    is_integer(X) andalso X >= 0 andalso X < ?MAP_SIZE andalso
    is_integer(Y) andalso Y >= 0 andalso Y < ?MAP_SIZE;
valid_position(_Position) ->
    false.

init([]) ->
    ?POSITION_TABLE = ets:new(?POSITION_TABLE, [
        named_table,
        set,
        public,
        {read_concurrency, true},
        {write_concurrency, auto}
    ]),
    lists:foreach(fun(MapId) ->
        CellTable = map_worker:cell_table(MapId),
        CellTable = ets:new(CellTable, [
            named_table,
            bag,
            public,
            {read_concurrency, true},
            {write_concurrency, auto}
        ]),
        MetricsTable = map_worker:metrics_table(MapId),
        MetricsTable = ets:new(MetricsTable, [named_table, public, set])
    end, map_ids()),
    {ok, #{}}.

handle_call(Request, _From, State) ->
    {reply, {error, {unsupported_call, Request}}, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

merge_operation_stats(MapStats, Acc) ->
    maps:fold(
        fun(Operation, #{count := Count,
                         total_us := TotalUs,
                         max_us := MaxUs}, OperationAcc) ->
            maps:update_with(
                Operation,
                fun(#{count := OldCount,
                      total_us := OldTotalUs,
                      max_us := OldMaxUs}) ->
                    #{count => OldCount + Count,
                      total_us => OldTotalUs + TotalUs,
                      max_us => erlang:max(OldMaxUs, MaxUs)}
                end,
                #{count => Count, total_us => TotalUs, max_us => MaxUs},
                OperationAcc)
        end,
        Acc,
        MapStats).

recover_join_result(MapId, Position, RolePid, {error, map_unavailable}) ->
    case location(RolePid) of
        {ok, {MapId, Position} = Location} -> {ok, Location};
        _ -> {error, map_unavailable}
    end;
recover_join_result(_MapId, _Position, _RolePid, Result) ->
    Result.

recover_leave_result(MapId, RolePid, {error, map_unavailable}) ->
    case location(RolePid) of
        {error, not_in_map} -> {ok, MapId};
        _ -> {error, {map_unavailable, MapId}}
    end;
recover_leave_result(_MapId, _RolePid, Result) ->
    Result.

recover_relocate_result(MapId, Position, RolePid, {error, map_unavailable}) ->
    case location(RolePid) of
        {ok, {MapId, Position}} -> {ok, Position};
        _ -> {error, map_unavailable}
    end;
recover_relocate_result(_MapId, _Position, _RolePid, Result) ->
    Result.
