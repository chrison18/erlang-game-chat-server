-module(map_server).
-behaviour(gen_server).

-export([start_link/0,
         enter/2,
         relocate/3,
         nearby/1,
         valid_position/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-define(CELL_TABLE, map_cells).
-define(POSITION_TABLE, map_role_positions).
-define(MAP_SIZE, 100).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

enter(RolePid, Position) ->
    gen_server:call(?MODULE, {enter, RolePid, Position}).

relocate(RolePid, OldPosition, NewPosition) ->
    case {valid_position(NewPosition), ets:lookup(?POSITION_TABLE, RolePid)} of
        {false, _} ->
            {error, invalid_position};
        {true, [{RolePid, OldPosition}]} when OldPosition =:= NewPosition ->
            ok;
        {true, [{RolePid, OldPosition}]} ->
            true = ets:delete_object(?CELL_TABLE, {OldPosition, RolePid}),
            true = ets:insert(?POSITION_TABLE, {RolePid, NewPosition}),
            true = ets:insert(?CELL_TABLE, {NewPosition, RolePid}),
            ok;
        {true, []} ->
            {error, not_entered};
        {true, [_]} ->
            {error, stale_position}
    end.

nearby({X, Y} = Position) ->
    case valid_position(Position) of
        true ->
            Coordinates = [{NearbyX, NearbyY}
                           || NearbyX <- lists:seq(erlang:max(0, X - 1),
                                                  erlang:min(?MAP_SIZE - 1, X + 1)),
                              NearbyY <- lists:seq(erlang:max(0, Y - 1),
                                                  erlang:min(?MAP_SIZE - 1, Y + 1))],
            lists:usort([RolePid
                         || Coordinate <- Coordinates,
                            {_StoredCoordinate, RolePid} <- ets:lookup(
                                ?CELL_TABLE, Coordinate)]);
        false ->
            []
    end.

valid_position({X, Y}) ->
    is_integer(X) andalso X >= 0 andalso X < ?MAP_SIZE andalso
    is_integer(Y) andalso Y >= 0 andalso Y < ?MAP_SIZE;
valid_position(_Position) ->
    false.

init([]) ->
    ?CELL_TABLE = ets:new(?CELL_TABLE, [
        named_table,
        bag,
        public,
        {read_concurrency, true},
        {write_concurrency, auto}
    ]),
    ?POSITION_TABLE = ets:new(?POSITION_TABLE, [
        named_table,
        set,
        public,
        {read_concurrency, true},
        {write_concurrency, auto}
    ]),
    {ok, #{monitors => #{}}}.

handle_call({enter, RolePid, Position}, _From,
            #{monitors := Monitors} = State) ->
    case {valid_position(Position), ets:lookup(?POSITION_TABLE, RolePid)} of
        {false, _} ->
            {reply, {error, invalid_position}, State};
        {true, [_]} ->
            {reply, {error, already_entered}, State};
        {true, []} ->
            MonitorRef = erlang:monitor(process, RolePid),
            true = ets:insert(?POSITION_TABLE, {RolePid, Position}),
            true = ets:insert(?CELL_TABLE, {Position, RolePid}),
            {reply, ok, State#{monitors := Monitors#{MonitorRef => RolePid}}}
    end;
handle_call(Request, _From, State) ->
    {reply, {error, {unsupported_call, Request}}, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({'DOWN', MonitorRef, process, RolePid, _Reason},
            #{monitors := Monitors} = State) ->
    case maps:take(MonitorRef, Monitors) of
        {RolePid, RemainingMonitors} ->
            remove_role(RolePid),
            {noreply, State#{monitors := RemainingMonitors}};
        error ->
            {noreply, State}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

remove_role(RolePid) ->
    case ets:take(?POSITION_TABLE, RolePid) of
        [{RolePid, Position}] ->
            true = ets:delete_object(?CELL_TABLE, {Position, RolePid}),
            ok;
        [] ->
            ok
    end.
