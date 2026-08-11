-module(map_server).
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
         valid_map/1,
         valid_position/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-define(CELL_TABLE, map_cells).
-define(POSITION_TABLE, map_role_positions).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

map_ids() ->
    ?MAP_IDS.

default_map_id() ->
    ?DEFAULT_MAP_ID.

join(RoleId, RolePid, MapId, Position) ->
    gen_server:call(?MODULE, {join, RoleId, RolePid, MapId, Position}).

leave(RoleId, RolePid) ->
    gen_server:call(?MODULE, {leave, RoleId, RolePid}).

relocate(RolePid, NewPosition) ->
    gen_server:call(?MODULE, {relocate, RolePid, NewPosition}).

nearby(Location) ->
    gen_server:call(?MODULE, {nearby, Location}).

location(RolePid) ->
    case ets:lookup(?POSITION_TABLE, RolePid) of
        [{RolePid, Location}] -> {ok, Location};
        [] -> {error, not_in_map}
    end.

valid_map(MapId) ->
    lists:member(MapId, ?MAP_IDS).

valid_position({X, Y}) ->
    is_integer(X) andalso X >= 0 andalso X < ?MAP_SIZE andalso
    is_integer(Y) andalso Y >= 0 andalso Y < ?MAP_SIZE;
valid_position(_Position) ->
    false.

init([]) ->
    ?CELL_TABLE = ets:new(?CELL_TABLE, [
        named_table,
        bag,
        protected,
        {read_concurrency, true}
    ]),
    ?POSITION_TABLE = ets:new(?POSITION_TABLE, [
        named_table,
        set,
        protected,
        {read_concurrency, true}
    ]),
    {ok, #{monitors => #{}}}.

handle_call({join, RoleId, RolePid, MapId, Position}, _From,
            #{monitors := Monitors} = State) ->
    case {valid_map(MapId), valid_position(Position),
          ets:lookup(?POSITION_TABLE, RolePid)} of
        {false, _, _} ->
            {reply, {error, invalid_map}, State};
        {true, false, _} ->
            {reply, {error, invalid_position}, State};
        {true, true, [{RolePid, {CurrentMapId, _CurrentPosition}}]} ->
            {reply, {error, {already_in_map, CurrentMapId}}, State};
        {true, true, []} ->
            case channel_server:join_map(MapId, RoleId, RolePid) of
                {ok, {map, MapId}} ->
                    Location = {MapId, Position},
                    true = ets:insert(?POSITION_TABLE, {RolePid, Location}),
                    true = ets:insert(
                        ?CELL_TABLE, {cell_key(Location), RolePid}),
                    {NewMonitors, _MonitorRef} =
                        ensure_monitor(RolePid, Monitors),
                    {reply, {ok, Location},
                     State#{monitors := NewMonitors}};
                {error, channel_unavailable} ->
                    {reply, {error, map_unavailable}, State}
            end
    end;
handle_call({leave, RoleId, RolePid}, _From, State) ->
    case ets:lookup(?POSITION_TABLE, RolePid) of
        [] ->
            {reply, {error, not_in_map}, State};
        [{RolePid, {MapId, _Position} = Location}] ->
            case channel_server:leave_map(MapId, RoleId) of
                {ok, {map, MapId}} ->
                    ok = remove_location(RolePid, Location),
                    {reply, {ok, MapId}, State};
                {error, not_joined} ->
                    ok = remove_location(RolePid, Location),
                    {reply, {ok, MapId}, State};
                {error, channel_unavailable} ->
                    {reply, {error, {map_unavailable, MapId}}, State}
            end
    end;
handle_call({relocate, RolePid, NewPosition}, _From, State) ->
    case {valid_position(NewPosition),
          ets:lookup(?POSITION_TABLE, RolePid)} of
        {false, _} ->
            {reply, {error, invalid_position}, State};
        {true, []} ->
            {reply, {error, not_in_map}, State};
        {true, [{RolePid, {_MapId, NewPosition}}]} ->
            {reply, {ok, NewPosition}, State};
        {true, [{RolePid, {MapId, _OldPosition} = OldLocation}]} ->
            NewLocation = {MapId, NewPosition},
            true = ets:delete_object(
                ?CELL_TABLE, {cell_key(OldLocation), RolePid}),
            true = ets:insert(?POSITION_TABLE, {RolePid, NewLocation}),
            true = ets:insert(
                ?CELL_TABLE, {cell_key(NewLocation), RolePid}),
            {reply, {ok, NewPosition}, State}
    end;
handle_call({nearby, {MapId, Position} = Location}, _From, State) ->
    Reply = case valid_map(MapId) andalso valid_position(Position) of
        true -> {ok, nearby_roles(Location)};
        false -> {error, not_in_map}
    end,
    {reply, Reply, State};
handle_call({nearby, _Location}, _From, State) ->
    {reply, {error, not_in_map}, State};
handle_call(Request, _From, State) ->
    {reply, {error, {unsupported_call, Request}}, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({'DOWN', MonitorRef, process, RolePid, _Reason},
            #{monitors := Monitors} = State) ->
    case maps:take(RolePid, Monitors) of
        {MonitorRef, RemainingMonitors} ->
            remove_role(RolePid),
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

nearby_roles({MapId, {X, Y}}) ->
    Coordinates = [{MapId, NearbyX, NearbyY}
                   || NearbyX <- lists:seq(erlang:max(0, X - 1),
                                          erlang:min(?MAP_SIZE - 1, X + 1)),
                      NearbyY <- lists:seq(erlang:max(0, Y - 1),
                                          erlang:min(?MAP_SIZE - 1, Y + 1))],
    lists:usort([RolePid
                 || Coordinate <- Coordinates,
                    {_StoredCoordinate, RolePid} <- ets:lookup(
                        ?CELL_TABLE, Coordinate)]).

cell_key({MapId, {X, Y}}) ->
    {MapId, X, Y}.

remove_role(RolePid) ->
    case ets:lookup(?POSITION_TABLE, RolePid) of
        [{RolePid, Location}] -> remove_location(RolePid, Location);
        [] -> ok
    end.

remove_location(RolePid, Location) ->
    true = ets:delete(?POSITION_TABLE, RolePid),
    true = ets:delete_object(?CELL_TABLE, {cell_key(Location), RolePid}),
    ok.
