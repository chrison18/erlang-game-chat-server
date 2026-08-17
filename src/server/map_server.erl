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
         leave/3,
         move/3,
         teleport/3,
         send_nearby/5,
         send_map/5,
         operation_stats/0,
         operation_stats/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-define(CALL_TIMEOUT_MS, 5000).

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

leave(MapPid, RoleId, RolePid) ->
    call(MapPid, {leave, RoleId, RolePid}).

move(MapPid, RolePid, NewPosition) ->
    call(MapPid, {move, RolePid, NewPosition}).

teleport(MapPid, RolePid, NewPosition) ->
    call(MapPid, {teleport, RolePid, NewPosition}).

send_nearby(MapPid, RoleId, RolePid, RoleName, Content) ->
    call(MapPid, {send_nearby, RoleId, RolePid, RoleName, Content}).

send_map(MapPid, RoleId, RolePid, RoleName, Content) ->
    call(MapPid, {send_map, RoleId, RolePid, RoleName, Content}).

operation_stats() ->
    lists:foldl(
        fun(MapId, Acc) ->
            maps:fold(
                fun(Operation,
                    #{count := Count, total_us := TotalUs, max_us := MaxUs},
                    OperationAcc) ->
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
                operation_stats(MapId))
        end,
        #{},
        map_ids()).

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
    {ok, #{map_id => MapId,
           members => #{},
           cells => #{},
           monitors => #{},
           operations => #{}}}.

handle_call({join, RoleId, RolePid, Position}, _From,
            #{map_id := MapId,
              members := Members,
              cells := Cells,
              monitors := Monitors} = State) ->
    StartedAt = erlang:monotonic_time(microsecond),
    ReplyState = case {valid_position(Position), maps:is_key(RoleId, Members)} of
        {false, _} ->
            {error, invalid_position, State};
        {true, true} ->
            {error, {already_in_map, MapId}, State};
        {true, false} ->
            MonitorRef = erlang:monitor(process, RolePid),
            Member = #{role_pid => RolePid,
                        position => Position,
                        monitor_ref => MonitorRef},
            NewMembers = Members#{RoleId => Member},
            NewCells = add_cell(Position, RolePid, Cells),
            NewMonitors = Monitors#{MonitorRef => RoleId},
            {{ok, {MapId, Position}},
             State#{members := NewMembers,
                   cells := NewCells,
                   monitors := NewMonitors}}
    end,
    operation_reply(MapId, join, StartedAt, ReplyState);
handle_call({leave, RoleId, RolePid}, _From,
            #{map_id := MapId,
              members := Members,
              cells := Cells,
              monitors := Monitors} = State) ->
    StartedAt = erlang:monotonic_time(microsecond),
    ReplyState = case maps:take(RoleId, Members) of
        {#{role_pid := RolePid,
           position := Position,
           monitor_ref := MonitorRef}, RemainingMembers} ->
            true = erlang:demonitor(MonitorRef, [flush]),
            NewState = State#{members := RemainingMembers,
                              cells := remove_cell(Position, RolePid, Cells),
                              monitors := maps:remove(MonitorRef, Monitors)},
            {{ok, MapId}, NewState};
        {Member, _RemainingMembers} ->
            {{error, not_in_map}, State#{members := Members#{RoleId => Member}}};
        error ->
            {{error, not_in_map}, State}
    end,
    operation_reply(MapId, leave, StartedAt, ReplyState);
handle_call({move, RolePid, NewPosition}, _From,
            #{map_id := MapId, members := Members, cells := Cells} = State) ->
    StartedAt = erlang:monotonic_time(microsecond),
    ReplyState = case valid_position(NewPosition) of
        false ->
            {{error, invalid_position}, State};
        true ->
            change_position(RolePid, NewPosition, Members, Cells, State)
    end,
    operation_reply(MapId, move, StartedAt, ReplyState);
handle_call({teleport, RolePid, NewPosition}, _From,
            #{map_id := MapId, members := Members, cells := Cells} = State) ->
    StartedAt = erlang:monotonic_time(microsecond),
    ReplyState = case valid_position(NewPosition) of
        false ->
            {{error, invalid_position}, State};
        true ->
            change_position(RolePid, NewPosition, Members, Cells, State)
    end,
    operation_reply(MapId, teleport, StartedAt, ReplyState);
handle_call({send_nearby, RoleId, RolePid, RoleName, Content}, _From,
            #{members := Members, cells := Cells} = State) ->
    case maps:find(RoleId, Members) of
        {ok, #{role_pid := RolePid, position := Position}} ->
            {X, Y} = Position,
            Packet = chat_server_protocol:encode_nearby_push(
                RoleId, RoleName, X, Y, Content),
            Targets = nearby_targets(Position, Cells),
            push(Targets, Packet),
            {reply, {ok, length(Targets)}, State};
        _ ->
            {reply, {error, not_in_map}, State}
    end;
handle_call({send_map, RoleId, RolePid, RoleName, Content}, _From,
            #{map_id := MapId, members := Members} = State) ->
    StartedAt = erlang:monotonic_time(microsecond),
    ReplyState = case maps:find(RoleId, Members) of
        {ok, #{role_pid := RolePid}} ->
            Packet = chat_server_protocol:encode_map_chat_push(
                MapId, RoleId, RoleName, Content),
            push([MemberPid
                  || #{role_pid := MemberPid} <- maps:values(Members)],
                 Packet),
            {{ok, MapId}, State};
        _ ->
            {{error, not_in_map}, State}
    end,
    operation_reply(MapId, send_map, StartedAt, ReplyState);
handle_call(operation_stats, _From, #{operations := Operations} = State) ->
    {reply, Operations, State};
handle_call(_Request, _From, State) ->
    {reply, {error, map_unavailable}, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({'DOWN', MonitorRef, process, _RolePid, _Reason},
            #{members := Members,
              cells := Cells,
              monitors := Monitors} = State) ->
    case maps:take(MonitorRef, Monitors) of
        {RoleId, RemainingMonitors} ->
            case maps:take(RoleId, Members) of
                {#{role_pid := RolePid, position := Position}, RemainingMembers} ->
                    {noreply, State#{members := RemainingMembers,
                                     cells := remove_cell(Position, RolePid, Cells),
                                     monitors := RemainingMonitors}};
                error ->
                    {noreply, State#{monitors := RemainingMonitors}}
            end;
        error ->
            {noreply, State}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

call(undefined, _Request) ->
    {error, map_unavailable};
call(MapPid, Request) when is_pid(MapPid) ->
    try gen_server:call(MapPid, Request, ?CALL_TIMEOUT_MS) of
        Reply -> Reply
    catch
        exit:_Reason -> {error, map_unavailable}
    end.

find_member(RolePid, Members) ->
    case lists:dropwhile(
             fun({_RoleId, #{role_pid := MemberPid}}) ->
                 MemberPid =/= RolePid
             end,
             maps:to_list(Members)) of
        [Member | _] -> Member;
        [] -> error
    end.

change_position(RolePid, NewPosition, Members, Cells, State) ->
    case find_member(RolePid, Members) of
        error ->
            {{error, not_in_map}, State};
        {_RoleId, #{position := NewPosition}} ->
            {{ok, NewPosition}, State};
        {RoleId, #{position := OldPosition} = Member} ->
            NewMember = Member#{position := NewPosition},
            NewState = State#{members := Members#{RoleId => NewMember},
                              cells := add_cell(
                                  NewPosition, RolePid,
                                  remove_cell(OldPosition, RolePid, Cells))},
            {{ok, NewPosition}, NewState}
    end.

nearby_targets({X, Y}, Cells) ->
    Coordinates = [{NearbyX, NearbyY}
                   || NearbyX <- lists:seq(erlang:max(0, X - 1),
                                           erlang:min(?MAP_SIZE - 1, X + 1)),
                      NearbyY <- lists:seq(erlang:max(0, Y - 1),
                                           erlang:min(?MAP_SIZE - 1, Y + 1))],
    lists:usort(lists:append([maps:get(Coordinate, Cells, [])
                              || Coordinate <- Coordinates])).

add_cell(Position, RolePid, Cells) ->
    maps:update_with(Position,
                     fun(RolePids) -> [RolePid | RolePids] end,
                     [RolePid],
                     Cells).

remove_cell(Position, RolePid, Cells) ->
    case maps:find(Position, Cells) of
        {ok, RolePids} ->
            case lists:delete(RolePid, RolePids) of
                [] -> maps:remove(Position, Cells);
                Remaining -> Cells#{Position => Remaining}
            end;
        error ->
            Cells
    end.

push(Targets, Packet) ->
    lists:foreach(
        fun(RolePid) -> gen_server:cast(RolePid, {push_batch, Packet}) end,
        Targets),
    ok.

operation_reply(MapId, Operation, StartedAt, {Reply, State}) ->
    ElapsedUs = erlang:monotonic_time(microsecond) - StartedAt,
    Operations = maps:get(operations, State),
    Previous = maps:get(Operation, Operations,
                        #{count => 0, total_us => 0, max_us => 0}),
    Updated = Previous#{count := maps:get(count, Previous) + 1,
                        total_us := maps:get(total_us, Previous) + ElapsedUs,
                        max_us := erlang:max(maps:get(max_us, Previous),
                                             ElapsedUs)},
    {reply, Reply,
     State#{map_id := MapId,
            operations := Operations#{Operation => Updated}}}.
