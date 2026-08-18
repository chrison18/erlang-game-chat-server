-module(chat_client).
-behaviour(gen_server).

%% 单个压测/手工客户端：发送命令、解码响应并保存服务端已经确认的本地缓存。

-include("chat_protocol.hrl").

-export([start_link/5]).
-export([init/1, handle_call/3, handle_cast/2, handle_continue/2,
         handle_info/2, terminate/2]).

-define(AUTO_SEND_INTERVAL_MS, 1000).
-define(OBSERVER_REPORT_INTERVAL_MS, 1000).
-define(MOVEMENT_SCHEDULE,
        [{30, move},
         {40, move},
         {45, move},
         {200, move},
         {210, move},
         {220, move},
         {230, move},
         {900, move},
         {950, move},
         {980, move},
         {1000, map_chat}]).

start_link(Host, Port, RoleName, Password, Mode) ->
    gen_server:start_link(
        ?MODULE, [Host, Port, RoleName, Password, Mode], []).

init([Host, Port, RoleName, Password, Mode]) ->
    Options = [binary, {packet, 4}, {active, true}],
    case gen_tcp:connect(Host, Port, Options) of
        {ok, Socket} ->
            State = mode_state(Mode, #{socket => Socket,
                                       role_name => RoleName,
                                       map_id => undefined,
                                       position => undefined,
                                       feedback => false,
                                       action_seq => 1,
                                       observer_received => 0,
                                       observer_invalid => 0}),
            {ok, State,
             {continue, {login, RoleName, Password}}};
        {error, Reason} ->
            {stop, {connect_failed, Reason}}
    end.

handle_call(position, _From, #{position := undefined} = State) ->
    {reply, {error, not_in_map}, State};
handle_call(position, _From, #{position := Position} = State) ->
    {reply, {ok, Position}, State};
handle_call(location, _From,
            #{map_id := undefined} = State) ->
    {reply, {error, not_in_map}, State};
handle_call(location, _From,
            #{map_id := MapId, position := Position} = State) ->
    {reply, {ok, {MapId, Position}}, State};
handle_call({set_feedback, Enabled}, _From, State)
  when is_boolean(Enabled) ->
    {reply, ok, State#{feedback := Enabled}};
handle_call(Request, _From, State) ->
    {stop, {unsupported_call, Request}, State}.

handle_cast(list_channels, State) ->
    %% 外部 API 使用 cast，只表示命令进入客户端进程，不代表服务端业务成功。
    {noreply, do_list_channels(State)};
handle_cast({join_channel, ChannelId}, State) ->
    {noreply, do_join_channel(ChannelId, State)};
handle_cast({leave_channel, ChannelId}, State) ->
    {noreply, do_leave_channel(ChannelId, State)};
handle_cast({send_channel, ChannelId, Content}, State) ->
    {noreply, do_send_channel(ChannelId, Content, State)};
handle_cast({send_private, TargetRoleName, Content}, State) ->
    {noreply, do_send_private(TargetRoleName, Content, State)};
handle_cast({move, Direction}, State) ->
    {noreply, do_move(Direction, State)};
handle_cast({teleport, X, Y}, State) ->
    {noreply, do_teleport(X, Y, State)};
handle_cast({send_nearby, Content}, State) ->
    {noreply, do_send_nearby(Content, State)};
handle_cast({join_map, MapId}, State) ->
    {noreply, do_join_map(MapId, State)};
handle_cast(leave_map, State) ->
    {noreply, do_leave_map(State)};
handle_cast({send_map, Content}, State) ->
    {noreply, do_send_map(Content, State)};
handle_cast(_Request, State) ->
    {noreply, State}.

handle_continue({login, RoleName, Password}, State) ->
    {noreply, do_login(RoleName, Password, State)}.

handle_info(auto_action,
            #{mode := normal,
              role_name := RoleName,
              action_seq := Sequence} = State) ->
    Content = auto_message(RoleName, Sequence),
    ActionState = do_auto_action(Content, State),
    schedule_next_action(),
    {noreply, ActionState#{action_seq := Sequence + 1}};
handle_info({movement_action, move},
            #{mode := map_load, action_seq := Sequence} = State) ->
    ActionState = do_move(random_direction(), State),
    {noreply, ActionState#{action_seq := Sequence + 1}};
handle_info({movement_action, map_chat},
            #{mode := map_load,
              role_name := RoleName,
              action_seq := Sequence} = State) ->
    Content = map_load_message(RoleName, Sequence),
    ActionState = do_send_map(Content, State),
    schedule_movement_cycle(),
    {noreply, ActionState#{action_seq := Sequence + 1}};
handle_info(observer_report,
            #{mode := observer,
              role_name := RoleName,
              observer_received := Received,
              observer_invalid := Invalid} = State) ->
    io:format("observer=~ts received=~p invalid=~p~n",
              [RoleName, Received, Invalid]),
    schedule_observer_report(),
    {noreply, State#{observer_received := 0,
                     observer_invalid := 0}};
handle_info({tcp, Socket, Packet}, #{socket := Socket} = State) ->
    {noreply, handle_server_packet(Packet, State)};
handle_info({tcp_closed, Socket}, #{socket := Socket} = State) ->
    {stop, normal, State};
handle_info({tcp_error, Socket, Reason}, #{socket := Socket} = State) ->
    {stop, {tcp_error, Reason}, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #{socket := Socket}) ->
    gen_tcp:close(Socket).

do_login(RoleName, Password, State) ->
    case normalize_texts([RoleName, Password]) of
        {ok, [RoleNameBinary, PasswordBinary]} ->
            case chat_client_protocol:encode_login(
                     RoleNameBinary, PasswordBinary) of
                Packet when is_binary(Packet) ->
                    send_packet(login, Packet,
                                State#{role_name := RoleNameBinary});
                {error, Reason} ->
                    report_result(login, {error, Reason}, State)
            end;
        {error, Reason} ->
            report_result(login, {error, Reason}, State)
    end.

do_list_channels(State) ->
    Packet = chat_client_protocol:encode_channel_list(),
    send_packet(list_channels, Packet, State).

do_join_channel(ChannelId, State) when is_integer(ChannelId),
                                       ChannelId >= 0,
                                       ChannelId =< 16#FFFFFFFF ->
    Packet = chat_client_protocol:encode_channel_join(ChannelId),
    send_packet({join_channel, ChannelId}, Packet, State);
do_join_channel(_ChannelId, State) ->
    report_result(join_channel, {error, invalid_channel_id}, State).

do_leave_channel(ChannelId, State) when is_integer(ChannelId),
                                        ChannelId >= 0,
                                        ChannelId =< 16#FFFFFFFF ->
    Packet = chat_client_protocol:encode_channel_leave(ChannelId),
    send_packet({leave_channel, ChannelId}, Packet, State);
do_leave_channel(_ChannelId, State) ->
    report_result(leave_channel, {error, invalid_channel_id}, State).

do_send_channel(ChannelId, Content, State)
  when is_integer(ChannelId), ChannelId >= 0, ChannelId =< 16#FFFFFFFF ->
    case normalize_text(Content) of
        {ok, ContentBinary} ->
            Packet = chat_client_protocol:encode_channel_send(
                ChannelId, ContentBinary),
            send_packet({send_channel, ChannelId}, Packet, State);
        {error, Reason} ->
            report_result(send_channel, {error, Reason}, State)
    end;
do_send_channel(_ChannelId, _Content, State) ->
    report_result(send_channel, {error, invalid_channel_id}, State).

do_send_private(TargetRoleName, Content, State) ->
    case normalize_texts([TargetRoleName, Content]) of
        {ok, [TargetRoleNameBinary, ContentBinary]} ->
            case chat_client_protocol:encode_private_send(
                     TargetRoleNameBinary, ContentBinary) of
                Packet when is_binary(Packet) ->
                    send_packet({send_private, TargetRoleNameBinary},
                                Packet, State);
                {error, Reason} ->
                    report_result(send_private, {error, Reason}, State)
            end;
        {error, Reason} ->
            report_result(send_private, {error, Reason}, State)
    end.

do_move(Direction, State)
  when Direction =:= up; Direction =:= down;
       Direction =:= left; Direction =:= right ->
    Packet = chat_client_protocol:encode_move(Direction),
    send_packet({move, Direction}, Packet, State);
do_move(_Direction, State) ->
    report_result(move, {error, invalid_direction}, State).

do_teleport(X, Y, State)
  when is_integer(X), X >= 0, X =< 255,
       is_integer(Y), Y >= 0, Y =< 255 ->
    Packet = chat_client_protocol:encode_teleport(X, Y),
    send_packet({teleport, {X, Y}}, Packet, State);
do_teleport(_X, _Y, State) ->
    report_result(teleport, {error, invalid_position}, State).

do_send_nearby(Content, State) ->
    case normalize_text(Content) of
        {ok, ContentBinary} ->
            Packet = chat_client_protocol:encode_nearby_send(ContentBinary),
            send_packet(send_nearby, Packet, State);
        {error, Reason} ->
            report_result(send_nearby, {error, Reason}, State)
    end.

do_join_map(MapId, State)
  when is_integer(MapId), MapId >= 0, MapId =< 16#FFFF ->
    Packet = chat_client_protocol:encode_map_join(MapId),
    send_packet({join_map, MapId}, Packet, State);
do_join_map(_MapId, State) ->
    report_result(join_map, {error, invalid_map_id}, State).

do_leave_map(State) ->
    send_packet(leave_map, chat_client_protocol:encode_map_leave(), State).

do_send_map(Content, State) ->
    case normalize_text(Content) of
        {ok, ContentBinary} ->
            Packet = chat_client_protocol:encode_map_chat_send(ContentBinary),
            send_packet(send_map, Packet, State);
        {error, Reason} ->
            report_result(send_map, {error, Reason}, State)
    end.

send_packet(Action, Packet, #{socket := Socket} = State) ->
    case gen_tcp:send(Socket, Packet) of
        ok ->
            State;
        {error, Reason} ->
            report_result(Action, {error, {send_failed, Reason}}, State)
    end.

normalize_texts(Texts) ->
    normalize_texts(Texts, []).

normalize_texts([], Acc) ->
    {ok, lists:reverse(Acc)};
normalize_texts([Text | Rest], Acc) ->
    case normalize_text(Text) of
        {ok, Binary} -> normalize_texts(Rest, [Binary | Acc]);
        {error, Reason} -> {error, Reason}
    end.

normalize_text(Text) ->
    try unicode:characters_to_binary(Text) of
        Binary when is_binary(Binary) ->
            {ok, Binary};
        _Error ->
            {error, invalid_text}
    catch
        error:badarg ->
            {error, invalid_text}
    end.

handle_server_packet(Packet, State) ->
    %% 公共频道批包先在协议层完整校验，再逐条交给单包处理函数。
    case chat_client_protocol:decode_packet(Packet) of
        {ok, {channel_push_batch, Messages}} ->
            handle_channel_push_batch(Messages, State);
        {ok, {channel_push, Message}} ->
            handle_channel_push(Message, State);
        {ok, {private_push, Message}} ->
            handle_private_push(Message, State);
        {ok, {nearby_push, Message}} ->
            handle_nearby_push(Message, State);
        {ok, {map_chat_push, Message}} ->
            handle_map_chat_push(Message, State);
        {ok, Response} ->
            handle_response(Response, State);
        {error, Reason} ->
            handle_invalid_packet(Reason, State)
    end.

handle_response({login_result,
                 {ok, _RoleId, Position, ChannelIds} = Result}, State) ->
    %% 地图、坐标和频道缓存只取服务端成功响应，不在发送请求时提前修改。
    LoggedInState = remember_channels(
        ChannelIds, State#{map_id := ?DEFAULT_MAP_ID,
                           position := Position}),
    report_result(login, Result, start_mode_after_login(LoggedInState));
handle_response({login_result, Result}, State) ->
    report_result(login, Result, State);
handle_response({channel_list_result, {ok, Channels} = Result},
                #{channel_ids := _ChannelIds} = State) ->
    JoinedIds = [maps:get(channel_id, Channel)
                 || Channel <- Channels, maps:get(joined, Channel)],
    report_result(list_channels, Result,
                  State#{channel_ids := JoinedIds});
handle_response({channel_list_result, Result}, State) ->
    report_result(list_channels, Result, State);
handle_response({channel_join_result, {ok, ChannelId} = Result},
                #{channel_ids := ChannelIds} = State) ->
    report_result(join_channel, Result,
                  State#{channel_ids := [ChannelId | ChannelIds]});
handle_response({channel_join_result, Result}, State) ->
    report_result(join_channel, Result, State);
handle_response({channel_leave_result, {ok, ChannelId} = Result},
                #{channel_ids := ChannelIds} = State) ->
    report_result(leave_channel, Result,
                  State#{channel_ids := lists:delete(ChannelId, ChannelIds)});
handle_response({channel_leave_result, Result}, State) ->
    report_result(leave_channel, Result, State);
handle_response({channel_send_result, Result}, State) ->
    report_result(send_channel, Result, State);
handle_response({private_send_result, Result}, State) ->
    report_result(send_private, Result, State);
handle_response({move_result, {ok, Position} = Result}, State) ->
    report_result(move, Result, State#{position := Position});
handle_response({move_result, Result}, State) ->
    report_result(move, Result, State);
handle_response({teleport_result, {ok, Position} = Result}, State) ->
    report_result(teleport, Result, State#{position := Position});
handle_response({teleport_result, Result}, State) ->
    report_result(teleport, Result, State);
handle_response({nearby_send_result, Result}, State) ->
    report_result(send_nearby, Result, State);
handle_response({map_join_result, {ok, MapId, Position} = Result}, State) ->
    report_result(join_map, Result,
                  State#{map_id := MapId, position := Position});
handle_response({map_join_result, Result}, State) ->
    report_result(join_map, Result, State);
handle_response({map_leave_result, {ok, _MapId} = Result}, State) ->
    report_result(leave_map, Result,
                  State#{map_id := undefined, position := undefined});
handle_response({map_leave_result, Result}, State) ->
    report_result(leave_map, Result, State);
handle_response({map_chat_send_result, Result}, State) ->
    report_result(send_map, Result, State);
handle_response({server_error, RequestProtoId, Reason}, State) ->
    report_result({server_error, RequestProtoId}, {error, Reason}, State).

report_result(Action, Result, #{mode := observer} = State) ->
    print_result(Action, Result),
    State;
report_result(Action, Result, #{feedback := true} = State) ->
    print_result(Action, Result),
    State;
report_result(_Action, _Result, State) ->
    State.

handle_channel_push(Message,
                    #{mode := observer,
                      observer_received := Received} = State) ->
    print_channel_push(Message),
    State#{observer_received := Received + 1};
handle_channel_push(Message, #{feedback := true} = State) ->
    print_channel_push(Message),
    State;
handle_channel_push(_Message, State) ->
    State.

handle_private_push(Message, #{feedback := true} = State) ->
    print_private_push(Message),
    State;
handle_private_push(_Message, State) ->
    State.

handle_nearby_push(Message,
                   #{mode := observer,
                     observer_received := Received} = State) ->
    print_nearby_push(Message),
    State#{observer_received := Received + 1};
handle_nearby_push(Message, #{feedback := true} = State) ->
    print_nearby_push(Message),
    State;
handle_nearby_push(_Message, State) ->
    State.

handle_map_chat_push(Message,
                     #{mode := observer,
                       observer_received := Received} = State) ->
    print_map_chat_push(Message),
    State#{observer_received := Received + 1};
handle_map_chat_push(Message, #{feedback := true} = State) ->
    print_map_chat_push(Message),
    State;
handle_map_chat_push(_Message, State) ->
    State.

handle_channel_push_batch(Messages, State) ->
    lists:foldl(fun handle_channel_push/2, State, Messages).

handle_invalid_packet(Reason,
                      #{mode := observer,
                        observer_invalid := Invalid} = State) ->
    print_result(protocol, {error, Reason}),
    State#{observer_invalid := Invalid + 1};
handle_invalid_packet(Reason, #{feedback := true} = State) ->
    print_result(protocol, {error, Reason}),
    State;
handle_invalid_packet(_Reason, State) ->
    State.

start_mode_after_login(#{mode := observer} = State) ->
    schedule_observer_report(),
    State;
start_mode_after_login(#{mode := normal} = State) ->
    schedule_next_action(),
    State;
start_mode_after_login(#{mode := map_load} = State) ->
    schedule_movement_cycle(),
    State;
start_mode_after_login(State) ->
    State.

schedule_next_action() ->
    _ = erlang:send_after(
        ?AUTO_SEND_INTERVAL_MS, self(), auto_action),
    ok.

schedule_movement_cycle() ->
    lists:foreach(
        fun({Delay, Action}) ->
            _ = erlang:send_after(Delay, self(), {movement_action, Action})
        end,
        ?MOVEMENT_SCHEDULE),
    ok.

schedule_observer_report() ->
    _ = erlang:send_after(
        ?OBSERVER_REPORT_INTERVAL_MS, self(), observer_report),
    ok.

auto_message(RoleName, Sequence) ->
    <<RoleName/binary, " auto message ",
      (integer_to_binary(Sequence))/binary>>.

map_load_message(RoleName, Sequence) ->
    <<RoleName/binary, " map message ",
      (integer_to_binary(Sequence))/binary>>.

mode_state(observer, State) ->
    State#{mode => observer};
mode_state({normal, ClientId, StartId, EndId}, State) ->
    State#{mode => normal,
           client_id => ClientId,
           client_range => {StartId, EndId},
           channel_ids => []};
mode_state(map_load, State) ->
    State#{mode => map_load,
           channel_ids => []};
mode_state(manual, State) ->
    State#{mode => manual,
           channel_ids => []};
mode_state(normal, State) ->
    State#{mode => normal,
           client_id => undefined,
           client_range => undefined,
           channel_ids => []}.

remember_channels(ChannelIds, #{channel_ids := _OldChannelIds} = State) ->
    State#{channel_ids := ChannelIds};
remember_channels(_ChannelIds, State) ->
    State.

do_auto_action(Content, State) ->
    %% normal 模式每秒等概率执行七类业务动作。
    case rand:uniform(7) of
        1 -> do_send_channel(1, Content, State);
        2 -> auto_private(Content, State);
        3 -> do_move(random_direction(), State);
        4 -> do_teleport(random_coordinate(), random_coordinate(), State);
        5 -> do_send_nearby(Content, State);
        6 -> do_send_map(Content, State);
        7 -> auto_switch_map(State)
    end.

auto_private(Content,
             #{client_id := ClientId,
               client_range := {StartId, EndId}} = State) ->
    case random_target(ClientId, StartId, EndId) of
        none -> do_send_channel(1, Content, State);
        TargetId -> do_send_private(client_role_name(TargetId), Content, State)
    end;
auto_private(Content, State) ->
    do_send_channel(1, Content, State).

auto_switch_map(#{map_id := undefined} = State) ->
    do_join_map(random_member(?MAP_IDS), State);
auto_switch_map(#{map_id := MapId} = State) ->
    %% 切图沿用公开协议语义：先发 leave，再发目标地图 join。
    TargetMapId = random_member(lists:delete(MapId, ?MAP_IDS)),
    do_join_map(TargetMapId, do_leave_map(State)).

random_direction() ->
    element(rand:uniform(4), {up, down, left, right}).

random_coordinate() ->
    rand:uniform(?MAP_SIZE) - 1.

random_target(_ClientId, Id, Id) ->
    none;
random_target(ClientId, StartId, EndId) ->
    Candidate = StartId + rand:uniform(EndId - StartId) - 1,
    case Candidate >= ClientId of
        true -> Candidate + 1;
        false -> Candidate
    end.

random_member([]) ->
    none;
random_member(Items) ->
    lists:nth(rand:uniform(length(Items)), Items).

client_role_name(ClientId) ->
    <<"client_", (integer_to_binary(ClientId))/binary>>.

print_result(Action, Result) ->
    io:format("[~p] ~p~n", [Action, Result]).

print_channel_push(#{channel_id := ChannelId,
                     sender_role_id := SenderRoleId,
                     sender_role_name := SenderRoleName,
                     content := Content}) ->
    io:format("[channel ~p] ~ts(~p): ~ts~n",
              [ChannelId, SenderRoleName, SenderRoleId, Content]).

print_private_push(#{sender_role_id := SenderRoleId,
                     sender_role_name := SenderRoleName,
                     content := Content}) ->
    io:format("[private] ~ts(~p): ~ts~n",
              [SenderRoleName, SenderRoleId, Content]).

print_nearby_push(#{sender_role_id := SenderRoleId,
                    sender_role_name := SenderRoleName,
                    position := {X, Y},
                    content := Content}) ->
    io:format("[nearby ~p,~p] ~ts(~p): ~ts~n",
              [X, Y, SenderRoleName, SenderRoleId, Content]).

print_map_chat_push(#{map_id := MapId,
                      sender_role_id := SenderRoleId,
                      sender_role_name := SenderRoleName,
                      content := Content}) ->
    io:format("[map ~p] ~ts(~p): ~ts~n",
              [MapId, SenderRoleName, SenderRoleId, Content]).
