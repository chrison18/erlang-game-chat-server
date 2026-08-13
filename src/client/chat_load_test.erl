-module(chat_load_test).

%% 批量客户端控制入口。异步命令返回 ok 仅表示已投递给 chat_client。

-export([start/2,
         start_map/2,
         start_observer/0,
         send_channel/3,
         send_private/3,
         move/2,
         teleport/3,
         send_nearby/2,
         join_map/2,
         leave_map/1,
         send_map/2,
         set_feedback/2,
         position/1,
         location/1]).

-define(DEFAULT_HOST, "127.0.0.1").
-define(DEFAULT_PORT, 5555).
-define(DEFAULT_PASSWORD, <<"123456">>).
-define(OBSERVER_ROLE_NAME, <<"observer_001">>).

start(StartId, EndId) ->
    start(StartId, EndId, normal).

start_map(StartId, EndId) ->
    start(StartId, EndId, map_load).

start(StartId, EndId, LoadMode)
  when is_integer(StartId), StartId > 0,
       is_integer(EndId), EndId >= StartId ->
    case existing_client(StartId, EndId) of
        none ->
            %% 整段 ClientId 必须空闲，避免一批压测混入已有客户端。
            Host = application:get_env(chat, client_host, ?DEFAULT_HOST),
            Port = application:get_env(chat, port, ?DEFAULT_PORT),
            start_clients(StartId, EndId, Host, Port,
                          {LoadMode, StartId, EndId}, 0);
        ClientId ->
            {error, {client_already_started, ClientId}}
    end;
start(_StartId, _EndId, _LoadMode) ->
    {error, invalid_client_range}.

start_observer() ->
    Host = application:get_env(chat, client_host, ?DEFAULT_HOST),
    Port = application:get_env(chat, port, ?DEFAULT_PORT),
    case chat_client_sup:start_client(
             observer,
             Host,
             Port,
             ?OBSERVER_ROLE_NAME,
             ?DEFAULT_PASSWORD,
             observer) of
        {ok, _ObserverPid} ->
            ok;
        {error, {already_started, _ObserverPid}} ->
            {error, observer_already_started};
        {error, already_present} ->
            {error, observer_already_started};
        {error, Reason} ->
            {error, {observer_start_failed, Reason}}
    end.

send_channel(ClientId, ChannelId, Content)
  when is_integer(ClientId), ClientId > 0 ->
    send_client_command(ClientId, {send_channel, ChannelId, Content});
send_channel(_ClientId, _ChannelId, _Content) ->
    {error, invalid_client_id}.

send_private(SenderId, TargetId, Content)
  when is_integer(SenderId), SenderId > 0,
       is_integer(TargetId), TargetId > 0 ->
    send_client_command(
        SenderId, {send_private, role_name(TargetId), Content});
send_private(_SenderId, _TargetId, _Content) ->
    {error, invalid_client_id}.

move(ClientId, Direction)
  when is_integer(ClientId), ClientId > 0,
       Direction =:= up orelse Direction =:= down orelse
       Direction =:= left orelse Direction =:= right ->
    send_client_command(ClientId, {move, Direction});
move(ClientId, _Direction) when is_integer(ClientId), ClientId > 0 ->
    {error, invalid_direction};
move(_ClientId, _Direction) ->
    {error, invalid_client_id}.

teleport(ClientId, X, Y)
  when is_integer(ClientId), ClientId > 0,
       is_integer(X), X >= 0, X =< 255,
       is_integer(Y), Y >= 0, Y =< 255 ->
    send_client_command(ClientId, {teleport, X, Y});
teleport(ClientId, _X, _Y) when is_integer(ClientId), ClientId > 0 ->
    {error, invalid_position};
teleport(_ClientId, _X, _Y) ->
    {error, invalid_client_id}.

send_nearby(ClientId, Content)
  when is_integer(ClientId), ClientId > 0 ->
    send_client_command(ClientId, {send_nearby, Content});
send_nearby(_ClientId, _Content) ->
    {error, invalid_client_id}.

join_map(ClientId, MapId)
  when is_integer(ClientId), ClientId > 0,
       is_integer(MapId), MapId >= 0, MapId =< 16#FFFF ->
    send_client_command(ClientId, {join_map, MapId});
join_map(ClientId, _MapId) when is_integer(ClientId), ClientId > 0 ->
    {error, invalid_map_id};
join_map(_ClientId, _MapId) ->
    {error, invalid_client_id}.

leave_map(ClientId) when is_integer(ClientId), ClientId > 0 ->
    send_client_command(ClientId, leave_map);
leave_map(_ClientId) ->
    {error, invalid_client_id}.

send_map(ClientId, Content) when is_integer(ClientId), ClientId > 0 ->
    send_client_command(ClientId, {send_map, Content});
send_map(_ClientId, _Content) ->
    {error, invalid_client_id}.

set_feedback(ClientId, Enabled)
  when is_integer(ClientId), ClientId > 0, is_boolean(Enabled) ->
    call_client(ClientId, {set_feedback, Enabled});
set_feedback(ClientId, _Enabled)
  when is_integer(ClientId), ClientId > 0 ->
    {error, invalid_feedback};
set_feedback(_ClientId, _Enabled) ->
    {error, invalid_client_id}.

position(ClientId) when is_integer(ClientId), ClientId > 0 ->
    call_client(ClientId, position);
position(_ClientId) ->
    {error, invalid_client_id}.

location(ClientId) when is_integer(ClientId), ClientId > 0 ->
    call_client(ClientId, location);
location(_ClientId) ->
    {error, invalid_client_id}.

start_clients(ClientId, EndId, _Host, _Port, _LoadConfig, Count)
  when ClientId > EndId ->
    {ok, Count};
start_clients(ClientId, EndId, Host, Port,
              {LoadMode, StartId, EndId} = LoadConfig, Count) ->
    %% 串行启动便于准确返回首个失败 ClientId；已启动客户端按约定保留。
    ClientMode = case LoadMode of
        normal -> {normal, ClientId, StartId, EndId};
        map_load -> map_load
    end,
    case chat_client_sup:start_client(
             ClientId,
             Host,
             Port,
             role_name(ClientId),
             ?DEFAULT_PASSWORD,
             ClientMode) of
        {ok, _ClientPid} ->
            start_clients(ClientId + 1, EndId, Host, Port,
                          LoadConfig, Count + 1);
        {error, Reason} ->
            {error, {client_start_failed, ClientId, Reason}}
    end.

existing_client(StartId, EndId) ->
    case client_children() of
        {ok, Children} ->
            ExistingIds = maps:from_list([
                {ClientId, true}
             || {{chat_client, ClientId}, ClientPid, worker, _Modules} <- Children,
                is_integer(ClientId),
                is_pid(ClientPid)
            ]),
            existing_client(StartId, EndId, ExistingIds);
        error ->
            none
    end.

existing_client(ClientId, EndId, _ExistingIds) when ClientId > EndId ->
    none;
existing_client(ClientId, EndId, ExistingIds) ->
    case maps:is_key(ClientId, ExistingIds) of
        true -> ClientId;
        false -> existing_client(ClientId + 1, EndId, ExistingIds)
    end.

send_client_command(ClientId, Command) ->
    case find_client(ClientId) of
        {ok, ClientPid} ->
            gen_server:cast(ClientPid, Command);
        error ->
            {error, {client_not_found, ClientId}}
    end.

call_client(ClientId, Request) ->
    case find_client(ClientId) of
        {ok, ClientPid} ->
            try gen_server:call(ClientPid, Request) of
                Reply -> Reply
            catch
                exit:_Reason -> {error, {client_not_found, ClientId}}
            end;
        error ->
            {error, {client_not_found, ClientId}}
    end.

find_client(ClientId) ->
    case client_children() of
        {ok, Children} ->
            case lists:keyfind({chat_client, ClientId}, 1, Children) of
                {{chat_client, ClientId}, ClientPid, worker, _Modules}
                  when is_pid(ClientPid) ->
                    {ok, ClientPid};
                false ->
                    error
            end;
        error ->
            error
    end.

client_children() ->
    try supervisor:which_children(chat_client_sup) of
        Children -> {ok, Children}
    catch
        exit:_Reason -> error
    end.

role_name(ClientId) ->
    <<"client_", (integer_to_binary(ClientId))/binary>>.
