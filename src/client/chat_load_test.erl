-module(chat_load_test).

-export([start/2,
         start_observer/0,
         send_channel/3,
         send_private/3]).

-define(DEFAULT_HOST, "127.0.0.1").
-define(DEFAULT_PORT, 5555).
-define(DEFAULT_PASSWORD, <<"123456">>).
-define(OBSERVER_ROLE_NAME, <<"observer_001">>).

start(StartId, EndId)
  when is_integer(StartId), StartId > 0,
       is_integer(EndId), EndId >= StartId ->
    case existing_client(StartId, EndId) of
        none ->
            Host = application:get_env(chat, client_host, ?DEFAULT_HOST),
            Port = application:get_env(chat, port, ?DEFAULT_PORT),
            start_clients(StartId, EndId, Host, Port, 0);
        ClientId ->
            {error, {client_already_started, ClientId}}
    end;
start(_StartId, _EndId) ->
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

start_clients(ClientId, EndId, _Host, _Port, Count)
  when ClientId > EndId ->
    {ok, Count};
start_clients(ClientId, EndId, Host, Port, Count) ->
    case chat_client_sup:start_client(
             ClientId,
             Host,
             Port,
             role_name(ClientId),
             ?DEFAULT_PASSWORD,
             normal) of
        {ok, _ClientPid} ->
            start_clients(ClientId + 1, EndId, Host, Port, Count + 1);
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
