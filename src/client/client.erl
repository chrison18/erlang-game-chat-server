-module(client).
-behaviour(gen_server).

-export([start_link/0,
         start_client/2,
         start_observer/0,
         send_channel/3,
         send_private/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-define(DEFAULT_HOST, "127.0.0.1").
-define(DEFAULT_PORT, 5555).
-define(DEFAULT_PASSWORD, <<"123456">>).
-define(OBSERVER_ROLE_NAME, <<"observer_001">>).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

start_client(StartId, EndId)
  when is_integer(StartId), StartId > 0,
       is_integer(EndId), EndId >= StartId ->
    gen_server:call(?MODULE, {start_client, StartId, EndId}, infinity);
start_client(_StartId, _EndId) ->
    {error, invalid_client_range}.

start_observer() ->
    gen_server:call(?MODULE, start_observer, infinity).

send_channel(ClientId, ChannelId, Content)
  when is_integer(ClientId), ClientId > 0 ->
    gen_server:call(
        ?MODULE, {send_channel, ClientId, ChannelId, Content});
send_channel(_ClientId, _ChannelId, _Content) ->
    {error, invalid_client_id}.

send_private(SenderId, TargetId, Content)
  when is_integer(SenderId), SenderId > 0,
       is_integer(TargetId), TargetId > 0 ->
    gen_server:call(
        ?MODULE, {send_private, SenderId, TargetId, Content});
send_private(_SenderId, _TargetId, _Content) ->
    {error, invalid_client_id}.

init([]) ->
    Host = application:get_env(chat, client_host, ?DEFAULT_HOST),
    Port = application:get_env(chat, port, ?DEFAULT_PORT),
    {ok, #{host => Host,
           port => Port,
           clients => #{},
           monitors => #{}}}.

handle_call({start_client, StartId, EndId}, _From,
            #{clients := Clients} = State) ->
    case find_existing_client(StartId, EndId, Clients) of
        none ->
            case start_clients(StartId, EndId, State, 0) of
                {ok, Count, NewState} ->
                    {reply, {ok, Count}, NewState};
                {error, Reason, NewState} ->
                    {reply, {error, Reason}, NewState}
            end;
        ClientId ->
            {reply, {error, {client_already_started, ClientId}}, State}
    end;
handle_call(start_observer, _From,
            #{host := Host,
              port := Port,
              clients := Clients,
              monitors := Monitors} = State) ->
    case find_client_pid(observer, Clients) of
        {ok, _ObserverPid} ->
            {reply, {error, observer_already_started}, State};
        error ->
            case chat_client_sup:start_client(
                     observer, Host, Port, observer) of
                {ok, ObserverPid} ->
                    MonitorRef = erlang:monitor(process, ObserverPid),
                    ObserverPid ! {login, ?OBSERVER_ROLE_NAME,
                                    ?DEFAULT_PASSWORD},
                    NewState = State#{
                        clients := Clients#{
                            observer => {ObserverPid, MonitorRef}},
                        monitors := Monitors#{MonitorRef => observer}
                    },
                    {reply, ok, NewState};
                {error, Reason} ->
                    {reply, {error, {observer_start_failed, Reason}}, State}
            end
    end;
handle_call({send_channel, ClientId, ChannelId, Content}, _From,
            #{clients := Clients} = State) ->
    case find_client_pid(ClientId, Clients) of
        {ok, ClientPid} ->
            ClientPid ! {send_channel, ChannelId, Content},
            {reply, ok, State};
        error ->
            {reply, {error, {client_not_found, ClientId}}, State}
    end;
handle_call({send_private, SenderId, TargetId, Content}, _From,
            #{clients := Clients} = State) ->
    case {find_client_pid(SenderId, Clients),
          find_client_pid(TargetId, Clients)} of
        {{ok, SenderPid}, {ok, _TargetPid}} ->
            SenderPid ! {send_private, role_name(TargetId), Content},
            {reply, ok, State};
        {error, _TargetResult} ->
            {reply, {error, {client_not_found, SenderId}}, State};
        {_SenderResult, error} ->
            {reply, {error, {client_not_found, TargetId}}, State}
    end;
handle_call(Request, _From, State) ->
    {reply, {error, {unsupported_call, Request}}, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({'DOWN', MonitorRef, process, _ClientPid, _Reason},
            #{clients := Clients, monitors := Monitors} = State) ->
    case maps:take(MonitorRef, Monitors) of
        {ClientId, RemainingMonitors} ->
            {noreply, State#{clients := maps:remove(ClientId, Clients),
                             monitors := RemainingMonitors}};
        error ->
            {noreply, State}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

start_clients(ClientId, EndId, State, Count) when ClientId > EndId ->
    {ok, Count, State};
start_clients(ClientId, EndId,
              #{host := Host,
                port := Port,
                clients := Clients,
                monitors := Monitors} = State,
              Count) ->
    case chat_client_sup:start_client(ClientId, Host, Port, normal) of
        {ok, ClientPid} ->
            MonitorRef = erlang:monitor(process, ClientPid),
            ClientPid ! {login, role_name(ClientId), ?DEFAULT_PASSWORD},
            NewState = State#{
                clients := Clients#{ClientId => {ClientPid, MonitorRef}},
                monitors := Monitors#{MonitorRef => ClientId}
            },
            start_clients(ClientId + 1, EndId, NewState, Count + 1);
        {error, Reason} ->
            {error, {client_start_failed, ClientId, Reason}, State}
    end.

find_existing_client(ClientId, EndId, _Clients)
  when ClientId > EndId ->
    none;
find_existing_client(ClientId, EndId, Clients) ->
    case maps:is_key(ClientId, Clients) of
        true -> ClientId;
        false -> find_existing_client(ClientId + 1, EndId, Clients)
    end.

find_client_pid(ClientId, Clients) ->
    case maps:find(ClientId, Clients) of
        {ok, {ClientPid, _MonitorRef}} ->
            case is_process_alive(ClientPid) of
                true -> {ok, ClientPid};
                false -> error
            end;
        error ->
            error
    end.

role_name(ClientId) ->
    <<"client_", (integer_to_binary(ClientId))/binary>>.
