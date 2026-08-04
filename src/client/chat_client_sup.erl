-module(chat_client_sup).
-behaviour(supervisor).

-export([start_link/0, start_client/4]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_client(ClientId, Host, Port, Mode) ->
    ChildSpec = #{id => {chat_client, ClientId},
                  start => {chat_client, start_link, [Host, Port, Mode]},
                  restart => temporary,
                  shutdown => 5000,
                  type => worker,
                  modules => [chat_client]},
    supervisor:start_child(?MODULE, ChildSpec).

init([]) ->
    SupFlags = #{strategy => rest_for_one,
                 intensity => 5,
                 period => 10},
    ClientManager = #{id => client,
                      start => {client, start_link, []},
                      restart => permanent,
                      shutdown => 5000,
                      type => worker,
                      modules => [client]},
    {ok, {SupFlags, [ClientManager]}}.
