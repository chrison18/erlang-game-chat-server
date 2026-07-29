-module(chat_client_sup).
-behaviour(supervisor).

-export([start_link/0, start_client/2]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_client(Host, Port) ->
    ChildSpec = #{id => make_ref(),
                  start => {chat_client, start_link, [Host, Port]},
                  restart => temporary,
                  shutdown => 5000,
                  type => worker,
                  modules => [chat_client]},
    supervisor:start_child(?MODULE, ChildSpec).

init([]) ->
    SupFlags = #{strategy => one_for_one,
                 intensity => 5,
                 period => 10},
    {ok, {SupFlags, []}}.
