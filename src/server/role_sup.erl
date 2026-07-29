-module(role_sup).
-behaviour(supervisor).

-export([start_link/0, start_role/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_role() ->
    ChildSpec = #{id => make_ref(),
                  start => {role_server, start_link, []},
                  restart => temporary,
                  shutdown => 5000,
                  type => worker,
                  modules => [role_server]},
    supervisor:start_child(?MODULE, ChildSpec).

init([]) ->
    SupFlags = #{strategy => one_for_one,
                 intensity => 5,
                 period => 10},
    {ok, {SupFlags, []}}.
