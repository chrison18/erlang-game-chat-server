-module(role_sup).
-behaviour(supervisor).

%% 动态监督每条客户端连接对应的 role_server，连接结束后不重启。

-export([start_link/0, start_role/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_role() ->
    ChildSpec = #{id => make_ref(),
                  start => {role_server, start_link, []},
                  restart => temporary},
    supervisor:start_child(?MODULE, ChildSpec).

init([]) ->
    {ok, {#{strategy => one_for_one}, []}}.
