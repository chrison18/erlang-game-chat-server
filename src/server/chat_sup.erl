-module(chat_sup).
-behaviour(supervisor).

%% 服务端顶层监督树。子进程顺序同时表达状态依赖关系。

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    %% 上游状态 owner 重启时，rest_for_one 会重建依赖它的下游进程。
    SupFlags = #{strategy => rest_for_one,
                 intensity => 5,
                 period => 10},
    ChildSpecs = [
        #{id => role_online_server,
          start => {role_online_server, start_link, []}},
        #{id => map_server_sup,
          start => {map_server_sup, start_link, []},
          type => supervisor},
        channel_server:child_spec(1),
        #{id => channel_sup,
          start => {channel_sup, start_link, []},
          type => supervisor},
        #{id => role_sup,
          start => {role_sup, start_link, []},
          type => supervisor},
        #{id => chat_listener,
          start => {chat_listener, start_link, []}}
    ],
    {ok, {SupFlags, ChildSpecs}}.
