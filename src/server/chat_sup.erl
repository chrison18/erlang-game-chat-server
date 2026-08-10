-module(chat_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    SupFlags = #{strategy => rest_for_one,
                 intensity => 5,
                 period => 10},
    ChildSpecs = [
        #{id => role_online_server,
          start => {role_online_server, start_link, []}},
        #{id => map_server,
          start => {map_server, start_link, []}},
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
