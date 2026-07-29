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
          start => {role_online_server, start_link, []},
          restart => permanent,
          shutdown => 5000,
          type => worker,
          modules => [role_online_server]},
        #{id => channel_manager,
          start => {channel_manager, start_link, []},
          restart => permanent,
          shutdown => 5000,
          type => worker,
          modules => [channel_manager]},
        #{id => channel_sup,
          start => {channel_sup, start_link, []},
          restart => permanent,
          shutdown => infinity,
          type => supervisor,
          modules => [channel_sup]},
        #{id => role_sup,
          start => {role_sup, start_link, []},
          restart => permanent,
          shutdown => infinity,
          type => supervisor,
          modules => [role_sup]},
        #{id => chat_listener,
          start => {chat_listener, start_link, []},
          restart => permanent,
          shutdown => 5000,
          type => worker,
          modules => [chat_listener]}
    ],
    {ok, {SupFlags, ChildSpecs}}.
