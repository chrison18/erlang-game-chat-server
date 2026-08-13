-module(chat_client_sup).
-behaviour(supervisor).

%% 压测客户端的动态监督者，每个 ClientId 对应一个 chat_client。

-export([start_link/0, start_client/6]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_client(ClientId, Host, Port, RoleName, Password, Mode) ->
    ChildSpec = #{id => {chat_client, ClientId},
                  start => {chat_client, start_link,
                            [Host, Port, RoleName, Password, Mode]},
                  restart => temporary},
    supervisor:start_child(?MODULE, ChildSpec).

init([]) ->
    {ok, {#{strategy => one_for_one}, []}}.
