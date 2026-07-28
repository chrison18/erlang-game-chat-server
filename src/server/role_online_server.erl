-module(role_online_server).
-behaviour(gen_server).

-include("chat_record.hrl").

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    role_accounts = ets:new(role_accounts, [
        named_table,
        set,
        private,
        {keypos, #role_account.role_name}
    ]),
    online_roles = ets:new(online_roles, [
        named_table,
        set,
        protected,
        {keypos, #online_role.role_name}
    ]),
    {ok, #{next_role_id => 1}}.

handle_call(Request, _From, State) ->
    {reply, {error, {unsupported_call, Request}}, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.
