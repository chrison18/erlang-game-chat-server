-module(role_online_server).
-behaviour(gen_server).

-include("chat_record.hrl").

-export([start_link/0, login/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

login(RolePid, RoleName, Password) ->
    gen_server:call(?MODULE, {login, RolePid, RoleName, Password}).

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
    {ok, #{next_role_id => 1, monitors => #{}}}.

handle_call({login, RolePid, RoleName, Password}, _From, State) ->
    login_role(RolePid, RoleName, Password, State);
handle_call(Request, _From, State) ->
    {reply, {error, {unsupported_call, Request}}, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({'DOWN', MonitorRef, process, _RolePid, _Reason},
            #{monitors := Monitors} = State) ->
    case maps:take(MonitorRef, Monitors) of
        {RoleName, RemainingMonitors} ->
            true = ets:delete(online_roles, RoleName),
            {noreply, State#{monitors := RemainingMonitors}};
        error ->
            {noreply, State}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

login_role(RolePid, RoleName, Password,
           #{next_role_id := NextRoleId} = State) ->
    case ets:lookup(role_accounts, RoleName) of
        [] ->
            true = ets:insert(role_accounts, #role_account{
                role_name = RoleName,
                role_id = NextRoleId,
                password = Password
            }),
            NewState = add_online_role(RolePid, RoleName, State),
            {reply, {ok, NextRoleId},
            NewState#{next_role_id := NextRoleId + 1}};
        [#role_account{role_id = RoleId, password = Password}] ->
            case ets:member(online_roles, RoleName) of
                % 账号在线
                true ->
                    {reply, {error, already_online}, State};
                % 账号存在但离线
                false ->
                    NewState = add_online_role(RolePid, RoleName, State),
                    {reply, {ok, RoleId}, NewState}
            end;
        [#role_account{}] ->
            {reply, {error, invalid_login}, State} % 密码错误
    end.

add_online_role(RolePid, RoleName, #{monitors := Monitors} = State) ->
    MonitorRef = erlang:monitor(process, RolePid),
    true = ets:insert(online_roles, #online_role{
        role_name = RoleName,
        role_pid = RolePid
    }),
    State#{monitors := Monitors#{MonitorRef => RoleName}}.
