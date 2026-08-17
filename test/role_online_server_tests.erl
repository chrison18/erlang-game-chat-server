-module(role_online_server_tests).

-include_lib("eunit/include/eunit.hrl").
-include("chat_record.hrl").

login_uses_calling_process_and_reconnects_test_() ->
    {setup,
     fun start_server/0,
     fun stop_server/1,
     fun(_ServerPid) ->
         fun() ->
             RoleName = <<"alice">>,
             Password = <<"secret">>,
             Parent = self(),
             LoggedInPid = spawn(fun() ->
                 Parent ! {login_result, self(),
                           role_online_server:login(RoleName, Password)},
                 receive stop -> ok end
             end),
             receive
                 {login_result, LoggedInPid, LoginResult} ->
                     ?assertEqual({ok, 1}, LoginResult)
             after 500 ->
                 ?assert(false)
             end,
             [#online_role{role_name = RoleName,
                           role_pid = LoggedInPid}] =
                 ets:tab2list(online_roles),
             ?assertEqual({error, already_online},
                          role_online_server:login(RoleName, Password)),
             ?assertEqual({error, invalid_login},
                          role_online_server:login(
                              RoleName, <<"wrong password">>)),
             LoggedInPid ! stop,
             ?assertEqual(ok, wait_until_offline(RoleName, 100)),
             ?assertEqual({ok, 1},
                          role_online_server:login(RoleName, Password))
         end
     end}.

start_server() ->
    {ok, ServerPid} = role_online_server:start_link(),
    ServerPid.

stop_server(ServerPid) ->
    gen_server:stop(ServerPid).

wait_until_offline(_RoleName, 0) ->
    timeout;
wait_until_offline(RoleName, Attempts) ->
    case ets:member(online_roles, RoleName) of
        false ->
            ok;
        true ->
            timer:sleep(10),
            wait_until_offline(RoleName, Attempts - 1)
    end.
