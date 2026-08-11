-module(chat_metrics).

-include("chat_record.hrl").

-export([snapshot/0, online_clients/0, print_online_clients/0]).

snapshot() ->
    {RoleCount, RoleQueueTotal, RoleQueueMax} =
        queue_stats(child_pids(role_sup, role_server)),
    {ChannelCount, ChannelQueueTotal, ChannelQueueMax} =
        queue_stats(child_pids(chat_sup, channel_server) ++
                    child_pids(channel_sup, channel_server)),
    {WorkerCount, WorkerQueueTotal, WorkerQueueMax} =
        queue_stats(child_pids(channel_sup, world_broadcast_worker)),
    #{node => node(),
      schedulers_online => erlang:system_info(schedulers_online),
      online_count => table_size(online_roles),
      world_member_count => lists:sum([
          table_size(Table) || Table <- channel_server:world_member_tables()]),
      role_count => RoleCount,
      role_queue_total => RoleQueueTotal,
      role_queue_max => RoleQueueMax,
      channel_count => ChannelCount,
      channel_queue_total => ChannelQueueTotal,
      channel_queue_max => ChannelQueueMax,
      world_worker_count => WorkerCount,
      world_worker_queue_total => WorkerQueueTotal,
      world_worker_queue_max => WorkerQueueMax,
      beam_process_count => erlang:system_info(process_count),
      beam_port_count => erlang:system_info(port_count),
      beam_memory_mb => erlang:memory(total) / (1024 * 1024)}.

online_clients() ->
    lists:sort([
        online_client(RoleName, RolePid)
     || #online_role{role_name = RoleName, role_pid = RolePid} <-
            ets:tab2list(online_roles)
    ]).

print_online_clients() ->
    Clients = online_clients(),
    lists:foreach(fun(Client) -> io:format("~p~n", [Client]) end, Clients),
    {ok, length(Clients)}.

online_client(RoleName, RolePid) ->
    Dictionary = case process_info(RolePid, dictionary) of
        {dictionary, Values} -> Values;
        undefined -> []
    end,
    QueueLength = case process_info(RolePid, message_queue_len) of
        {message_queue_len, Length} -> Length;
        undefined -> undefined
    end,
    #{role_id => proplists:get_value(role_id, Dictionary),
      role_name => RoleName,
      role_pid => RolePid,
      message_queue_len => QueueLength,
      map_id => proplists:get_value(map_id, Dictionary),
      position => proplists:get_value(position, Dictionary)}.

child_pids(Supervisor, Module) ->
    try supervisor:which_children(Supervisor) of
        Children ->
            [ChildPid
             || {_Id, ChildPid, worker, Modules} <- Children,
                is_pid(ChildPid),
                lists:member(Module, Modules)]
    catch
        exit:_Reason -> []
    end.

queue_stats(Pids) ->
    lists:foldl(fun add_queue_length/2, {0, 0, 0}, Pids).

add_queue_length(Pid, {Count, Total, Max}) ->
    case process_info(Pid, message_queue_len) of
        {message_queue_len, Length} ->
            {Count + 1, Total + Length, erlang:max(Max, Length)};
        undefined ->
            {Count, Total, Max}
    end.

table_size(Table) ->
    case ets:info(Table, size) of
        undefined -> 0;
        Size -> Size
    end.
