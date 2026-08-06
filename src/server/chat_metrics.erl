-module(chat_metrics).

-export([snapshot/0]).

snapshot() ->
    {RoleCount, RoleQueueTotal, RoleQueueMax} =
        queue_stats(child_pids(role_sup, role_server)),
    {ChannelCount, ChannelQueueTotal, ChannelQueueMax} =
        queue_stats(child_pids(channel_sup, channel_server)),
    {WorkerCount, WorkerQueueTotal, WorkerQueueMax} =
        queue_stats(child_pids(channel_sup, world_broadcast_worker)),
    #{node => node(),
      schedulers_online => erlang:system_info(schedulers_online),
      online_count => table_size(online_roles),
      world_member_count => table_size(world_channel_members),
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

child_pids(Supervisor, Module) ->
    case whereis(Supervisor) of
        undefined ->
            [];
        _SupervisorPid ->
            [ChildPid
             || {_Id, ChildPid, worker, Modules} <-
                    supervisor:which_children(Supervisor),
                is_pid(ChildPid),
                lists:member(Module, Modules)]
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
