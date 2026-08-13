-ifndef(CHAT_RECORD_HRL).
-define(CHAT_RECORD_HRL, true).

%% ETS 与频道进程共享的记录定义。

-record(role_account, {
    role_name,
    role_id,
    password
}).

-record(online_role, {
    role_name,
    role_pid
}).

-record(channel_state, {
    channel_id,
    channel_type,
    members = #{},
    member_monitors = #{},
    packets = [],
    batch_size = 0,
    batch_generation = 0,
    flush_ref = undefined
}).

%% batch_generation/batch_start 标记成员从当前批次的哪个位置开始收消息。
-record(channel_member, {
    role_pid,
    monitor_ref,
    batch_generation = 0,
    batch_start = 0
}).

-record(world_channel_member, {
    role_id,
    role_pid
}).

-endif.
