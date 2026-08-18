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
    flush_ref = undefined
}).

-record(world_channel_member, {
    role_id,
    role_pid
}).

-endif.
