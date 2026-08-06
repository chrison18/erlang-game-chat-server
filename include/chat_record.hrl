-ifndef(CHAT_RECORD_HRL).
-define(CHAT_RECORD_HRL, true).

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
    member_monitors = #{}
}).

-record(channel_member, {
    role_pid,
    monitor_ref
}).

-record(world_channel_member, {
    role_id,
    role_pid
}).

-endif.
