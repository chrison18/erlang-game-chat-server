# Chat V1\.1 设计文档

### 修改历史

|日期|修改内容|
|---|---|
|2026\-07\-28|项目改名为 `chat`；调整服务端和客户端模块；完善登录、固定频道、私聊、ETS 数据、TCP 协议及运行流程设计。|

## **1\. 项目目标**

使用 Erlang/OTP 实现一个简单的 TCP 聊天程序。项目名为 `chat`，服务端和客户端都由本项目实现。

V1\.0 只完成三个基础功能：

- 登录

- 多频道聊天

- 在线角色私聊



## **2\. 整体架构**

### **2\.1 进程架构图**

实线表示 Supervisor 的启动和监督关系，虚线表示普通调用、数据归属或动态启动关系。

核心进程职责：

- 一个 role\_server 对应一个在线玩家和一条 TCP 连接。

- role\_online\_server 管理账号和在线角色。

- channel\_manager 只管理频道资料和频道 PID。

- 每个 channel\_server 独立管理自己的频道成员。

- 频道聊天和私聊由玩家 role\_server 主动发起。



### **2\.2 主要消息流程图**

登录过程中，role\_online\_server 只处理账号和在线状态。频道加入由玩家 role\_server 直接调用各个 channel\_server。



频道消息广播产生的 2009 推送不在这张图中逐个展开。图中只表达发送方、频道进程的检查与 2008 发送结果。

channel\_manager 不经过任何一条频道聊天消息。



私聊不会经过 role\_online\_server 的消息邮箱。发送方 role\_server 直接读取 online\_roles ETS，再把私聊消息发送给目标 role\_server。



## 3\. 技术约定和项目目录

### 3\.1 技术约定

- 使用单个 Erlang 节点和单个 OTP Application。

- 服务端和客户端通过 TCP 长连接通信。

- 使用 `gen_tcp`，不引入第三方网络库。

- Socket 使用 `{packet, 4}` 处理 TCP 半包和粘包。

- `role_online_server`、`role_server`、`channel_manager`、`channel_server` 和 `chat_client` 使用 `gen_server`。

- 使用 Supervisor 管理固定服务、频道进程、玩家进程和客户端进程。

- 业务进程通过 `gen_server:call/2`、`gen_server:cast/2` 或普通消息通信，不传递 `fun` 执行业务。

- 使用 ETS 保存账号、在线角色和频道资料。

- ETS 中的数据使用 record，方便以后增加字段。

- 每个 `role_server` 使用进程字典保存当前玩家自己的业务状态。

- 暂不使用 `rebar3`，通过 `erlc` 和 Shell 脚本编译、启动。

- V1\.0 不增加心跳、空闲超时和没有实际用途的定时任务。



### 3\.2 项目目录

```Plain Text
chat/
├── src/
│   ├── server/
│   │   ├── chat.app.src
│   │   ├── chat_app.erl
│   │   ├── chat_sup.erl
│   │   ├── chat_listener.erl
│   │   ├── role_sup.erl
│   │   ├── role_server.erl
│   │   ├── role_online_server.erl
│   │   ├── chat_server_protocol.erl
│   │   ├── channel_sup.erl
│   │   ├── channel_manager.erl
│   │   └── channel_server.erl
│   └── client/
│       ├── chat_client_sup.erl
│       ├── chat_client_manager.erl
│       ├── chat_client.erl
│       └── chat_client_protocol.erl
├── include/
│   ├── chat_protocol.hrl
│   └── chat_record.hrl
├── ebin/
├── scripts/
│   ├── compile.sh
│   ├── start_server.sh
│   └── start_client.sh
├── README.md
└── CHAT_SERVER_DESIGN.md
```



### 3\.3 公共头文件

`chat_protocol.hrl` 保存客户端和服务端共用的 `ProtoId`、`ResultCode` 和 `ChannelType`。

`chat_record.hrl` 保存服务端数据 record：

```Plain Text
role_account
online_role
channel_info
channel_state
channel_member
```

客户端只需要包含协议头文件，不直接读取服务端 ETS。



### 3\.4 模块改名说明

```Plain Text
chat_connection_sup.erl -> role_sup.erl
chat_connection.erl     -> role_server.erl
旧 role_server.erl       -> role_online_server.erl
```

新设计中不再保留 `chat_connection` 这个模块名。一个 TCP 连接对应一个玩家主体，因此由 `role_server` 同时负责玩家状态和 Socket。



## 4\. 模块和监督设计

### 4\.1 服务端模块

|模块|类型|主要职责|
|---|---|---|
|`chat_app`|application|启动服务端根监督进程 `chat_sup`|
|`chat_sup`|supervisor|按顺序启动所有服务端固定进程|
|`chat_listener`|gen\_server|监听 TCP 端口；收到连接后启动 `role_server` 并转交 Socket|
|`role_sup`|supervisor|动态监督玩家进程，一个连接对应一个 `role_server`|
|`role_server`|gen\_server|代表一个在线玩家；持有 Socket 和玩家状态；处理登录、频道聊天和私聊|
|`role_online_server`|gen\_server|创建账号、验证密码、分配 RoleId、管理在线角色 ETS|
|`chat_server_protocol`|普通模块|解码客户端请求，编码服务端结果和消息推送|
|`channel_sup`|supervisor|监督 `main` 和 9 个公共频道进程|
|`channel_manager`|gen\_server|创建 `channel_info` ETS，记录频道 ID、名称、类型和 PID|
|`channel_server`|gen\_server|管理一个频道的成员，处理加入、退出、成员检查和消息广播|

服务端进程结构：

```Plain Text
chat_app
└── chat_sup
    ├── role_online_server
    ├── channel_manager
    ├── channel_sup
    │   ├── channel_server main
    │   └── 9 个 public channel_server
    ├── role_sup
    │   └── 动态 role_server
    └── chat_listener
```

固定服务和固定频道进程使用 `permanent`。动态玩家进程 `role_server` 使用 `temporary`，断线后不使用旧 Socket 自动重启。

一个 `role_server` 对应：

```Plain Text
一个在线玩家
一个 RoleId
一个 RoleName
一条 TCP 长连接
一份玩家进程字典
```

主要业务路径：

```Plain Text
登录：
role_server -> role_online_server

频道聊天：
role_server -> channel_server -> 成员 role_server

私聊：
role_server -> online_roles ETS -> 目标 role_server
```

`channel_manager` 不保存频道成员，也不经过频道聊天消息。每个 `channel_server` 独立保存自己的成员并处理本频道广播。



### 4\.2 客户端模块

|模块|类型|主要职责|
|---|---|---|
|`chat_client_sup`|supervisor|动态监督多个客户端进程|
|`chat_client_manager`|普通模块|提供客户端启动、停止和查看接口|
|`chat_client`|gen\_server|代表一个客户端用户，持有一条 TCP 长连接|
|`chat_client_protocol`|普通模块|编码客户端请求，解码服务端结果和推送|

`chat_client` 使用 `temporary`。V1\.0 只实现手动启动和操作客户端，批量客户端和自动压力测试放到后续版本。



## 5\. 数据设计

### 5\.1 record 定义

服务端数据 record 放在 `include/chat_record.hrl`：

```Erlang
-record(role_account, {
    role_name,
    role_id,
    password
}).

-record(online_role, {
    role_name,
    role_id,
    role_pid,
    monitor_ref
}).

-record(channel_info, {
    channel_id,
    channel_type,
    channel_name,
    channel_pid
}).

-record(channel_state, {
    channel_id,
    channel_type,
    channel_name,
    members = #{}
}).

-record(channel_member, {
    role_id,
    role_pid,
    monitor_ref
}).
```



### 5\.2 账号和在线角色 ETS

`role_online_server` 创建两张 `named_table set` ETS。record 的第一个元组元素是 record 名称，因此使用 `keypos` 明确业务主键：

```Erlang
ets:new(role_accounts, [
    named_table,
    set,
    private,
    {keypos, #role_account.role_name}
]).

ets:new(online_roles, [
    named_table,
    set,
    protected,
    {keypos, #online_role.role_name}
]).
```

`role_accounts` 保存 RoleName、RoleId 和密码，只有 `role_online_server` 可以读写。`online_roles` 保存当前在线的 RolePid，`role_online_server` 负责写入和删除，其他 `role_server` 可以读取它查找私聊目标。

登录规则：

- RoleName 不存在时，使用本次密码创建新账号并分配 RoleId。

- RoleName 已存在且离线时，密码正确即可使用原 RoleId 登录。

- 密码错误返回 `invalid_login`。

- RoleName 已在线时返回 `already_online`。

- V1\.0 不持久化 ETS，服务端重启后账号数据重新开始。

    

### 5\.3 频道资料和成员状态

`channel_manager` 创建 `protected named_table set`：

```Erlang
ets:new(channel_info, [
    named_table,
    set,
    protected,
    {keypos, #channel_info.channel_id}
]).
```

`channel_info` 只保存固定频道的 ID、类型、名称和最新 ChannelPid。`channel_manager` 负责写入，各个 `role_server` 直接读取。

服务端固定创建 `1 = main`，以及 `2` 到 `10 = public_1` 到 `public_9`。每个 `channel_server` 在自己的 `#channel_state.members` Map 中保存成员：

```Erlang
Members = #{
    RoleId => #channel_member{
        role_id = RoleId,
        role_pid = RolePid,
        monitor_ref = MonitorRef
    }
}.
```

加入成功后，`channel_server` 监控 RolePid。主动退出时删除成员并取消监控；RolePid 退出时通过 `DOWN` 消息删除成员。不存在中央 `channel_members` ETS。



### 5\.4 role\_server 玩家状态

Socket 保存在 `role_server` 的 `gen_server State` 中。登录成功后，玩家业务状态写入当前进程的进程字典：

```Erlang
put(role_id, RoleId),
put(role_name, RoleName),
put(channel_ids, #{
    1 => true,
    3 => true,
    7 => true
}).
```

`channel_ids` 只保存已加入的 ChannelId，不长期缓存 ChannelPid。发送消息时先检查自己的`channel_ids`，再读取 `channel_info` ETS 取得最新 ChannelPid。加入或退出成功后同步更新 `channel_ids`，断线后进程字典随 `role_server` 一起消失。



## 6\. 接口和进程消息

### 6\.1 服务端接口

|接口|方式|返回值或作用|
|---|---|---|
|`role_sup:start_role()`|Supervisor API|`{ok, RolePid} | {error, Reason}`|
|`role_online_server:login(RolePid, RoleName, Password)`|`call`|`{ok, RoleId} | {error, Reason}`|
|`channel_manager:register_channel(ChannelId, Type, Name, ChannelPid)`|`call`|`ok`，登记固定频道|
|`channel_server:join(ChannelPid, RoleId, RolePid)`|`call`|`{ok, ChannelId} | {error, already_joined}`|
|`channel_server:leave(ChannelPid, RoleId)`|`call`|`{ok, ChannelId} | {error, Reason}`|
|`channel_server:send_channel(ChannelPid, RoleId, RoleName, Content)`|`call`|`{ok, ChannelId} | {error, not_joined}`|

`role_server` 直接调用对应的 `channel_server`，不再通过 `channel_manager` 加入、退出或发送消息。频道发送检查成功后，`channel_server` 遍历自己的成员并发送：

```Erlang
gen_server:cast(MemberRolePid, {
    push_channel,
    ChannelId,
    SenderRoleId,
    SenderRoleName,
    Content
}).
```

发送私聊时，发送方 `role_server` 直接读取 `online_roles` ETS。目标在线时向目标玩家进程发送：

```Erlang
gen_server:cast(TargetRolePid, {
    push_private,
    SenderRoleId,
    SenderRoleName,
    Content
}).
```

目标 `role_server` 收到结构化消息后，调用 `chat_server_protocol` 编码，再通过自己的 Socket 推送给客户端。



### 6\.2 role\_server 处理关系

|客户端请求|role\_server 的处理|
|---|---|
|登录|`call role_online_server`；成功后直接加入固定频道|
|查询频道|读取 `channel_info` ETS，并结合自己的 `channel_ids` 返回|
|加入频道|读取 ChannelPid，`call channel_server`，成功后更新进程字典|
|退出频道|读取 ChannelPid，`call channel_server`，成功后更新进程字典|
|频道聊天|本地检查后 `call channel_server`，根据结果发送 `2008`|
|私聊|读取 `online_roles`，cast 给目标 RolePid，再发送 `3002`|

`role_online_server` 监控登录成功的 RolePid，并在收到 `DOWN` 后删除 `online_roles`。每个 `channel_server` 也独立监控自己的成员，并在收到 `DOWN` 后删除成员。



### 6\.3 客户端接口

|接口|返回值|
|---|---|
|`chat_client_manager:start_client(Host, Port)`|`{ok, ClientPid} | {error, Reason}`|
|`chat_client_manager:stop_client(ClientPid)`|`ok`|
|`chat_client_manager:list_clients()`|`[ClientPid]`|
|`chat_client:login(ClientPid, RoleName, Password)`|`{ok, RoleId, ChannelIds} | {error, Reason}`|
|`chat_client:list_channels(ClientPid)`|`{ok, ChannelList} | {error, Reason}`|
|`chat_client:join_channel(ClientPid, ChannelId)`|`{ok, ChannelId} | {error, Reason}`|
|`chat_client:leave_channel(ClientPid, ChannelId)`|`{ok, ChannelId} | {error, Reason}`|
|`chat_client:send_channel(ClientPid, ChannelId, Content)`|`{ok, ChannelId} | {error, Reason}`|
|`chat_client:send_private(ClientPid, TargetName, Content)`|`{ok, TargetName} | {error, target_offline}`|

需要服务端结果时，`chat_client` 在 `handle_call` 中发送 TCP 请求并暂存 `From`，收到结果报文后调用 `gen_server:reply/2`。协议没有 `RequestId`，所以每个客户端同时只允许一个等待结果的请求，新请求返回 `{error, request_busy}`。

频道发送等待 `2008`，私聊发送等待 `3002`。客户端收到 `2009` 频道推送或 `3003` 私聊推送时直接打印。



## 7\. TCP 二进制协议

### 7\.1 外层格式和字段

监听和连接 Socket 使用 `[binary, {packet, 4}, {active, false}]`。完成 Socket 控制权转交后，`role_server` 改为 `{active, once}`。网络格式为：

```Erlang
<<PacketLength:32, ProtoId:16, Data/binary>>
```

`PacketLength` 由 `gen_tcp` 自动添加和去除，业务代码处理 `<<ProtoId:16, Data/binary>>`。每处理一条 `{tcp, Socket, Packet}` 后重新设置 `{active, once}`。

|字段|位数|
|---|---|
|`ProtoId`|16|
|`ResultCode`|8|
|`RoleId`|32|
|`ChannelId`|32|
|字符串长度|16|
|频道数量|16|

整数使用默认大端序，字符串和聊天内容使用 UTF\-8 binary。位于报文末尾的 `Password` 或 `Content` 不再增加长度字段，由 `{packet, 4}` 的完整报文边界确定结束位置。



### 7\.2 ProtoId

|ProtoId|含义|ProtoId|含义|
|---|---|---|---|
|1001|登录请求|1002|登录结果|
|2001|查询频道请求|2002|查询频道结果|
|2003|加入频道请求|2004|加入频道结果|
|2005|退出频道请求|2006|退出频道结果|
|2007|发送频道消息|2008|频道发送结果|
|2009|推送频道消息|3001|发送私聊|
|3002|私聊发送结果|3003|推送私聊|
|9001|通用错误|||



### 7\.3 登录

```Erlang
请求：<<1001:16, NameLength:16, RoleName:NameLength/binary, Password/binary>>
成功：<<1002:16, 0:8, RoleId:32, ChannelCount:16, ChannelIds/binary>>
失败：<<1002:16, ResultCode:8>>
```

`ChannelIds` 由 `ChannelCount` 个 32 位 `ChannelId` 组成。结果码：`0 = success`、`1 = invalid_login`、`2 = already_online`。



### 7\.4 查询频道

```Erlang
请求：<<2001:16>>
结果：<<2002:16, ChannelCount:16, ChannelList/binary>>
频道项：<<ChannelId:32, ChannelType:8, Joined:8,
          NameLength:16, ChannelName:NameLength/binary>>
```

`ChannelType`：`1 = main`、`2 = public`；`Joined`：`0 = 未加入`、`1 = 已加入`。



### 7\.5 加入和退出频道

```Erlang
加入请求：<<2003:16, ChannelId:32>>
加入结果：<<2004:16, ResultCode:8, ChannelId:32>>
退出请求：<<2005:16, ChannelId:32>>
退出结果：<<2006:16, ResultCode:8, ChannelId:32>>
```

加入结果码：`0 = success`、`1 = invalid_channel`、`2 = already_joined`。退出结果码：`0 = success`、`1 = invalid_channel`、`2 = not_joined`、`3 = cannot_leave_main`。



### 7\.6 频道聊天

```Erlang
发送：<<2007:16, ChannelId:32, Content/binary>>
结果：<<2008:16, ResultCode:8, ChannelId:32>>
推送：<<2009:16, ChannelId:32, SenderRoleId:32,
        SenderNameLength:16,
        SenderRoleName:SenderNameLength/binary, Content/binary>>
```

结果码：`0 = success`、`1 = invalid_channel`、`2 = not_joined`。

客户端不发送自己的身份。发送方 `role_server` 从进程字典取得发送者信息，并先检查自己是否加入了该频道；`channel_server` 再检查成员状态，向频道全部在线成员推送 `2009`，包括发送者本人。处理完成后，发送方客户端收到 `2008` 发送结果。



### 7\.7 私聊

```Erlang
发送：<<3001:16, TargetNameLength:16,
        TargetRoleName:TargetNameLength/binary, Content/binary>>
结果：<<3002:16, ResultCode:8, TargetNameLength:16,
        TargetRoleName:TargetNameLength/binary>>
推送：<<3003:16, SenderRoleId:32, SenderNameLength:16,
        SenderRoleName:SenderNameLength/binary, Content/binary>>
```

私聊使用 `TargetRoleName` 查找目标玩家，不要求客户端知道对方的 `RoleId`。结果码：`0 = success`、`1 = target_offline`。成功表示在 `online_roles` 中找到了目标 `RolePid`，并向目标 `role_server` 发出了推送消息，不表示对方已经阅读。



### 7\.8 通用错误

```Erlang
<<9001:16, RequestProtoId:16, ErrorCode:8>>
```

错误码：`1 = not_logged_in`、`2 = invalid_packet`、`3 = unknown_proto`。`9001` 只处理无法进入具体业务流程的通用错误。加入频道、退出频道和发送频道消息产生的错误，分别通过 `2004`、`2006` 和 `2008` 返回。



## 8\. 主要运行流程

### 8\.1 服务端启动

1. `chat_app` 启动根监督进程 `chat_sup`。

2. `role_online_server` 创建账号表 `role_accounts` 和在线表 `online_roles`。

3. `channel_manager` 创建 `channel_info`，`channel_sup` 启动 `main` 和 9 个公共频道进程，并登记频道资料。

4. `role_sup` 准备动态监督玩家进程，`chat_listener` 开始监听 TCP 端口。

    

### 8\.2 接收连接

1. `chat_listener` 使用 `gen_tcp:accept/1` 等待连接，不设置定时唤醒。

2. 接收 Socket 后，调用 `role_sup:start_role()` 启动一个等待 Socket 的 `role_server`。

3. `chat_listener` 使用 `gen_tcp:controlling_process/2` 把 Socket 控制权转交给该 `role_server`。

4. 转交成功后发送 `{socket_ready, Socket}`；`role_server` 保存 Socket，并设置 `{active, once}` 接收数据。

5. 任一步骤失败都关闭新 Socket；成功后 `chat_listener` 继续等待下一个连接。

    

### 8\.3 登录

1. `role_server` 收到 `1001`，解码出 `RoleName` 和 `Password`，再 `call role_online_server`。

2. 新 RoleName 自动创建账号并分配 RoleId；已有账号检查密码和在线状态。

3. 登录成功后，`role_online_server` 写入 `online_roles` 并监控 RolePid。

4. `role_server` 加入必须进入的 `main`，再随机加入 1 到 3 个不同的公共频道。

5. `role_server` 把 RoleId、RoleName 和 ChannelIds 写入自己的进程字典，并返回 `1002`。

6. 登录失败时返回对应结果码，TCP 连接保留，客户端可以再次登录。

    

### 8\.4 频道操作和频道聊天

- 查询频道：`role_server` 读取 `channel_info`，结合自己的 `channel_ids` 返回全部 10 个频道及加入状态。

- 加入频道：`role_server` 取得 ChannelPid 并 `call channel_server`；成功后把 ChannelId 加入自己的 `channel_ids`。

- 退出频道：`role_server` 取得 ChannelPid 并 `call channel_server`；成功后从自己的 `channel_ids` 删除 ChannelId。`main` 不允许退出。

- 发送频道消息：`role_server` 先检查自己的 `channel_ids`，再 `call channel_server`。`channel_server` 二次检查成员身份，向本频道成员的 RolePid 发送 cast，并返回成功或错误。发送方收到 `2008`，频道成员收到 `2009`。

频道的加入、退出和消息发送都不经过 `channel_manager`。每个 `channel_server` 独立维护自己的成员 Map，因此不同频道可以分别处理消息。



### 8\.5 私聊

1. 发送方 `role_server` 收到 `3001`，按 `TargetRoleName` 读取 `online_roles`。

2. 找到目标 RolePid 后，向目标 `role_server` cast 结构化私聊消息。

3. 目标 `role_server` 编码并通过自己的 Socket 发送 `3003`。

4. 发送方 `role_server` 返回 `3002`；目标不在线时返回 `target_offline`。

    

### 8\.6 断开连接

Socket 关闭后，对应的 `role_server` 结束。`role_online_server` 收到监控 `DOWN` 后删除该玩家的在线记录；各个 `channel_server` 分别收到自己的监控 `DOWN`，从成员 Map 中删除该 RoleId。`channel_manager` 不参与成员清理。玩家重新登录时会再次加入 `main` 和随机公共频道。



## 9\. 编译、启动和验收

### 9\.1 编译和启动

```Plain Text
scripts/compile.sh      -> 编译 server/client，准备 ebin/chat.app
scripts/start_server.sh -> application:start(chat)
scripts/start_client.sh -> chat_client_sup:start_link()
```

服务端不会自动创建测试客户端。客户端 Shell 使用 `chat_client_manager:start_client/2` 创建一个或多个客户端。



### 9\.2 V1\.0 验收

1. 服务端正常启动 `role_online_server`、`channel_manager`、`main` 和 9 个公共频道进程，并监听 TCP 端口。

2. 新 RoleName 第一次登录时自动创建账号并取得 RoleId；已有账号使用原 RoleId 登录。

3. 密码错误返回 `invalid_login` 并允许重试；同一账号重复在线返回 `already_online`。

4. 登录成功后，角色一定加入 `main`，并随机加入 1 到 3 个不同的公共频道。

5. 客户端能查询全部频道，能加入和退出公共频道，但不能退出 `main`。

6. 已加入成员发送频道消息时收到 `2008` 成功结果，目标频道成员收到 `2009`，非成员收不到。

7. 未加入频道时发送消息返回 `not_joined`，发送者本人属于频道成员时也能收到频道推送。

8. 客户端能按 RoleName 私聊在线角色；目标收到 `3003`，发送方收到 `3002`。目标离线时返回 `target_offline`。

9. 客户端断线后，`online_roles` 在线记录和各频道中的成员记录都被清理，其他在线客户端不受影响。

    

### 9\.3 V1\.0 限制

- 不实现独立注册接口；新账号只在第一次登录时自动创建。

- 不实现密码加密、账号持久化、聊天记录和离线私聊。

- 不创建、删除或回收频道，不实现频道权限、邀请、踢人和群主。

- 不实现地图、移动、AOI、地图聊天和周围聊天。

- 不实现自动压力测试、心跳和空闲超时。

- 只要求基础业务跑通，不处理服务进程崩溃后的完整业务状态恢复。



后续版本再增加批量客户端、在线进程信息打印、地图和 AOI 等最终考核功能。

