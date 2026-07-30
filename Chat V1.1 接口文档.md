# Chat V1.1 接口文档

## 1. 文档说明

本文档用于快速了解和验收 Chat V1.1 的外部接口。项目内部监督树、进程关系、ETS 结构和完整二进制报文格式见 [Chat V1.1 设计文档](<Chat V1.1 设计文档.md>)。

V1.1 提供以下功能：

- TCP 客户端连接与断开
- 账号首次登录自动创建、密码校验和重复在线检查
- 查询、加入和退出固定频道
- 频道聊天和私聊
- 角色下线后的在线记录与频道成员清理

## 2. 编译与启动

编译：

```bash
./scripts/compile.sh
```

启动服务端：

```bash
./scripts/start_server.sh
```

启动客户端 Shell：

```bash
./scripts/start_client.sh
```

默认监听地址为本机，端口为 `5555`。服务端和客户端 Shell 应在不同终端中运行。

## 3. 客户端生命周期接口

| 接口 | 返回值 | 说明 |
|---|---|---|
| `chat_client_manager:start_client(Host, Port)` | `{ok, ClientPid} \| {error, Reason}` | 创建一个拥有独立 TCP 连接的客户端进程 |
| `chat_client_manager:stop_client(ClientPid)` | `ok` | 向客户端发送停止消息 |
| `chat_client_manager:list_clients()` | `[ClientPid]` | 返回当前客户端监督树下的客户端进程 |

示例：

```erlang
{ok, Client} =
    chat_client_manager:start_client("127.0.0.1", 5555).

chat_client_manager:list_clients().
chat_client_manager:stop_client(Client).
```

## 4. 客户端业务接口

每个 `chat_client` 都是一个独立的行为进程。调用方只负责发送普通 Erlang 消息，登录、发包、收包和状态更新均由客户端自身完成。

| 普通消息 | 含义 |
|---|---|
| `{login, RoleName, Password}` | 登录或首次创建账号 |
| `list_channels` | 查询全部频道及加入状态 |
| `{join_channel, ChannelId}` | 加入公共频道 |
| `{leave_channel, ChannelId}` | 退出公共频道 |
| `{send_channel, ChannelId, Content}` | 向已加入频道发送消息 |
| `{send_private, TargetRoleName, Content}` | 按角色名发送私聊 |
| `stop` | 停止客户端并关闭 Socket |

直接发送消息：

```erlang
Client ! {login, <<"alice">>, <<"123456">>}.
Client ! list_channels.
Client ! {join_channel, 10}.
Client ! {send_channel, 10, <<"hello">>}.
Client ! {send_private, <<"bob">>, <<"hello">>}.
Client ! {leave_channel, 10}.
Client ! stop.
```

## 5. 异步结果约定

`ClientPid ! Message` 会立即返回被发送的消息，只表示消息已经发往客户端进程，不表示 TCP 发送或服务端业务处理成功。

服务端响应稍后由 `chat_client` 接收。客户端会更新连接、角色和频道状态，并在 Shell 中打印结果。客户端不保存最近一次业务结果，也不使用 `gen_server:call/3`、`From` 或 `gen_server:reply/2` 向原发送者同步返回结果。

常见结果格式：

| 操作 | 成功结果 | 失败结果示例 |
|---|---|---|
| 登录 | `{ok, RoleId, ChannelIds}` | `{error, invalid_login}`、`{error, already_online}` |
| 查询频道 | `{ok, Channels}` | `{error, Reason}` |
| 加入频道 | `{ok, ChannelId}` | `{error, already_joined, ChannelId}` |
| 退出频道 | `{ok, ChannelId}` | `{error, not_joined, ChannelId}`、`{error, cannot_leave_main, 1}` |
| 频道发送 | `{ok, ChannelId}` | `{error, not_joined, ChannelId}` |
| 私聊发送 | `{ok, TargetRoleName}` | `{error, target_offline, TargetRoleName}` |

打印示例：

```text
[login] {ok,1,[1,3,2]}
[join_channel] {ok,10}
[join_channel] {error,already_joined,10}
[send_private] {error,target_offline,<<"nobody">>}
```

频道和私聊推送格式：

```text
[channel 10] alice(1): hello
[private] alice(1): hello
```

验收或调试时可以读取客户端状态：

```erlang
sys:get_state(Client).
```

`sys:get_state/1` 仅作为 OTP 调试手段，不属于项目业务接口。

## 6. TCP 协议索引

Socket 使用 `binary`、`{packet, 4}` 和 `{active, once}`。`PacketLength` 由 `gen_tcp` 自动处理，业务层报文从 16 位 `ProtoId` 开始。

| 请求 | 结果或推送 | 功能 |
|---|---|---|
| `1001` | `1002` | 登录 |
| `2001` | `2002` | 查询频道 |
| `2003` | `2004` | 加入频道 |
| `2005` | `2006` | 退出频道 |
| `2007` | `2008`、`2009` | 发送结果、频道消息推送 |
| `3001` | `3002`、`3003` | 私聊结果、私聊消息推送 |
| - | `9001` | 未登录、非法报文或未知协议 |

完整字段和结果码定义见设计文档第 7 章。

## 7. 最短验收示例

在客户端 Shell 中创建两个客户端：

```erlang
{ok, Alice} =
    chat_client_manager:start_client("127.0.0.1", 5555).
{ok, Bob} =
    chat_client_manager:start_client("127.0.0.1", 5555).

Alice ! {login, <<"alice">>, <<"123456">>}.
Bob ! {login, <<"bob">>, <<"123456">>}.

Alice ! {join_channel, 10}.
Bob ! {join_channel, 10}.
Alice ! {send_channel, 10, <<"hello channel">>}.
Alice ! {send_private, <<"bob">>, <<"hello bob">>}.
Alice ! {leave_channel, 10}.
Alice ! {send_channel, 10, <<"should fail">>}.
Alice ! stop.
```

验收时确认：

1. 两个客户端均能异步登录成功。
2. 登录后必定加入 `main`，并随机加入 1 到 3 个公共频道。
3. Alice 和 Bob 加入频道 10 后都能收到频道消息，发送者本人也能收到。
4. Bob 能收到 Alice 的私聊推送。
5. Alice 退出频道 10 后再次发送会得到 `not_joined`。
6. Alice 断开后，服务端会清理其在线记录和频道成员记录，Bob 不受影响。

## 8. V1.1 边界

- 不提供独立注册接口，首次登录自动创建账号。
- 账号、频道和在线数据不持久化，服务端重启后重新初始化。
- 不保存聊天记录，不支持离线私聊。
- 客户端断开后不自动重连。
- 不包含心跳、空闲超时和自动压力测试。
- TCP 协议不包含 `RequestId`，不提供逐请求同步返回或回调关联。
