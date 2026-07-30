# chat

一个使用 Erlang/OTP 和 `gen_tcp` 实现的单节点 TCP 聊天项目。

## 编译

```bash
./scripts/compile.sh
```

## 启动服务端

```bash
./scripts/start_server.sh
```

当前已经包含根监督树、账号与在线角色 ETS、10 个固定频道进程，以及基础 TCP 连接生命周期。

客户端 Shell 通过 `chat_client_manager:start_client/2` 创建客户端。Shell 只向
客户端发送行为消息，登录、频道操作、TCP 响应和状态更新都由客户端进程处理。

```erlang
{ok, Client} = chat_client_manager:start_client("127.0.0.1", 5555).
Client ! {login, <<"alice">>, <<"secret">>}.
Client ! list_channels.
Client ! {join_channel, 2}.
Client ! {send_channel, 2, <<"hello">>}.
Client ! {send_private, <<"bob">>, <<"hello">>}.
Client ! {leave_channel, 2}.
```
