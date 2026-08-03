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

客户端 Shell 通过 `client` 模块按 ClientId 操作客户端。每个客户端创建后使用
`client_N` 作为账号名、`123456` 作为密码自动登录。登录成功后，每个客户端
每隔 1000ms 自动向 `main` 频道发送一条消息。

```erlang
{ok, 2} = client:start_client(1, 2).
ok = client:send_channel(1, 1, <<"hello">>).
ok = client:send_private(1, 2, <<"hello">>).
```
