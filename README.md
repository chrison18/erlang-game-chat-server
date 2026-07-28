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

当前已经包含根监督树、账号与在线角色 ETS、频道资料 ETS，以及 10 个固定频道进程。
