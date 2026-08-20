# 分布式租约与 fencing token 服务 (Lease & Fencing Token Service)

## 业务问题

本项目实现一个"分布式租约与 fencing token 服务"：对命名资源颁发独占、带 TTL 的租约，每个租约携带一个全局单调递增、持久化的 fencing token，用于阻止过期持有者续约或释放已被他人重新持有的资源。租约记录与 fencing 计数器持久化到嵌入式 KV 数据库 bbolt，进程重启后从数据库恢复全部未过期租约与 fencing 计数器，并立即清扫停机期间已过期的租约。

核心特性：
- **独占租约**：同一资源在任一时刻最多存在一个活跃租约；活跃租约被再次颁发返回 409。
- **fencing token 全局单调递增并持久化**：每次新颁发分配一个严格大于所有历史 token 的新值；续约不改变 token；过期持有者（旧 token）对已被新 token 接管的资源续约/释放返回 409。
- **禁止续约/释放过期租约**：deadline 已过的租约视为死亡；续约返回 410、释放返回 410，调用方必须重新颁发获取新 token，不得沿用旧 token "复活"过期租约。
- **重启恢复与停机过期清扫**：进程重启从数据库恢复全部租约与 fencing 计数器（计数器不得归零），并在启动时立即清扫 deadline ≤ "重启时刻" 的租约；被清扫资源可立即重新颁发，新 token 严格递增。
- **原子事务**：fencing token 的分配与对应租约的写入在同一个 bbolt 事务内完成，进程在颁发过程中崩溃不会出现"分配了 token 但无对应租约"。
- **可注入时钟**：通过 `internal/clock` 抽象时间，真实服务用 wall-clock，自检与测试用 FakeClock 精确推进时间、确定性验证过期行为。

主要输入/输出：

| 接口 | 输入 | 输出 |
| --- | --- | --- |
| `POST /leases` | `{"resource","holder","ttl_seconds"}` | 颁发租约 `{"resource","holder","token","deadline_unix","acquired_unix","ttl_seconds"}`；活跃被占 409；字段非法 400 |
| `POST /leases/renew` | `{"resource","holder","token","ttl_seconds"}` | `{"resource","holder","token","deadline_unix"}`；无租约 404；holder/token 不符 409；已过期 410 |
| `POST /leases/release` | `{"resource","holder","token"}` | `{"resource","released":true}`；无租约 404；holder/token 不符 409；已过期 410 |
| `GET /leases?resource=X` | — | 租约详情（含 `active`）；无租约 404 |
| `GET /leases` | — | `{"leases":[...]}`（按 resource 字典序） |
| `POST /leases/expire` | — | `{"expired_count":N}` |
| `GET /healthz` | — | `200 {"status":"ok"}` |

## 本地命令

```bash
go build ./... # 编译
go run . --smoke-test    # 自检（不监听端口，成功退出码 0）
go test ./...   # 单元测试（clock / lease / store / main）
```

启动 HTTP 服务：`go run . --db ./leases.db`（默认监听 `:8080`，可用 `--addr :9090` 指定）。

## Docker

构建脚本 `build_benzhi_docker.sh` 接受两个参数：

1. `IMAGE_NAME`：镜像名（默认 `my-project`）。
2. `DOCKER_PLATFORM`：目标平台（默认 `linux/amd64`）。

构建 amd64 与 arm64 评测镜像：

```bash
bash ./build_benzhi_docker.sh go-task-benzhi:amd64 linux/amd64
bash ./build_benzhi_docker.sh go-task-benzhi:arm64 linux/arm64
```

进入容器交互式 shell：

```bash
docker run -it go-task-benzhi:amd64
```

双架构运行时镜像（交付用 `Dockerfile`）：

```bash
docker buildx build --platform linux/amd64 --load -t go-task-check:amd64 .
docker run --rm go-task-check:amd64 --smoke-test
docker buildx build --platform linux/arm64 --load -t go-task-check:arm64 .
docker run --rm go-task-check:arm64 --smoke-test
```

## 技术栈

- Go `1.26.3`，`GOTOOLCHAIN=local`，标准库 + `go.etcd.io/bbolt`（纯 Go 嵌入式 KV 数据库，`CGO_ENABLED=0`）。
- 持久化：bbolt 单文件嵌入式 KV 数据库，租约与 fencing 计数器全部存盘，重启从数据库恢复。
- 交付镜像 `CGO_ENABLED=0`，`linux/amd64` 与 `linux/arm64` 双架构。
