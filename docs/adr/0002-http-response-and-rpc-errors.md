# ADR-0002：统一 HTTP 响应与内部 RPC 业务错误

- 状态：已接受
- 日期：2026-08-22

## 背景

DogX 的 API Handler 由 goctl 生成，RPC 使用 gRPC。普通 Go 错误跨越 gRPC 后会丢失自定义类型；标准 gRPC Code 只有固定语义，无法直接表达 DogX 的数字业务码。同时，浏览器需要稳定的 `code/message/data` JSON 响应。

DogX 当前的 gRPC 只作为内部协议，服务端和客户端均由项目控制；对外协议为 HTTP JSON。

## 决策

### HTTP 输出

保留 goctl 默认 Handler，不维护自定义 Handler 模板：

```text
成功 → httpx.OkJsonCtx → 全局 OkHandler
错误 → httpx.ErrorCtx  → 全局 ErrorHandlerCtx
```

成功响应：

```json
{
  "code": 0,
  "message": "success",
  "data": {}
}
```

业务错误返回 HTTP 200：

```json
{
  "code": 100001,
  "message": "用户名已经存在",
  "data": null
}
```

参数和技术错误使用真实 HTTP 4xx/5xx，响应体中的 `code` 等于 HTTP 状态码；内部错误消息必须脱敏。

### 业务错误码

- `0` 表示成功。
- `100001` 是默认业务错误码。
- 不预先规划大量细分错误码；只有客户端确实需要分支处理时才新增具体业务码。
- 业务代码通过 `bizerror.New(message)` 或 `bizerror.NewCode(code, message)` 返回标准 Go `error`。

### 内部 gRPC 传输

RPC 服务端拦截器只识别 `bizerror.Error`，并转换为：

```go
status.Error(codes.Code(businessCode), businessMessage)
```

数值范围约定为：

```text
标准 gRPC / 框架错误：0～16
DogX 业务错误：       100000～2147483647
```

API ErrorHandler 先判断 gRPC Code 是否位于 DogX 业务范围；命中时恢复为 HTTP 200 业务响应，否则按标准 gRPC Code 映射真实 HTTP 状态。

这是有意采用的内部协议扩展，不符合标准 gRPC Code 的可移植性要求。它不得用于对外 gRPC API；若未来开放 gRPC、接入其他语言客户端或不受控代理，必须改用标准 gRPC Code 加 Status Details 等兼容方案。

## 后果

- 业务 Logic 保持 `return nil, err` 的 Go 错误处理习惯。
- API 不需要解析 protobuf Status Details，业务错误判断简单明确。
- go-zero 和 grpc-go 升级时必须运行真实 gRPC 往返测试，确认扩展 Code 仍能原样传输。
- 普通数据库错误、panic、超时、熔断、过载和鉴权失败不能转换为业务码，继续使用标准技术错误。
