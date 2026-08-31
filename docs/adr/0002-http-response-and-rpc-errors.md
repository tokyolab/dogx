# ADR-0002：统一 HTTP 响应、参数校验与内部 RPC 业务错误

- 状态：已接受
- 日期：2026-08-22
- 最后更新：2026-08-31

## 背景

DogX 的 API Handler 由 goctl 生成，RPC 使用 gRPC。普通 Go 错误跨越 gRPC 后会丢失自定义类型；标准 gRPC Code 只有固定语义，无法同时表达 DogX 的数字业务码、稳定机器标识和可读诊断信息。浏览器还需要稳定且支持国际化的 `code/subcode/message/data` JSON 响应。

DogX 当前的 gRPC 只作为内部协议，服务端和客户端均由项目控制；对外协议为 HTTP JSON。

## 决策

### HTTP 输出

保留 goctl 默认 Handler，不维护自定义 Handler 模板：

```text
成功 → httpx.OkJsonCtx → 全局 OkHandler
错误 → httpx.ErrorCtx  → 全局 ErrorHandlerCtx
```

所有 HTTP 响应使用相同结构：

```json
{
  "code": 0,
  "subcode": "",
  "message": "成功",
  "data": {}
}
```

字段职责：

- `code` 是粗粒度数字分类：成功为 `0`，默认业务错误为 `100001`，参数和技术错误等于 HTTP 状态码。
- `subcode` 是稳定机器标识，供测试、客户端分支和国际化使用；成功时为空字符串。
- `message` 是当前语言下的用户展示文案，允许调整，不作为程序判断依据。
- `data` 是业务数据，错误时为 `null`。

业务错误仍返回 HTTP 200：

```json
{
  "code": 100001,
  "subcode": "system.role.code_exists",
  "message": "角色编码已存在",
  "data": null
}
```

参数和技术错误使用真实 HTTP 4xx/5xx。底层数据库错误、RPC 原始诊断和其他内部信息不得直接返回客户端。

### subcode

业务 subcode 使用以下格式：

```text
<稳定业务域>.<资源或子域>.<错误原因>
```

例如：

```text
system.role.not_found
system.role.code_exists
system.auth.invalid_credentials
common.invalid_request
common.permission_denied
```

第一段按业务域划分，不按 API、RPC、MQ 或 Job 等部署进程划分。参数解析、字段校验和 RPC 参数兜底统一使用 `common.invalid_request`，不为 required、max、格式等每条字段规则创建 subcode。需要精确定位校验规则的测试直接断言 Validator 返回的字段和规则。

通用 subcode 放在 `pkg/subcode`；业务 subcode 放在 `apps/<domain>/internal/subcode` 并按资源拆分文件。不建立集中且巨大的业务错误码文件，也不为错误处理新增 Domain 业务层。

### API 参数校验

API 层使用 `go-playground/validator` 检查必填、长度、范围和枚举等通用字段边界。校验标签只写在 `.api` 契约源文件中，由 goctl 生成到 Types：

```go
Code string `json:"code" validate:"required,max=64"`
```

API 启动时通过 go-zero v1.10.3 的 `httpx.SetValidator` 注册全局 Validator。goctl 生成 Handler 已调用 `httpx.Parse`，因此不得为接入校验而修改生成 Handler 或维护自定义 Handler 模板。

只允许使用 `go-playground/validator` 官方内置 Tag，不注册 `min_bytes`、`max_bytes`、通用正则等项目自定义 Tag。字符串 `min/max/len` 与 PostgreSQL `VARCHAR(n)` 均按 Unicode 字符数理解。角色编码等无法由官方 Tag 完整表达的业务格式由前端提供即时提示，并由 RPC 手写校验和 PostgreSQL `CHECK` 约束保证安全边界。

字段校验失败统一使用 `common.invalid_request`，同时保留 Validator 原始详细诊断，便于直接调试接口：

```json
{
  "code": 400,
  "subcode": "common.invalid_request",
  "message": "Key: 'CreateRoleReq.Code' Error:Field validation for 'Code' failed on the 'required' tag",
  "data": null
}
```

Validator 诊断不参与后端国际化，也不作为程序判断依据。面向最终用户的友好字段提示由前端表单负责。

JSON 损坏、字段类型不匹配等无法解析的请求同样返回 `common.invalid_request`，message 使用通用的请求参数错误文案；详细解析错误只写服务端日志，不记录完整请求体，也不直接暴露给客户端。

### RPC 参数兜底

内部调用可以绕过 HTTP API，因此 RPC 必须继续检查影响数据完整性和资源安全的规则，例如 ID、状态枚举、字段边界、编码格式和分页上限。RPC 参数错误使用标准 gRPC Code，并保留可读的原始诊断：

```go
status.Error(codes.InvalidArgument, "role code has an invalid format")
```

该 message 供内部调用日志和排障使用。API 不把它直接暴露给浏览器，而是映射为真实 HTTP 400、`common.invalid_request` 和当前语言的通用参数错误文案。

### 业务错误码与内部 gRPC 传输

- `0` 表示成功。
- `100001` 是默认业务错误码。
- 不预先规划大量细分数字错误码；只有客户端确实需要按粗粒度类型分支时才新增具体数字码。
- 内部 RPC 使用 `100000～2147483647` 的扩展 gRPC Code 传递 DogX 业务错误；标准 gRPC Code `0～16` 留给框架和技术错误。

业务代码返回包含 `code/subcode/message` 的 `bizerror.Error`。其中 message 必须保留可直接阅读的内部诊断语义，不能退化成翻译键：

```go
bizerror.New(subcode.RoleCodeExists, "角色编码已存在")
```

RPC 服务端拦截器转换为：

```text
gRPC Code                    = 100001
gRPC Message                 = 角色编码已存在
google.rpc.ErrorInfo.Reason  = system.role.code_exists
```

`ErrorInfo` 是 Google 提供的标准 gRPC Status Detail。DogX 只使用 `Reason` 承载 subcode，不设置 `Domain`，也不自定义 protobuf。API 仅在 gRPC Code 位于 DogX 业务错误范围时读取 `ErrorInfo.Reason`；缺失或非法时记录协议错误并降级为通用业务错误。

go-zero v1.10.3 自身不使用 `ErrorInfo` 传递框架错误。标准 gRPC 错误根据 Code 映射真实 HTTP 状态，不读取业务 Detail。

扩展 gRPC Code 是 DogX 内部协议，不得直接用于对外 gRPC API。若未来开放 gRPC、接入不受控代理或第三方客户端，必须重新评估并迁移到完全标准的 gRPC Code 分类。

### 国际化

API 根据 `Accept-Language` 返回用户文案，当前支持 `zh-CN` 和 `en-US`，缺失或无法匹配时默认使用 `zh-CN`。客户端不得根据 message 做业务判断。

后端翻译资源保存在两个随二进制嵌入的 JSON 文件中：

```text
pkg/i18n/locales/zh-CN.json
pkg/i18n/locales/en-US.json
```

翻译资源不放入数据库，不引入缓存和多实例同步。后端只翻译用户可见的业务错误以及通用认证、权限和服务错误；Validator 原始诊断、内部日志、数据库错误、CLI/迁移错误和数据库中的角色名、菜单名、API 描述等业务数据不翻译。

请求语言缺少某个翻译时先回退到中文；中文仍缺失时返回当前语言的通用错误并记录日志，不向客户端暴露翻译键或内部诊断。

## 测试约定

- RPC Logic 测试断言业务 Code 和 subcode，不断言可调整的展示文案。
- HTTP 协议测试断言 HTTP 状态、code 和 subcode；普通业务测试不锁定 message。
- Validator 测试直接断言官方返回的字段名和校验规则，例如 `field=Code`、`tag=max`，不锁定完整诊断文案。
- 国际化专项测试验证中英文键完整、翻译结果非空、缺失键安全降级；只有该层可以验证具体翻译行为。
- 真实 gRPC 往返测试必须验证扩展 Code、原始 Message 和 `ErrorInfo.Reason` 均能无损传输。

## 后果

- 业务 Logic 保持 `return nil, err` 的 Go 错误处理习惯。
- RPC 原始 message 可直接用于内部排障，HTTP message 可独立国际化。
- subcode 为测试和客户端提供稳定语义，修改展示文案不再破坏业务测试。
- API Validator 提供具体字段反馈，RPC 校验继续保护内部调用边界。
- 不修改任何 `DO NOT EDIT` 生成文件，也不需要修改 RPC protobuf 契约。
