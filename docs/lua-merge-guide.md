# Lua 合并脚本开发指南

Sink 的 Merge 操作用 Lua 描述单条记录的读、改、写规则。业务服务提交
`incoming` 文档和 Lua 程序，Sink 读取当前文档、执行程序，并通过存储修订版本保证
单条记录更新的原子性。

## 最小脚本

每个程序必须返回一个接收两个参数的函数：

```lua
return function(current, incoming)
    current = current or json.object()
    current.stock = incoming.stock
    return current
end
```

- `current` 是存储中的当前 JSON 对象。记录不存在且 Merge 使用
  `MISSING_DOCUMENT_MODE_CREATE` 时，它是 `nil`。
- `incoming` 是本次 Merge 携带的 JSON 对象，始终存在。
- 返回值必须是 JSON 对象，不能返回 `nil`、数组或标量。
- Sink 发生修订冲突时会读取最新的 `current` 并重新执行同一个函数，所以脚本必须是
  确定性的。

Lua 函数没有第三个 `context` 参数。需要执行时间时使用
`sink.v1.time.now()`。

## 完整示例

下面的脚本更新非空字段、合并标签、保留最近 20 条历史记录，并记录本次 Merge
时间：

```lua
local array = sink.v1.array
local object = sink.v1.object

return function(current, incoming)
    current = current or json.object()

    object.replace_nonempty_string(current, incoming, "title")
    object.replace_nonempty_array(current, incoming, "images")
    current.tags = array.union_strings(current.tags, incoming.tags)

    local history = current.history or json.array()
    array.append_all(history, incoming.history)
    history = array.deduplicate(history, function(item)
        return item.id
    end)
    current.history = array.keep_tail(history, 20)
    current.updated_at = sink.v1.time.now()
    return current
end
```

建议在程序顶部为常用子模块创建局部变量。这样既缩短源码，也减少每次查找全局表的
开销。

## `sink.v1` 公共工具

`sink.v1` 是 Sink 提供的版本化、确定性工具 API。不存在的参数、额外参数、错误类型、
不连续数组、非法回调返回值和越界限制都会让当前 Merge 明确失败，不会静默修改数据。

### 数组工具

所有数组参数必须是 JSON 数组，也就是来自 `current`、`incoming`，或者通过
`json.array()` 创建的数组。不要用空的 `{}` 代替数组。

#### `sink.v1.array.append_all(target, source)`

- `target`：必填 JSON 数组。
- `source`：JSON 数组或 `nil`。
- 按原顺序把 `source` 的所有元素追加到 `target`。
- 会修改 `target`，并返回同一个 `target`；不会修改 `source`。
- `source` 为 `nil` 时不做任何修改。

#### `sink.v1.array.deduplicate(items, key_function)`

- `items`：必填 JSON 数组。
- `key_function(item)`：必填函数，必须恰好返回一个字符串、数字或布尔值。
- 保留每个键第一次出现的元素及原顺序，返回新的 JSON 数组。
- 不修改 `items`。回调错误会让 Merge 失败。
- 键不能是 `nil`、表或函数。需要对对象生成复合键时，应在回调中返回稳定字符串。

```lua
local unique = sink.v1.array.deduplicate(incoming.offers, function(item)
    return (item.platform or "") .. ":" .. (item.id or "")
end)
```

#### `sink.v1.array.keep_tail(items, limit)`

- `items`：必填 JSON 数组。
- `limit`：大于或等于 `0` 的整数。
- 返回一个新的 JSON 数组，只保留最后 `limit` 个元素。
- `limit` 为 `0` 时返回空数组；不会修改 `items`。

#### `sink.v1.array.union_strings(left, right)`

- `left`、`right`：字符串 JSON 数组或 `nil`。
- 按 `left` 后 `right` 的顺序合并并去重，返回新的 JSON 数组。
- 不修改任何输入。任一数组包含非字符串元素时 Merge 会失败。

### 对象工具

对象工具的 `target` 和 `source` 必须是 JSON 对象。它们适合批量处理字段列表：

```lua
local fields = {"title", "description", "country"}
for index = 1, #fields do
    sink.v1.object.replace_nonempty_string(current, incoming, fields[index])
end
```

#### `sink.v1.object.replace_nonempty_string(target, source, field)`

- `field` 必须是字符串。
- `source[field]` 不存在或是空字符串时不修改 `target`。
- 非空字符串会写入 `target[field]`。
- `source[field]` 是其他类型时 Merge 失败，避免把错误类型写入已有字段。
- 无返回值。

#### `sink.v1.object.replace_nonempty_array(target, source, field)`

- `field` 必须是字符串。
- `source[field]` 不存在或是空数组时不修改 `target`。
- 非空 JSON 数组会写入 `target[field]`。
- `source[field]` 是其他类型或不连续数组时 Merge 失败。
- 无返回值。

### 时间工具

#### `sink.v1.time.now()`

- 不接收参数。
- 返回 UTC RFC3339Nano 字符串，例如 `2026-08-30T09:08:07.654321Z`。
- 返回的是本次 Merge 的固定观察时间。同一脚本内多次调用，以及同一操作因修订冲突
  重新执行时，结果都相同。
- 同步 Merge 使用服务端开始处理该操作的时间；异步 Merge 使用 Worker 开始处理 Kafka
  记录的时间，不是客户端提交时间。
- 将返回值写入结果文档时，Sink 会保留日期时间元数据，使 MongoDB 可以存储为 BSON
  datetime。

Sink 不开放 `os.time`、宿主时钟或可变时区，以免重试产生不同结果。

## JSON 与 Lua 类型

输入和输出遵循以下映射：

| JSON | Lua |
| --- | --- |
| object | table |
| array | table，带 JSON 数组标记 |
| string | string |
| integer | Lua 64 位 integer |
| decimal | number |
| boolean | boolean |
| null | `json.null` |

公共 JSON 工具：

- `json.object()` 创建明确的空 JSON 对象。
- `json.array()` 创建明确的空 JSON 数组。
- `json.null` 表示 JSON null。Lua 的 `nil` 会删除表字段。
- `json.is_null(value)` 判断是否为 `json.null`。

空的 Lua 表 `{}` 会编码为对象。需要空数组时必须使用 `json.array()`。

## 可用和禁用的 Lua 能力

Sink 提供确定性的 base、string、table、math 和 UTF-8 功能。
`utf8.upper(value)` 支持 Unicode 大写转换；`string.upper` 只适合 ASCII。

以下能力不可用：宿主文件与网络 I/O、操作系统 API、`require`/package、动态代码加载、
协程、debug API、随机数、输出、修改 metatable，以及无界字符串重复。业务脚本不能访问
Sink 配置、环境变量、凭据或其他租户数据。

## 重试、异步和幂等性

- 同一记录的并发 Merge 通过修订条件检测冲突，并以最新文档重新执行。
- Kafka 是至少一次投递。Worker 在写入成功但提交 offset 前崩溃时，同一 Merge 可能再次
  执行。
- 去重键、追加逻辑和计数逻辑必须考虑重复执行。仅执行 `current.count = current.count + 1`
  不是天然幂等的。
- 不要依赖表遍历以外的隐式顺序、随机数、外部状态或每次调用都变化的时间。
- `sink.v1.time.now()` 只保证一次 Merge 操作及其修订冲突重试内稳定；Kafka 记录被重新
  消费属于新的执行，业务仍需设计幂等规则。

## 资源限制与错误

每次执行受配置的墙钟时间、指令数、调用深度、VM 栈、脚本源码大小和结果大小限制。
`sink.v1` 的原生数组循环也检查执行超时和工作量上限。脚本语法、参数、类型、回调、
资源或返回值错误只会让对应 Write operation 失败，并返回结构化 failure。

业务上线前至少应覆盖：

1. 记录存在和不存在两种输入。
2. 字段缺失、空字符串、空数组和 `json.null`。
3. 重复元素、历史上限和非法字段类型。
4. 使用同一 incoming 重放两次后的结果。
5. 并发更新造成修订冲突时的最终结果。
6. 同步模式和 Kafka 异步模式的真实 Sink 集成测试。

## 在 Go 客户端中使用

脚本应跟随业务代码版本管理，并在进程内创建一次 `LuaProgram` 后复用：

```go
source := []byte(`
return function(current, incoming)
    current = current or json.object()
    current.stock = incoming.stock
    current.updated_at = sink.v1.time.now()
    return current
end`)

program, err := sink.NewLuaProgram(source)
if err != nil {
    return err
}
options := sink.MergeOptions{
    Incoming:            incoming,
    Program:             program,
    MissingDocumentMode: sink.MissingDocumentCreate,
}
operation, err := sink.NewMerge(address, options)
if err != nil {
    return err
}
```

同一个 Write RPC 中相同源码只声明一次，Merge operation 使用 SHA-256 引用。异步 Kafka
记录仍携带完整业务源码以支持独立处理；公共工具函数由 Sink 内建，因此不会随每个请求
重复传输。

## API 版本

脚本必须显式使用 `sink.v1`。v1 内函数的名称和语义保持稳定；未来不兼容调整将使用新的
版本命名空间。不要检测或调用未记录的全局变量、内部字段或更高版本 API。
