# ABAP 转 SQL 规则说明

本文档说明当前 ABAP 转 SQL 功能已经固化的受控规则。目标不是覆盖所有 ABAP 语法，而是先把高频且已验证正确的逻辑固定下来，后续按样例逐步补充。

## 当前范围

- `result_package`、`source_package` 只作为解析阶段的容器语义；最终 SQL 输出时统一直接替换为用户输入的生成表名，不再保留 `AS result_package` 这类别名。
- `<result_fields>`、`<source_fields>` 在最终 SQL 中也统一展开成生成表名下的字段引用。
- 最终 SQL 中的输出字段名默认保留 ABAP 原始写法；像 `/bic/cplant` 这类带斜杠字段会原样保留，不再改写成 `BIC_CPLANT` 之类的规范化名称。
- `CALL METHOD` 会递归追溯到 `PROG_CODE` 中的方法源码，使用 `EXPORTING` 作为输入条件，使用 `IMPORTING` 作为返回字段。
- `CALL FUNCTION` 与 `CALL METHOD` 使用相同处理原则。
- 如果 `CALL METHOD` 或 `CALL FUNCTION` 同时返回业务字段和调试变量，例如 `lv_lookup_successful_flag`，伪 SQL 也要把这些返回结果一并保留下来。
- 已稳定的“填充字段选择内容”逻辑保持不变。
- 除上述范围外，其余通用赋值翻译暂不扩展，避免结果失控。

## 选择条件翻译

当 ABAP 代码构造范围表或选择条件时，当前固定翻译规则如下。

| SIGN | OPTION | SQL 语义 |
| --- | --- | --- |
| `I` | `EQ` | `IN` |
| `I` | `BT` | `BETWEEN` |
| `I` | `CP` | `LIKE` |
| `E` | `EQ` | `NOT IN` |
| `E` | `BT` | `NOT BETWEEN` |
| `E` | `CP` | `NOT LIKE` |

## 当前输出示例

### EQ

```sql
LOGSYS IN ( SELECT DISTINCT ls_result.low FROM rsdioselopt WHERE iobjnm = '0LOGSYS' );
```

### CP

```sql
LOGSYS LIKE ( SELECT DISTINCT REPLACE(ls_result.low, '*', '%') FROM rsdioselopt WHERE iobjnm = '0LOGSYS' );
```

### BT

```sql
CALDAY BETWEEN ( SELECT DISTINCT ls_result.low, ls_result.high FROM rsdioselopt WHERE iobjnm = '0CALDAY' );
```

### 取反条件

```sql
LOGSYS NOT IN ( SELECT DISTINCT ls_result.low FROM rsdioselopt WHERE iobjnm = '0LOGSYS' );
LOGSYS NOT LIKE ( SELECT DISTINCT REPLACE(ls_result.low, '*', '%') FROM rsdioselopt WHERE iobjnm = '0LOGSYS' );
CALDAY NOT BETWEEN ( SELECT DISTINCT ls_result.low, ls_result.high FROM rsdioselopt WHERE iobjnm = '0CALDAY' );
```

## 后续补充方式

- 如果新增规则仍属于选择条件翻译，优先追加到本文件的“选择条件翻译”部分。
- 如果新增规则属于 `CALL METHOD` 或 `CALL FUNCTION` 的字段回写语义，应补充到“当前范围”中，并附一个最小样例。
- 如果某条规则尚未稳定，不应直接放进这里，而应先用真实样例验证后再固化。