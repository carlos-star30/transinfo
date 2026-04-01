# 路径逻辑与表关联说明

本文档用于后续排查主页 Path Search、Aligned Mapping、字段文本、逻辑程序与导出模板相关问题。内容只覆盖当前项目已经落地的真实逻辑。

## 1. 运行链路总览

主页路径分析功能分成四段：

1. 前端调用 /api/path-selection/search，先找候选路径。
2. 用户选中路径后，前端调用 /api/path-selection/mapping，拉取分段字段映射。
3. 需要字段文本时，再补调 /api/path-selection/text。
4. 需要公式、Routine、Constant 时，再补调 /api/path-selection/logic。

当前前后端交互字段已经统一为数据库技术名风格的大写字段，例如 SOURCE、SOURCESYS、TARGETNAME、TRANIDS、ROWS、STEP_LOGIC。

## 2. 核心接口与输入输出

### 2.1 /api/path-selection/search

输入关键字段：

- SOURCE
- SOURCESYS
- TARGETNAME
- TRANID
- WAYPOINTS

两种入口模式：

1. 直接给 TRANID，后端按单条转换生成一条路径。
2. 给 SOURCE + SOURCESYS + TARGETNAME，后端基于 RSTRAN 图搜索候选路径。

输出关键字段：

- CANDIDATE_PATHS
- CANDIDATE_COUNT
- RESOLVED_START_NAME
- SEARCH_STATS

每条路径内的重要字段：

- SEGMENT_COUNT
- NODE_SEQUENCE
- SEGMENTS

每个 SEGMENT 当前包含：

- SOURCE
- TARGETNAME
- SOURCETYPE
- TARGETTYPE
- SOURCESYS
- TARGETSYSTEM
- TRANIDS
- LOGIC_IDS
- HAS_LOGIC

## 3. 路径搜索的真实逻辑

后端主函数在 backend/import_status_api.py 中的 search_candidate_paths。

当前做法：

1. 从 RSTRAN 读取激活版本记录。
2. 基于 SOURCE 或 DATASOURCE 字段作为起点对象。
3. 对 RSDS 类型对象，节点键会把对象名和系统一起编码，避免同名不同系统串线。
4. 以对象图方式做 BFS，产出候选路径。

重要约束：

1. RSDS 必须结合 SOURCESYS 才能唯一定位。
2. 节点唯一键不是单纯对象名，而是 make_path_node_key 生成的对象键。
3. 之前曾有一个过严的 TRANID 预过滤，会把合法中间路径剪掉，导致本应 6 条路径只剩 1 条。该逻辑已经移除，后续不要再按单个 TRANID 先裁剪整张图。

## 4. Mapping、Text、Logic 的拼装关系

后端主函数：

- build_path_mapping_payload
- build_path_text_payload
- build_path_logic_payload

其中 mapping 是主载体，text 和 logic 本质上都是补丁式增强：

1. mapping 先返回每个 segment 的 ROWS。
2. text 再把 SOURCE_TEXT、TARGET_TEXT 合并回同一批行。
3. logic 再把 LOGIC_ENTRIES、HAS_LOGIC_ENTRY、STEP_LOGIC 合并回结果。

前端对应的合并入口在 frontend-prototype/app.js：

- normalizePathMappingPayload
- mergePathTextPayload
- mergePathLogicPayload

注意：

1. 规则级逻辑匹配键必须是 TRANID + RULEID + STEPID。
2. 这是已经验证过的约束，不能只按 TRANID 或 RULEID 匹配，否则会串错程序内容。

## 5. 关键业务表与用途

### 5.1 RSTRAN

用途：转换主表，也是路径图的边来源。

重点字段：

- TRANID
- SOURCE 或 DATASOURCE
- SOURCESYS
- TARGETNAME
- SOURCETYPE
- TARGETTYPE
- STARTROUTINE
- ENDROUTINE
- EXPERT
- OBJVERS

当前主页路径搜索、转换元数据、TRANID 反查，都会优先依赖这张表。

### 5.2 rstran_mapping_rule_full

用途：字段映射明细主来源。

重点字段：

- tran_id
- rule_id
- step_id
- seg_id
- source_field
- target_field
- rule / rule_type
- aggr
- source_object / target_object
- source_system / target_system

Aligned Mapping 网格的主体行数据来自这里。

### 5.3 rsdssegfd

用途：RSDS 数据源字段清单与字段元数据。

当前用于：

- 字段是否为 Key
- DATATYPE
- LENG
- DECIMALS

### 5.4 rsksfieldnew

用途：TRCS 类对象字段清单与字段元数据。

当前用于：

- 字段清单
- KEYFLAG
- DATATYPE
- LENG
- DECIMALS

### 5.5 dd03l

用途：ADSO、ODSO、IOBJ 相关底表字段结构来源。

当前用于：

- FIELDNAME
- KEYFLAG
- DATATYPE
- LENG
- DECIMALS

说明：ADSO/ODSO/IOBJ 的字段元数据最终很多都从 dd03l 派生。

### 5.6 rsoadso / rsoadsot

用途：ADSO 对象信息与文本信息。

当前用于：

- 对象文本名
- 激活表后缀
- 导出时展示真实 A 表技术名

### 5.7 bw_object_name

用途：对象英文名映射。

当前用于路径候选里 NODE_SEQUENCE 的 OBJECT_NAME，以及网格头部展示的红字业务名称。

## 6. 字段元数据是如何补进 Mapping 的

当前后端会在构建 mapping rows 后，按 source/target 所属对象类型选择不同库存：

1. RSDS 走 rsdssegfd。
2. TRCS 走 rsksfieldnew。
3. ADSO/ODSO/IOBJ 走 dd03l 派生库存。

统一补出的字段有：

- SOURCE_DATATYPE
- SOURCE_LENGTH
- SOURCE_DECIMALS
- TARGET_DATATYPE
- TARGET_LENGTH
- TARGET_DECIMALS

前端页面与导出模板都直接消费这些字段。

## 7. Diagnostics 的意义

每个 segment 都会附带 DIAGNOSTICS：

- DIAGNOSTICS.SOURCE
- DIAGNOSTICS.TARGET

关键字段包括：

- UNIQUE_FIELD_COUNT
- COMPARISON_AVAILABLE
- INVENTORY_FIELD_COUNT
- DIFFERENCE_COUNT
- MISSING_COUNT
- EXTRA_COUNT
- HAS_DIFFERENCE
- INVENTORY_ORIGIN
- INVENTORY_LABEL
- ADSO_TABLE_NAME

这个结构用于：

1. 页面上显示字段差异是否已校验。
2. 导出时拿 ADSO 的真实 A 表表名。

已踩过的坑：

后端把 diagnostics 改成大写字段后，前端一度仍按小写读取，导致页面上全部显示“未校验”。现在前端已通过 getPathDiagnosticsBucket 统一兼容大小写。

## 8. 前端 Aligned Mapping 的关键规则

前端不是简单按 segment 逐行平铺，而是做了按上下游字段的对齐：

1. 以最后一个 segment 的 rows 作为初始骨架。
2. 向前逐段用 source_field / target_field 做关联对齐。
3. 生成多段横向对齐后的 alignedRows。

因此这几个字段非常关键：

- SOURCE_FIELD
- TARGET_FIELD
- RULEPOSIT
- ROW_KIND

如果后端改动了这些字段名或含义，最容易引发“路径有，但下面列表出不来”的问题。

## 9. 导出模板的当前约定

当前导出只保留一个工作表：Aligned Mapping。

主要约定：

1. Step 标题只保留技术名链路，不再拼接红字业务名。
2. Source Table / Target Table 分成两列：技术名 和 文本名。
3. Source 与 Target 两侧都包含 Data Type、Length、Decimals。
4. 若模板文件不可读，前端会自动回退为内存生成模板，不再因模板文件 404 或锁定而失败。

模板相关前端代码集中在 frontend-prototype/app.js。

## 10. 排障建议

### 10.1 候选路径数量异常

优先检查：

1. RSTRAN 是否存在 SOURCE / DATASOURCE 字段差异。
2. RSDS 起点是否带上了 SOURCESYS。
3. 是否又引入了按 TRANID 先裁剪整图的逻辑。

### 10.2 路径有，但下方 mapping 不显示

优先检查：

1. 前端是否仍在读小写 segments / rows。
2. 后端返回是否是大写字段 SEGMENTS / ROWS。
3. Aligned rows 对齐键 SOURCE_FIELD / TARGET_FIELD 是否被改名。

### 10.3 诊断全部显示未校验

优先检查前端 getPathDiagnosticsBucket 是否仍覆盖当前返回结构。

### 10.4 导出报模板读取失败

优先检查：

1. frontend-prototype/Assets/Download template.xlsx 是否可读。
2. 代理是否能正常静态返回 xlsx。
3. 即使模板读取失败，前端也应走 fallback workbook，不应直接报错中断。

## 11. 建议保持不变的接口约束

为了避免再出现一轮大小写和结构回归，建议后续继续保持：

1. 后端 Path Search / Mapping / Text / Logic 都以大写字段为主输出。
2. 前端规范化层继续保留对旧小写字段的兼容读取。
3. 规则逻辑匹配键固定为 TRANID + RULEID + STEPID。
4. RSDS 节点唯一性固定依赖 对象名 + 系统。