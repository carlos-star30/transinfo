# Table Analysis UI Kit

这是一个可以整目录复制走的小组件包，里面打包了你刚才提到的 3 块内容：

1. `Display EN / Display DE / Full Length Display` 这一排按钮
2. 下面配套的列表表格
3. `Object Value Selection Screen` 弹窗，以及弹窗里的表格和按钮

## 目录内容

- `table-analysis-ui-kit.css`
  组件样式
- `table-analysis-ui-kit.js`
  组件行为逻辑
- `demo.html`
  可直接打开看的示例页

## 你怎么用

最简单的方法：

1. 把整个 `table-analysis-ui-kit` 目录复制到你的其他项目里
2. 在你的页面里引入这两个文件

```html
<link rel="stylesheet" href="./table-analysis-ui-kit.css" />
<script src="./table-analysis-ui-kit.js"></script>
```

3. 放一个占位容器，用来挂表格组件

```html
<div id="displayTableMount"></div>
```

4. 在页面脚本里初始化

```html
<script>
  const table = window.TableAnalysisUIKit.createDisplayTable(
    document.getElementById("displayTableMount"),
    {
      title: "Result List",
      columns: [
        { key: "object_type", label: "Type" },
        { key: "object_id", label: "Object" },
        { key: "object_name_en", label: "Object Name EN", localeGroup: "en" },
        { key: "object_name_de", label: "Object Name DE", localeGroup: "de" },
        { key: "name_en", label: "Field Name EN", localeGroup: "en" },
        { key: "name_de", label: "Field Name DE", localeGroup: "de" }
      ],
      rows: [
        {
          object_type: "IOBJ",
          object_id: "0CUSTOMER",
          object_name_en: "Customer",
          object_name_de: "Kunde",
          name_en: "Customer",
          name_de: "Kunde"
        }
      ]
    }
  );
```

## `Display EN / Display DE` 怎么工作

只要某一列写了：

- `localeGroup: "en"`
- 或 `localeGroup: "de"`

它就会被对应按钮控制显示或隐藏。

比如：

```js
{ key: "object_name_en", label: "Object Name EN", localeGroup: "en" }
{ key: "object_name_de", label: "Object Name DE", localeGroup: "de" }
```

## 表格已经包含的功能

- `Display EN`
- `Display DE`
- `Full Length Display`
- 列表渲染
- 行选中高亮
- 点击表头排序
- 列宽拖拽调整
- `Clear Sort`

## Object Value Selection Screen 怎么用

```html
<script>
  const valueModal = window.TableAnalysisUIKit.createValueSelectionModal({
    onConfirm(rules, fieldName) {
      console.log(fieldName, rules);
    },
    onError(error) {
      alert(error.message || String(error));
    }
  });

  valueModal.open({
    title: "Object Value Selection Screen",
    fieldName: "object_id",
    rules: [
      { exclude: false, from: "1000", to: "1999" },
      { exclude: true, from: "1500", to: "1599" }
    ]
  });
</script>
```

## 弹窗已经包含的功能

- 打开 / 关闭
- 最大化 / 还原
- 拖动窗口
- 右下角拖拽改大小
- 增加行
- 清空选择
- 从剪贴板粘贴
- 确认返回规则
- 列排序
- 列宽拖拽
- 行删除

## 建议你怎么复制到其他项目

如果你不想改太多代码，最稳的方式是：

1. 先把整个目录复制过去
2. 先直接打开 `demo.html` 看效果
3. 再把 `demo.html` 里那段初始化代码改成你的字段名和数据

## 这个包放在哪里

当前目录位置：

`reusable/table-analysis-ui-kit`
