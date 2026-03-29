window.MockData = {
  searchPool: [
    { id: "FAO123", type: "ADSO", source: "BW", desc: "财务汇总对象" },
    { id: "ZFAO", type: "InfoSource", source: "ECC", desc: "上游信息源" },
    { id: "RRFAOBB", type: "Report", source: "BW", desc: "报表输出节点" },
    { id: "QWE12FAO", type: "CP", source: "BW", desc: "计算过程链路" },
    { id: "SDFAOEE", type: "DataSource", source: "HANA", desc: "业务数据源" },
    { id: "FAOBSBP1", type: "ADSO", source: "BW", desc: "成本分析对象" },
    { id: "FMEC001", type: "InfoSource", source: "S/4", desc: "工厂信息输入" },
    { id: "ABCADDD", type: "Report", source: "BW", desc: "跨域报表节点" }
  ],
  recentSearches: ["FAO123", "ZFAO", "FMEC001", "FAOBSBP1", "ABCADDD"],
  flowGraph: {
    nodes: [
      { id: "FAO123", label: "FAO123", type: "adso", level: 2, lane: 4, desc: "主对象" },
      { id: "DS_SALES", label: "DS_SALES", type: "datasource", level: 1, lane: 2, desc: "销售数据源" },
      { id: "IS_FIN", label: "IS_FIN", type: "infosource", level: 1, lane: 4, desc: "财务信息源" },
      { id: "CP_ALLOC", label: "CP_ALLOC", type: "cp", level: 3, lane: 3, desc: "分摊计算过程" },
      { id: "ADSO_COST", label: "ADSO_COST", type: "adso", level: 4, lane: 3, desc: "成本中间层" },
      { id: "RP_MONTH", label: "RP_MONTH", type: "report", level: 5, lane: 3, desc: "月报输出" },
      { id: "RP_YEAR", label: "RP_YEAR", type: "report", level: 5, lane: 5, desc: "年报输出" },
      { id: "IS_MATERIAL", label: "IS_MATERIAL", type: "infosource", level: 2, lane: 1, desc: "物料信息源" },
      { id: "CP_CHECK", label: "CP_CHECK", type: "cp", level: 3, lane: 6, desc: "校验流程" }
    ],
    edges: [
      { source: "DS_SALES", target: "FAO123" },
      { source: "IS_FIN", target: "FAO123" },
      { source: "IS_MATERIAL", target: "FAO123" },
      { source: "FAO123", target: "CP_ALLOC" },
      { source: "CP_ALLOC", target: "ADSO_COST" },
      { source: "ADSO_COST", target: "RP_MONTH" },
      { source: "ADSO_COST", target: "RP_YEAR" },
      { source: "FAO123", target: "CP_CHECK" },
      { source: "CP_CHECK", target: "RP_YEAR" }
    ]
  },
  colorMap: {
    datasource: "#9aa0a6",
    infosource: "#ffd54a",
    adso: "#5f8dff",
    cp: "#ff9f43",
    report: "#ff9f43"
  }
};
