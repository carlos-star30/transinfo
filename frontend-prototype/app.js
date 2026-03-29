(() => {
  const appTitle = String(window.__DATAFLOW_APP_TITLE__ || "转换映射查询").trim() || "转换映射查询";
  const appVersion = String(window.__DATAFLOW_APP_VERSION__ || "1.0").trim() || "1.0";
  const appVersionBadge = document.getElementById("appVersionBadge");
  const appTitleText = document.getElementById("appTitleText");
  const loginTitleText = document.getElementById("loginTitleText");
  if (appVersionBadge) {
    appVersionBadge.textContent = `v${appVersion}`;
  }
  if (appTitleText) {
    appTitleText.textContent = appTitle;
  }
  if (loginTitleText) {
    loginTitleText.textContent = appTitle;
  }
  document.title = `${appTitle} v${appVersion}`;

  const VENDOR_ASSET_URLS = Object.freeze({
    aceCore: "./vendor/ace/ace.min.js",
    aceTheme: "./vendor/ace/theme-tomorrow_night_bright.min.js",
    aceModeAbap: "./vendor/ace/mode-abap.min.js",
    excelJs: "./vendor/exceljs/exceljs.min.js",
    xlsx: "./vendor/xlsx/xlsx.full.min.js"
  });
  const vendorScriptLoadPromises = new Map();

  const userMenuBtn = document.getElementById("userMenuBtn");
  const userMenuPanel = document.getElementById("userMenuPanel");
  const userMenuWho = document.getElementById("userMenuWho");
  const changePasswordBtn = document.getElementById("changePasswordBtn");
  const openUserAdminBtn = document.getElementById("openUserAdminBtn");
  const logoutBtn = document.getElementById("logoutBtn");
  const loginGate = document.getElementById("loginGate");
  const loginShell = loginGate ? loginGate.querySelector(".login-shell") : null;
  const loginUsername = document.getElementById("loginUsername");
  const loginPassword = document.getElementById("loginPassword");
  const loginShowPassword = document.getElementById("loginShowPassword");
  const loginErrorText = document.getElementById("loginErrorText");
  const loginSubmitBtn = document.getElementById("loginSubmitBtn");
  const loginCancelBtn = document.getElementById("loginCancelBtn");
  const changePasswordModal = document.getElementById("changePasswordModal");
  const changePasswordModalShell = changePasswordModal ? changePasswordModal.querySelector(".import-shell") : null;
  const maxChangePasswordModal = document.getElementById("maxChangePasswordModal");
  const closeChangePasswordModal = document.getElementById("closeChangePasswordModal");
  const currentPasswordInput = document.getElementById("currentPasswordInput");
  const newPasswordInput = document.getElementById("newPasswordInput");
  const submitChangePasswordBtn = document.getElementById("submitChangePasswordBtn");
  const userAdminModal = document.getElementById("userAdminModal");
  const userAdminModalShell = userAdminModal ? userAdminModal.querySelector(".import-shell") : null;
  const maxUserAdminModal = document.getElementById("maxUserAdminModal");
  const closeUserAdminModal = document.getElementById("closeUserAdminModal");
  const createUserModal = document.getElementById("createUserModal");
  const createUserModalShell = createUserModal ? createUserModal.querySelector(".import-shell") : null;
  const maxCreateUserModal = document.getElementById("maxCreateUserModal");
  const closeCreateUserModal = document.getElementById("closeCreateUserModal");
  const cancelCreateUserBtn = document.getElementById("cancelCreateUserBtn");
  const adminResetUserModal = document.getElementById("adminResetUserModal");
  const adminResetUserModalShell = adminResetUserModal ? adminResetUserModal.querySelector(".import-shell") : null;
  const maxAdminResetUserModal = document.getElementById("maxAdminResetUserModal");
  const closeAdminResetUserModal = document.getElementById("closeAdminResetUserModal");
  const cancelAdminResetUserBtn = document.getElementById("cancelAdminResetUserBtn");
  const logicViewerModal = document.getElementById("logicViewerModal");
  const logicViewerModalShell = logicViewerModal ? logicViewerModal.querySelector(".import-shell") : null;
  const maxLogicViewerModal = document.getElementById("maxLogicViewerModal");
  const closeLogicViewerModal = document.getElementById("closeLogicViewerModal");
  const logicViewerTitle = document.getElementById("logicViewerTitle");
  const logicViewerMeta = document.getElementById("logicViewerMeta");
  const logicViewerNav = document.getElementById("logicViewerNav");
  const logicViewerEntryMeta = document.getElementById("logicViewerEntryMeta");
  const logicViewerPre = document.getElementById("logicViewerPre");
  const logicViewerCode = document.getElementById("logicViewerCode");
  const copyLogicViewerBtn = document.getElementById("copyLogicViewerBtn");
  const submitAdminResetUserBtn = document.getElementById("submitAdminResetUserBtn");
  const adminResetUserNameInput = document.getElementById("adminResetUserNameInput");
  const adminResetUserRoleInput = document.getElementById("adminResetUserRoleInput");
  const adminResetUserPasswordInput = document.getElementById("adminResetUserPasswordInput");
  const userAdminTableBody = document.getElementById("userAdminTableBody");
  const newUserNameInput = document.getElementById("newUserNameInput");
  const newUserPasswordInput = document.getElementById("newUserPasswordInput");
  const newUserRoleSelect = document.getElementById("newUserRoleSelect");
  const createUserBtn = document.getElementById("createUserBtn");
  const submitCreateUserBtn = document.getElementById("submitCreateUserBtn");
  const refreshUsersBtn = document.getElementById("refreshUsersBtn");
  const importRstranCard = document.getElementById("importRstranCard");
  const importBwObjectCard = document.getElementById("importBwObjectCard");
  const importRstranTime = document.getElementById("importRstranTime");
  const importBwObjectTime = document.getElementById("importBwObjectTime");
  const importRstranCount = document.getElementById("importRstranCount");
  const importBwObjectCount = document.getElementById("importBwObjectCount");
  const importModal = document.getElementById("importModal");
  const importModalShell = importModal ? importModal.querySelector(".import-shell") : null;
  const maxImportModal = document.getElementById("maxImportModal");
  const closeImportModal = document.getElementById("closeImportModal");
  const importModalTitle = document.getElementById("importModalTitle");
  const importFileInput = document.getElementById("importFileInput");
  const importSheetSelect = document.getElementById("importSheetSelect");
  const importHeaderRowWrap = document.getElementById("importHeaderRowWrap");
  const importHeaderRowSelect = document.getElementById("importHeaderRowSelect");
  const importMeta = document.getElementById("importMeta");
  const modeQuery = document.getElementById("modeQuery");
  const modeDataQuery = document.getElementById("modeDataQuery");
  const modeUpload = document.getElementById("modeUpload");
  const queryWorkspace = document.getElementById("queryWorkspace");
  const dataQueryWorkspace = document.getElementById("dataQueryWorkspace");
  const uploadWorkspace = document.getElementById("uploadWorkspace");
  const appTabButtons = [modeQuery, modeDataQuery, modeUpload].filter(Boolean);
  const appTabPanels = [queryWorkspace, dataQueryWorkspace, uploadWorkspace].filter(Boolean);
  const startModelInput = document.getElementById("startModelInput");
  const startSourceSystemInput = document.getElementById("startSourceSystemInput");
  const endModelInput = document.getElementById("endModelInput");
  const tranIdInput = document.getElementById("tranIdInput");
  const clearPathFieldsBtn = document.getElementById("clearPathFieldsBtn");
  const clearTranIdBtn = document.getElementById("clearTranIdBtn");
  const startModelHistoryList = document.getElementById("startModelHistoryList");
  const startSourceSystemHistoryList = document.getElementById("startSourceSystemHistoryList");
  const endModelHistoryList = document.getElementById("endModelHistoryList");
  const tranIdHistoryList = document.getElementById("tranIdHistoryList");
  const pathSearchBtn = document.getElementById("pathSearchBtn");
  const pathSelectionPanel = document.getElementById("pathSelectionPanel");
  const pathSelectionStage = document.getElementById("pathSelectionStage");
  const togglePathSelectionBtn = document.getElementById("togglePathSelectionBtn");
  const autoCollapsePathBtn = document.getElementById("autoCollapsePathBtn");
  const pathGraphCanvas = document.getElementById("pathGraphCanvas");
  const pathSelectionSummary = document.getElementById("pathSelectionSummary");
  const confirmPathSelectionBtn = document.getElementById("confirmPathSelectionBtn");
  const mappingResultPanel = document.getElementById("mappingResultPanel");
  const mappingPanelsGrid = document.getElementById("mappingPanelsGrid");
  const toggleMappingResultPanelBtn = document.getElementById("toggleMappingResultPanelBtn");
  const dataQueryResultPanel = document.getElementById("dataQueryResultPanel");
  const toggleDataQueryResultPanelBtn = document.getElementById("toggleDataQueryResultPanelBtn");
  const dataQueryMainTableSelect = document.getElementById("dataQueryMainTableSelect");
  const dataQueryJoinTableSelect = document.getElementById("dataQueryJoinTableSelect");
  const dataQueryJoinTypeSelect = document.getElementById("dataQueryJoinTypeSelect");
  const dataQueryLimitInput = document.getElementById("dataQueryLimitInput");
  const dataQuerySelectFieldSearchInput = document.getElementById("dataQuerySelectFieldSearchInput");
  const dataQuerySelectFieldsList = document.getElementById("dataQuerySelectFieldsList");
  const dataQuerySelectFieldsPanel = document.getElementById("dataQuerySelectFieldsPanel");
  const dataQuerySelectAllFieldsBtn = document.getElementById("dataQuerySelectAllFieldsBtn");
  const dataQueryClearSelectedFieldsBtn = document.getElementById("dataQueryClearSelectedFieldsBtn");
  const dataQueryApplySelectFieldsBtn = document.getElementById("dataQueryApplySelectFieldsBtn");
  const dataQueryToggleSelectFieldsBtn = document.getElementById("dataQueryToggleSelectFieldsBtn");
  const dataQueryJoinConditionsList = document.getElementById("dataQueryJoinConditionsList");
  const dataQueryAddJoinConditionBtn = document.getElementById("dataQueryAddJoinConditionBtn");
  const dataQueryClearJoinConditionsBtn = document.getElementById("dataQueryClearJoinConditionsBtn");
  const dataQueryConfigHint = document.getElementById("dataQueryConfigHint");
  const dataQueryAddFilterBtn = document.getElementById("dataQueryAddFilterBtn");
  const dataQueryClearFiltersBtn = document.getElementById("dataQueryClearFiltersBtn");
  const dataQueryPreviewBtn = document.getElementById("dataQueryPreviewBtn");
  const dataQueryRunBtn = document.getElementById("dataQueryRunBtn");
  const toggleDataQueryConfigBtn = document.getElementById("toggleDataQueryConfigBtn");
  const dataQueryBuilderPanel = document.getElementById("dataQueryBuilderPanel");
  const dataQueryConfigCard = document.getElementById("dataQueryConfigCard");
  const dataQueryFiltersList = document.getElementById("dataQueryFiltersList");
  const dataQueryResultSummary = document.getElementById("dataQueryResultSummary");
  const dataQueryResultFieldsPanel = document.getElementById("dataQueryResultFieldsPanel");
  const dataQueryResultFieldsList = document.getElementById("dataQueryResultFieldsList");
  const dataQuerySelectAllResultFieldsBtn = document.getElementById("dataQuerySelectAllResultFieldsBtn");
  const dataQueryClearResultFieldsBtn = document.getElementById("dataQueryClearResultFieldsBtn");
  const dataQueryApplyResultFieldsBtn = document.getElementById("dataQueryApplyResultFieldsBtn");
  const dataQueryToggleResultFieldsBtn = document.getElementById("dataQueryToggleResultFieldsBtn");
  const dataQueryResultEmpty = document.getElementById("dataQueryResultEmpty");
  const dataQueryGridShell = document.getElementById("dataQueryGridShell");
  const dataQueryGrid = document.getElementById("dataQueryGrid");
  const exportDataQueryResultBtn = document.getElementById("exportDataQueryResultBtn");
  const pathResultActionButtons = [
    document.getElementById("exportPathResultBtn"),
    document.getElementById("copySourceKeyFieldsBtn")
  ].filter(Boolean);
  const importCards = [...document.querySelectorAll(".import-card[data-table]")];
  if (importCards.length) {
    const cardsByContainer = new Map();
    importCards.forEach((card) => {
      const parent = card.parentElement;
      if (!parent) return;
      if (!cardsByContainer.has(parent)) {
        cardsByContainer.set(parent, []);
      }
      cardsByContainer.get(parent).push(card);
    });
    cardsByContainer.forEach((cards, container) => {
      cards
        .sort((left, right) => String(left?.dataset?.table || "").localeCompare(String(right?.dataset?.table || ""), "en", { sensitivity: "base" }))
        .forEach((card) => container.appendChild(card));
    });
  }
  const importCardElements = Object.fromEntries(
    importCards
      .map((card) => [String(card.dataset.table || "").trim().toLowerCase(), card])
      .filter(([tableName]) => tableName)
  );
  const rebuildRstranMappingRuleBtn = document.getElementById("rebuildRstranMappingRuleBtn");
  const rebuildRstranMappingRuleBtnTitle = document.getElementById("rebuildRstranMappingRuleBtnTitle");
  const rebuildRstranMappingRuleFullBtn = document.getElementById("rebuildRstranMappingRuleFullBtn");
  const rebuildRstranMappingRuleFullBtnTitle = document.getElementById("rebuildRstranMappingRuleFullBtnTitle");
  const clearableImportTables = ["rstran", "rstranrule", "rstranfield", "rstranstepcnst", "rstransteprout", "rsoadso", "rsoadsot", "rsds", "rsdst", "rsdssegfd", "rsdssegfdt", "rsksnew", "rsksnewt", "rsksfieldnew", "rsksfieldnewt", "dd03l", "dd02t", "dd03t", "dd04t", "rsdiobj", "rsdiobjt"];
  const importStatusElements = {
    rstran: { time: document.getElementById("importRstranTime"), count: document.getElementById("importRstranCount") },
    rstranrule: { time: document.getElementById("importRstranruleTime"), count: document.getElementById("importRstranruleCount") },
    rstranfield: { time: document.getElementById("importRstranfieldTime"), count: document.getElementById("importRstranfieldCount") },
    rstranstepcnst: { time: document.getElementById("importRstranstepcnstTime"), count: document.getElementById("importRstranstepcnstCount") },
    rstransteprout: { time: document.getElementById("importRstransteproutTime"), count: document.getElementById("importRstransteproutCount") },
    rsoadso: { time: document.getElementById("importRsoadsoTime"), count: document.getElementById("importRsoadsoCount") },
    rsoadsot: { time: document.getElementById("importRsoadsotTime"), count: document.getElementById("importRsoadsotCount") },
    rsds: { time: document.getElementById("importRsdsTime"), count: document.getElementById("importRsdsCount") },
    rsdst: { time: document.getElementById("importRsdstTime"), count: document.getElementById("importRsdstCount") },
    rsdssegfd: { time: document.getElementById("importRsdssegfdTime"), count: document.getElementById("importRsdssegfdCount") },
    rsdssegfdt: { time: document.getElementById("importRsdssegfdtTime"), count: document.getElementById("importRsdssegfdtCount") },
    rsksnew: { time: document.getElementById("importRsksnewTime"), count: document.getElementById("importRsksnewCount") },
    rsksnewt: { time: document.getElementById("importRsksnewtTime"), count: document.getElementById("importRsksnewtCount") },
    rsksfieldnew: { time: document.getElementById("importRsksfieldnewTime"), count: document.getElementById("importRsksfieldnewCount") },
    rsksfieldnewt: { time: document.getElementById("importRsksfieldnewtTime"), count: document.getElementById("importRsksfieldnewtCount") },
    dd03l: { time: document.getElementById("importDd03lTime"), count: document.getElementById("importDd03lCount") },
    dd02t: { time: document.getElementById("importDd02tTime"), count: document.getElementById("importDd02tCount") },
    dd03t: { time: document.getElementById("importDd03tTime"), count: document.getElementById("importDd03tCount") },
    dd04t: { time: document.getElementById("importDd04tTime"), count: document.getElementById("importDd04tCount") },
    rsdiobj: { time: document.getElementById("importRsiobjTime"), count: document.getElementById("importRsiobjCount") },
    rsdiobjt: { time: document.getElementById("importRsiobjtTime"), count: document.getElementById("importRsiobjtCount") }
  };
  const rebuildDependencyConfigs = {
    mappingRule: {
      actionLabel: "重建字段规则表",
      requiredTables: ["rstranrule", "rstranfield"],
      optionalTables: ["rstransteprout", "rstranstepcnst"]
    },
    mappingRuleFull: {
      actionLabel: "重建完整字段表",
      requiredTables: ["rstran", "rstranrule", "rstranfield"],
      optionalTables: ["rstransteprout", "rstranstepcnst", "dd03l", "rsdssegfd", "rsksfieldnew", "rsdiobj", "rsdiobjt", "rsoadso"]
    }
  };
  let latestImportStatusPayload = {};
  let importStatusLoadPromise = null;
  let importStatusLoadedOnce = false;

  function loadVendorScriptOnce(src) {
    const normalizedSrc = String(src || "").trim();
    if (!normalizedSrc) {
      return Promise.reject(new Error("Missing vendor script URL."));
    }
    if (vendorScriptLoadPromises.has(normalizedSrc)) {
      return vendorScriptLoadPromises.get(normalizedSrc);
    }

    const pending = new Promise((resolve, reject) => {
      const script = document.createElement("script");
      script.src = normalizedSrc;
      script.async = true;
      script.onload = () => resolve();
      script.onerror = () => {
        vendorScriptLoadPromises.delete(normalizedSrc);
        reject(new Error(`Failed to load script: ${normalizedSrc}`));
      };
      document.head.appendChild(script);
    });

    vendorScriptLoadPromises.set(normalizedSrc, pending);
    return pending;
  }

  async function ensureAceAssetsLoaded() {
    if (window.ace) return window.ace;
    await loadVendorScriptOnce(VENDOR_ASSET_URLS.aceCore);
    if (window.ace?.config && typeof window.ace.config.set === "function") {
      window.ace.config.set("basePath", "./vendor/ace");
    }
    await Promise.all([
      loadVendorScriptOnce(VENDOR_ASSET_URLS.aceTheme),
      loadVendorScriptOnce(VENDOR_ASSET_URLS.aceModeAbap)
    ]);
    if (!window.ace) {
      throw new Error("ACE editor failed to initialize.");
    }
    return window.ace;
  }

  async function ensureExcelJsLoaded() {
    if (window.ExcelJS) return window.ExcelJS;
    await loadVendorScriptOnce(VENDOR_ASSET_URLS.excelJs);
    if (!window.ExcelJS) {
      throw new Error("ExcelJS failed to initialize.");
    }
    return window.ExcelJS;
  }

  async function ensureXlsxLoaded() {
    if (window.XLSX) return window.XLSX;
    await loadVendorScriptOnce(VENDOR_ASSET_URLS.xlsx);
    if (!window.XLSX) {
      throw new Error("XLSX failed to initialize.");
    }
    return window.XLSX;
  }

  async function ensureXlsxLoadedOrNotify() {
    try {
      await ensureXlsxLoaded();
      return true;
    } catch {
      window.alert("当前环境未加载 Excel 解析库，请刷新后重试。若问题持续，请检查本地依赖文件。");
      return false;
    }
  }

  function updatePasswordToggleButton(toggleButton, visible) {
    if (!toggleButton) return;
    const isVisible = !!visible;
    toggleButton.setAttribute("aria-pressed", isVisible ? "true" : "false");
    toggleButton.setAttribute("aria-label", isVisible ? "隐藏密码" : "显示密码");
    toggleButton.setAttribute("title", isVisible ? "隐藏密码" : "显示密码");
    toggleButton.classList.toggle("is-visible", isVisible);
  }

  function setPasswordInputVisible(inputEl, visible) {
    if (!inputEl) return;
    const isVisible = !!visible;
    inputEl.setAttribute("type", isVisible ? "text" : "password");
    const wrapper = inputEl.closest(".login-password-inline, .password-toggle-field");
    const toggleButton = wrapper?.querySelector(".login-password-toggle, .password-toggle-btn") || null;
    updatePasswordToggleButton(toggleButton, isVisible);
  }

  function resetPasswordInputs(...inputs) {
    inputs.forEach((inputEl) => setPasswordInputVisible(inputEl, false));
  }

  function enhancePasswordInput(inputEl, options = {}) {
    if (!inputEl || inputEl.dataset.passwordToggleReady === "1") return;

    let wrapper = inputEl.closest(".login-password-inline, .password-toggle-field");
    let toggleButton = null;

    if (wrapper && wrapper.contains(inputEl)) {
      wrapper.classList.add("password-toggle-field");
      toggleButton = wrapper.querySelector(".login-password-toggle, .password-toggle-btn");
    } else {
      wrapper = document.createElement("div");
      wrapper.className = "password-toggle-field";
      inputEl.parentNode?.insertBefore(wrapper, inputEl);
      wrapper.appendChild(inputEl);
    }

    if (!toggleButton) {
      toggleButton = document.createElement("button");
      toggleButton.type = "button";
      toggleButton.className = "password-toggle-btn";
      wrapper.appendChild(toggleButton);
    }

    toggleButton.classList.add("password-toggle-btn");
    if (options.id) {
      toggleButton.id = options.id;
    }
    if (!toggleButton.querySelector(".password-toggle-eye, .login-password-toggle-eye")) {
      const eye = document.createElement("span");
      eye.className = "password-toggle-eye";
      eye.setAttribute("aria-hidden", "true");
      toggleButton.replaceChildren(eye);
    }

    if (toggleButton.dataset.passwordToggleBound !== "1") {
      toggleButton.addEventListener("click", () => {
        const nextVisible = toggleButton.getAttribute("aria-pressed") !== "true";
        setPasswordInputVisible(inputEl, nextVisible);
      });
      toggleButton.dataset.passwordToggleBound = "1";
    }

    inputEl.dataset.passwordToggleReady = "1";
    setPasswordInputVisible(inputEl, false);
  }
  const importProgressWrap = document.getElementById("importProgressWrap");
  const importProgressText = document.getElementById("importProgressText");
  const importProgressBar = document.getElementById("importProgressBar");
  const importMapBody = document.getElementById("importMapBody");
  const autoMapBtn = document.getElementById("autoMapBtn");
  const clearImportTableBtn = document.getElementById("clearImportTableBtn");
  const confirmImportBtn = document.getElementById("confirmImportBtn");
  const appLoadingOverlay = document.getElementById("appLoadingOverlay");
  const appLoadingText = document.getElementById("appLoadingText");
  const appToast = document.getElementById("appToast");
  const appToastCard = document.getElementById("appToastCard");
  const appToastTitle = document.getElementById("appToastTitle");
  const appToastTitleText = document.getElementById("appToastTitleText");
  const appToastText = document.getElementById("appToastText");
  const appToastActions = document.getElementById("appToastActions");
  const appToastCopyBtn = document.getElementById("appToastCopyBtn");
  const appToastSecondaryBtn = document.getElementById("appToastSecondaryBtn");
  const appToastPrimaryBtn = document.getElementById("appToastPrimaryBtn");
  const appToastCloseBtn = document.getElementById("appToastCloseBtn");
  const blockingModalEntries = [
    { modal: loginGate, shell: loginShell },
    { modal: importModal, shell: importModalShell },
    { modal: changePasswordModal, shell: changePasswordModalShell },
    { modal: userAdminModal, shell: userAdminModalShell },
    { modal: createUserModal, shell: createUserModalShell },
    { modal: adminResetUserModal, shell: adminResetUserModalShell },
    { modal: logicViewerModal, shell: logicViewerModalShell }
  ].filter((entry) => entry.modal && entry.shell);
  let modalZIndexSeed = 2000;
  let appLoadingDepth = 0;
  let activeWorkbenchCard = null;
  const LARGE_IMPORT_CSV_CHUNK_ROWS = 5000;
  let importCardRefreshDepth = 0;

  let currentUser = null;
  let authStateEpoch = 0;
  let isAuthBootstrapInFlight = true;
  function normalizeLoopbackApiHost(rawHost) {
    const host = String(rawHost || "").trim();
    if (!host) return host;
    const normalizedHost = host.replace(/^file:\/\//i, "http://");
    try {
      const url = new URL(normalizedHost);
      const loopbacks = new Set(["localhost", "127.0.0.1"]);
      const pageHost = String(window.location.hostname || "").trim();
      if (loopbacks.has(url.hostname) && loopbacks.has(pageHost) && url.hostname !== pageHost) {
        url.hostname = pageHost;
      }
      return url.toString().replace(/\/+$/, "");
    } catch {
      return normalizedHost.replace(/\/+$/, "");
    }
  }

  function resolveApiBase() {
    const fromRuntime = String(window.__DATAFLOW_API_BASE__ || "").trim();
    const fromQuery = String(new URLSearchParams(window.location.search).get("apiBase") || "").trim();
    const pageProtocol = window.location.protocol === "file:" ? "http:" : window.location.protocol;
    const pageDefault = `${pageProtocol}//${window.location.hostname || "localhost"}:8000`;
    if (!fromRuntime && !fromQuery) {
      try {
        window.localStorage.removeItem("df-api-base");
      } catch {
        // Ignore storage failures in restrictive browser contexts.
      }
    }
    const host = fromRuntime || fromQuery || pageDefault;
    return `${normalizeLoopbackApiHost(host)}/api`;
  }

  const importStatusApiBase = resolveApiBase();
  const importStatusApiHost = importStatusApiBase.replace(/\/api\/?$/, "");
  const AUTH_TOKEN_STORAGE_KEY = "df-session-token";
  try {
    // Keep all frontend pages on the same API host.
    window.localStorage.setItem("df-api-base", importStatusApiHost);
  } catch {
    // Ignore storage failures in restrictive browser contexts.
  }
  const HOME_STATE_KEY = "df-home-state-v1";
  const DATA_QUERY_STATE_KEY = "df-database-query-state-v1";
  const PATH_INPUT_HISTORY_KEY = "df-path-input-history-v1";
  const PATH_INPUT_HISTORY_MAX = 10;
  const DATA_QUERY_PREVIEW_LIMIT = 10;
  const DATA_QUERY_DEFAULT_LIMIT = 200;
  const PATH_MAPPING_REBUILD_MODE = false;
  const IMPORT_MAPPING_REBUILD_MODE = false;
  let activeImportTable = "";
  let activeExcelHeaders = [];
  let activeImportSheetNames = [];
  let activeImportFileName = "";
  let activeImportDataRowCount = 0;
  let toastTimer = null;
  let loginLockCountdownTimer = null;
  let loginLockRemainingSeconds = 0;
  let selectedAdminResetUsername = "";
  let importProgressTimer = null;
  let importProgressValue = 0;
  let importTaskLock = false;
  let mappingRuleRebuildLock = false;
  let exportPathWorkbookLock = false;
  let lastToastMessage = "";
  let toastIsBlocking = false;
  let toastCloseResolver = null;
  let toastDismissValue = "close";
  let activeHeaderRowNumber = 1;
  let pathInputHistory = {
    source: [],
    sourceSystem: [],
    target: [],
    tranId: []
  };
  let activePathSearchResult = null;
  let activePathMappingResult = null;
  let selectedPathCandidateId = "";
  const MAPPING_SOURCE_FIELD_MIN_WIDTH = 96;
  const MAPPING_TARGET_FIELD_MIN_WIDTH = 82;
  const MAPPING_FIELD_TEXT_MIN_WIDTH = 140;
  let isPathSelectionCollapsed = false;
  let autoCollapsePathSelection = true;
  let dataQueryTables = [];
  let dataQueryTablesLoadPromise = null;
  let dataQuerySchemaCache = {};
  let dataQuerySchemaLoadPromises = {};
  let dataQueryGridApi = null;
  let dataQueryPreviewTimer = 0;
  let dataQueryRequestSeq = 0;
  let dataQueryInFlight = false;
  let latestDataQueryResult = null;
  let latestDataQueryResultMode = "preview";
  let dataQueryResultFieldDraft = [];
  let dataQuerySelectedFieldDraft = [];
  let isDataQueryPanelCollapsed = false;
  let dataQueryState = {
    mainTable: "",
    joinTable: "",
    joinType: "left",
    joinConditions: [],
    selectedFields: [],
    outputFields: [],
    outputFieldsConfigured: false,
    resultFieldsCollapsed: false,
    selectFieldsCollapsed: false,
    limit: DATA_QUERY_DEFAULT_LIMIT,
    panelCollapsed: false,
    filters: [],
  };

  function getStoredAuthToken() {
    try {
      return String(window.localStorage.getItem(AUTH_TOKEN_STORAGE_KEY) || "").trim();
    } catch {
      return "";
    }
  }

  function storeAuthToken(token) {
    try {
      const normalized = String(token || "").trim();
      if (normalized) {
        window.localStorage.setItem(AUTH_TOKEN_STORAGE_KEY, normalized);
      } else {
        window.localStorage.removeItem(AUTH_TOKEN_STORAGE_KEY);
      }
    } catch {
      // Ignore storage failures in restrictive browser contexts.
    }
  }
  let appliedPathCandidateId = "";
  let pathSearchInFlight = false;
  let pathRuleColumnVisible = true;
  let pathHideNonKeyByStep = {};
  let pathFieldTextVisibleState = { source: false, stepTargets: {} };
  let activePathMappingFeatures = { logic: false, text: false };
  let activeAgGridApi = null;
  let activeAlignedRowsForView = [];
  let activeAgGridColumnState = [];
  let skipAgGridStateRestoreOnce = false;
  let activeMappingQuickFilter = "";
  let activeAgGridHeaderResizeObserver = null;
  let activeLogicViewerEntries = [];
  let activeLogicViewerIndex = 0;
  let activeLogicViewerContext = null;
  let logicViewerAceEditor = null;

  function clampNumber(value, min, max) {
    return Math.max(min, Math.min(max, value));
  }

  function getStepFieldTextToggleKey(segment, segmentIndex) {
    return String(segment?.index || segmentIndex + 1);
  }

  function buildFieldTextVisibleState(segments = []) {
    const stepTargets = {};
    (Array.isArray(segments) ? segments : []).forEach((segment, segmentIndex) => {
      stepTargets[getStepFieldTextToggleKey(segment, segmentIndex)] = false;
    });
    return { source: false, stepTargets };
  }

  function isSourceFieldTextVisible() {
    return Boolean(pathFieldTextVisibleState?.source);
  }

  function isStepTargetFieldTextVisible(segment, segmentIndex) {
    const key = getStepFieldTextToggleKey(segment, segmentIndex);
    return Boolean(pathFieldTextVisibleState?.stepTargets?.[key]);
  }

  function areAllFieldTextTogglesVisible() {
    const stepStates = Object.values(pathFieldTextVisibleState?.stepTargets || {});
    return [Boolean(pathFieldTextVisibleState?.source), ...stepStates].every(Boolean);
  }

  function isAnyFieldTextToggleVisible() {
    if (Boolean(pathFieldTextVisibleState?.source)) return true;
    return Object.values(pathFieldTextVisibleState?.stepTargets || {}).some(Boolean);
  }

  function getPathMappingFeaturesFromPayload(mappingResult) {
    return {
      logic: Boolean(mappingResult?.include_logic),
      text: Boolean(mappingResult?.include_text)
    };
  }

  function buildPathMappingRequestSegments(rawSegments) {
    return (Array.isArray(rawSegments) ? rawSegments : []).map((segment) => ({
      source: String(segment?.source || "").trim(),
      target: String(segment?.target || "").trim(),
      source_type: String(segment?.source_type || "").trim(),
      target_type: String(segment?.target_type || "").trim(),
      tran_ids: Array.isArray(segment?.tran_ids) ? segment.tran_ids : []
    }));
  }

  function getActivePathMappingRequestSegments() {
    const appliedPath = getAppliedPathCandidate();
    if (appliedPath?.segments) {
      return buildPathMappingRequestSegments(appliedPath.segments);
    }
    return buildPathMappingRequestSegments(activePathMappingResult?.segments);
  }

  function setFieldTextToggleVisible(toggleScope, isVisible) {
    const scope = String(toggleScope || "").trim();
    if (!scope) return;
    if (scope === "all") {
      const nextVisible = Boolean(isVisible);
      pathFieldTextVisibleState.source = nextVisible;
      pathFieldTextVisibleState.stepTargets = Object.fromEntries(
        Object.keys(pathFieldTextVisibleState?.stepTargets || {}).map((key) => [key, nextVisible])
      );
      return;
    }
    if (scope === "source") {
      pathFieldTextVisibleState.source = Boolean(isVisible);
      return;
    }
    if (scope.startsWith("step:")) {
      const stepKey = scope.slice(5).trim();
      if (!stepKey) return;
      if (!pathFieldTextVisibleState?.stepTargets) {
        pathFieldTextVisibleState.stepTargets = {};
      }
      pathFieldTextVisibleState.stepTargets[stepKey] = Boolean(isVisible);
    }
  }
  let activeAgGridHeaderSyncRaf = 0;
  let activeAgGridGroupHeaderHeight = 58;
  let activeAgGridBodyViewport = null;
  let activeAgGridBodyScrollHandler = null;

  function isBlockingModalVisible(modal) {
    return Boolean(modal) && !modal.classList.contains("hidden");
  }

  function syncBlockingModalState() {
    document.body.classList.toggle(
      "modal-open",
      blockingModalEntries.some((entry) => isBlockingModalVisible(entry.modal))
    );
    document.body.classList.toggle("logic-viewer-open", isBlockingModalVisible(logicViewerModal));
  }

  function bringBlockingModalToFront(modal) {
    if (!modal) return;
    modalZIndexSeed += 1;
    modal.style.zIndex = String(modalZIndexSeed);
  }

  function syncAppLoadingZIndex() {
    if (!appLoadingOverlay) return;
    const topEntry = getTopBlockingModalEntry();
    const topModalZIndex = topEntry ? Number.parseInt(window.getComputedStyle(topEntry.modal).zIndex || "0", 10) || 0 : 0;
    appLoadingOverlay.style.zIndex = String(Math.max(3200, modalZIndexSeed + 2, topModalZIndex + 2));
  }

  function showBlockingModal(modal) {
    if (!modal) return;
    modal.classList.remove("hidden");
    modal.setAttribute("aria-hidden", "false");
    bringBlockingModalToFront(modal);
    syncBlockingModalState();
  }

  function hideBlockingModal(modal) {
    if (!modal) return;
    modal.classList.add("hidden");
    modal.setAttribute("aria-hidden", "true");
    modal.style.removeProperty("z-index");
    syncBlockingModalState();
  }

  function getTopBlockingModalEntry() {
    const visibleEntries = blockingModalEntries.filter((entry) => isBlockingModalVisible(entry.modal));
    if (!visibleEntries.length) return null;
    visibleEntries.sort((left, right) => {
      const leftZ = Number.parseInt(window.getComputedStyle(left.modal).zIndex || "0", 10) || 0;
      const rightZ = Number.parseInt(window.getComputedStyle(right.modal).zIndex || "0", 10) || 0;
      return leftZ - rightZ;
    });
    return visibleEntries[visibleEntries.length - 1] || null;
  }

  function setupBlockingModalGuards() {
    const swallowBackdropEvent = (event) => {
      const topEntry = getTopBlockingModalEntry();
      if (!topEntry) return;
      const target = event.target;
      if (!(target instanceof Node)) return;
      if (topEntry.shell.contains(target)) {
        bringBlockingModalToFront(topEntry.modal);
        return;
      }
      if (topEntry.modal.contains(target)) {
        bringBlockingModalToFront(topEntry.modal);
        event.preventDefault();
        event.stopPropagation();
      }
    };

    document.addEventListener("pointerdown", swallowBackdropEvent, true);
    document.addEventListener("click", swallowBackdropEvent, true);
  }

  async function apiFetch(url, options = {}, skipAuthGuard = false) {
    const opts = options || {};
    const hasExternalSignal = Boolean(opts.signal);
    const timeoutMs = Number.isFinite(Number(opts.timeoutMs)) ? Number(opts.timeoutMs) : 20000;
    const controller = hasExternalSignal ? null : new AbortController();
    const headers = new Headers(opts.headers || {});
    const sessionToken = getStoredAuthToken();
    if (sessionToken && !headers.has("Authorization")) {
      headers.set("Authorization", `Bearer ${sessionToken}`);
    }
    const timeoutId = hasExternalSignal
      ? null
      : window.setTimeout(() => {
        controller.abort();
      }, timeoutMs);

    try {
      const response = await fetch(url, {
        credentials: "include",
        ...opts,
        headers,
        signal: hasExternalSignal ? opts.signal : controller.signal
      });
      if (response.status === 401 && !skipAuthGuard) {
        storeAuthToken("");
        showLoginGate();
      }
      return response;
    } catch (error) {
      if (error?.name === "AbortError") {
        throw new Error("request_timeout");
      }
      throw error;
    } finally {
      if (timeoutId !== null) {
        window.clearTimeout(timeoutId);
      }
    }
  }

  function parseErrorText(text, fallback) {
    let message = text || fallback;
    try {
      const parsed = JSON.parse(text);
      if (parsed?.detail) {
        message = String(parsed.detail);
      }
    } catch {
      // Keep raw text if response is not JSON.
    }
    return message;
  }

  function isUnauthorizedError(error) {
    const message = String(error?.message || "");
    return /(?:^|\b)status 401\b|unauthorized|未登录|未认证|会话已过期/i.test(message);
  }

  async function authLogin(username, password) {
    const resp = await apiFetch(`${importStatusApiBase}/auth/login`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ username, password })
    }, true);
    if (!resp.ok) {
      throw new Error(parseErrorText(await resp.text(), `status ${resp.status}`));
    }
    const payload = await resp.json();
    storeAuthToken(payload?.session_token || "");
    return payload;
  }

  async function authMe() {
    const resp = await apiFetch(`${importStatusApiBase}/auth/me`, {}, true);
    if (!resp.ok) {
      throw new Error(parseErrorText(await resp.text(), `status ${resp.status}`));
    }
    return resp.json();
  }

  async function authLogout() {
    const resp = await apiFetch(`${importStatusApiBase}/auth/logout`, { method: "POST" }, true);
    if (!resp.ok) {
      throw new Error(parseErrorText(await resp.text(), `status ${resp.status}`));
    }
    storeAuthToken("");
  }

  async function authChangePassword(currentPassword, newPassword) {
    const resp = await apiFetch(`${importStatusApiBase}/auth/change-password`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ current_password: currentPassword, new_password: newPassword })
    });
    if (!resp.ok) {
      throw new Error(parseErrorText(await resp.text(), `status ${resp.status}`));
    }
    return resp.json();
  }

  async function adminListUsers() {
    const resp = await apiFetch(`${importStatusApiBase}/admin/users`);
    if (!resp.ok) {
      throw new Error(parseErrorText(await resp.text(), `status ${resp.status}`));
    }
    return resp.json();
  }

  async function adminCreateUser(username, password, role) {
    const resp = await apiFetch(`${importStatusApiBase}/admin/users`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ username, password, role })
    });
    if (!resp.ok) {
      throw new Error(parseErrorText(await resp.text(), `status ${resp.status}`));
    }
    return resp.json();
  }

  async function adminToggleLock(username, lock) {
    const endpoint = lock ? "lock" : "unlock";
    const resp = await apiFetch(`${importStatusApiBase}/admin/users/${encodeURIComponent(username)}/${endpoint}`, {
      method: "POST"
    });
    if (!resp.ok) {
      throw new Error(parseErrorText(await resp.text(), `status ${resp.status}`));
    }
    return resp.json();
  }

  async function adminResetPassword(username, newPassword) {
    const resp = await apiFetch(`${importStatusApiBase}/admin/users/${encodeURIComponent(username)}/reset-password`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ new_password: newPassword })
    });
    if (!resp.ok) {
      throw new Error(parseErrorText(await resp.text(), `status ${resp.status}`));
    }
    return resp.json();
  }

  async function adminDeleteUser(username) {
    const resp = await apiFetch(`${importStatusApiBase}/admin/users/${encodeURIComponent(username)}`, {
      method: "DELETE"
    });
    if (!resp.ok) {
      throw new Error(parseErrorText(await resp.text(), `status ${resp.status}`));
    }
    return resp.json();
  }

  async function fetchDataQueryTables() {
    const resp = await apiFetch(`${importStatusApiBase}/data-query/tables`);
    if (!resp.ok) {
      throw new Error(parseErrorText(await resp.text(), `status ${resp.status}`));
    }
    return resp.json();
  }

  async function fetchDataQuerySchema(tableName) {
    const resp = await apiFetch(`${importStatusApiBase}/data-query/schema?table_name=${encodeURIComponent(tableName)}`);
    if (!resp.ok) {
      throw new Error(parseErrorText(await resp.text(), `status ${resp.status}`));
    }
    return resp.json();
  }

  async function runDataQueryRequest(payload) {
    const resp = await apiFetch(`${importStatusApiBase}/data-query/query`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    });
    if (!resp.ok) {
      throw new Error(parseErrorText(await resp.text(), `status ${resp.status}`));
    }
    return resp.json();
  }

  function createEmptyDataQueryFilter() {
    return {
      id: `dqf_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
      fieldRef: "",
      operator: "in",
      valuesText: "",
      rangeStart: "",
      rangeEnd: "",
    };
  }

  function createEmptyDataQueryJoinCondition() {
    return {
      id: `dqj_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
      mainField: "",
      joinField: "",
    };
  }

  function normalizeDataQueryTableName(value) {
    return String(value || "").trim().toLowerCase();
  }

  function normalizeDataQueryLimit(value) {
    const parsed = Number.parseInt(String(value || "").trim(), 10);
    if (!Number.isFinite(parsed) || parsed <= 0) {
      return DATA_QUERY_DEFAULT_LIMIT;
    }
    return clampNumber(parsed, DATA_QUERY_PREVIEW_LIMIT, 1000);
  }

  function normalizeDataQueryFieldRef(value) {
    const raw = String(value || "").trim();
    if (!raw || !raw.includes(".")) return "";
    const [sourceKeyRaw, ...fieldNameChunks] = raw.split(".");
    const sourceKey = String(sourceKeyRaw || "").trim().toLowerCase();
    const fieldName = fieldNameChunks.join(".").trim();
    if (!fieldName || (sourceKey !== "main" && sourceKey !== "join")) return "";
    return `${sourceKey}.${fieldName}`;
  }

  function normalizeDataQueryResultColumnKey(value) {
    return String(value || "").trim();
  }

  function splitDataQueryValuesText(text) {
    return String(text || "")
      .split(/[\n,;\t]+/)
      .map((item) => String(item || "").trim())
      .filter(Boolean);
  }

  async function readTextFromClipboard() {
    if (!navigator?.clipboard?.readText) {
      throw new Error("Clipboard API unavailable");
    }
    return navigator.clipboard.readText();
  }

  function isDataQueryJoinConfigured() {
    if (!dataQueryState.joinTable) return true;
    const joinConditions = Array.isArray(dataQueryState.joinConditions) ? dataQueryState.joinConditions : [];
    if (!joinConditions.length) return false;
    return joinConditions.every((joinItem) => String(joinItem?.mainField || "").trim() && String(joinItem?.joinField || "").trim());
  }

  function buildDataQueryFieldOptions() {
    const options = [];
    const appendOptions = (sourceKey, tableName) => {
      const normalizedTableName = normalizeDataQueryTableName(tableName);
      if (!normalizedTableName) return;
      const schema = dataQuerySchemaCache[normalizedTableName] || [];
      schema.forEach((column) => {
        const columnName = String(column?.name || "").trim();
        if (!columnName) return;
        options.push({
          value: `${sourceKey}.${columnName}`,
          label: `${sourceKey === "main" ? "主表" : "关联"}.${columnName}`,
          fieldName: columnName,
          fieldText: String(column?.field_text || "").trim(),
          comment: String(column?.comment || "").trim(),
        });
      });
    };
    appendOptions("main", dataQueryState.mainTable);
    appendOptions("join", dataQueryState.joinTable);
    return options;
  }

  function sanitizeDataQuerySelectedFields() {
    const validRefs = new Set(buildDataQueryFieldOptions().map((item) => String(item?.value || "").trim()).filter(Boolean));
    const dedup = [];
    const seen = new Set();
    (Array.isArray(dataQueryState.selectedFields) ? dataQueryState.selectedFields : []).forEach((fieldRef) => {
      const normalizedRef = normalizeDataQueryFieldRef(fieldRef);
      if (!normalizedRef || !validRefs.has(normalizedRef) || seen.has(normalizedRef)) return;
      seen.add(normalizedRef);
      dedup.push(normalizedRef);
    });
    dataQueryState.selectedFields = dedup;
  }

  function sanitizeDataQuerySelectedFieldDraft() {
    const validRefs = new Set(buildDataQueryFieldOptions().map((item) => String(item?.value || "").trim()).filter(Boolean));
    const dedup = [];
    const seen = new Set();
    (Array.isArray(dataQuerySelectedFieldDraft) ? dataQuerySelectedFieldDraft : []).forEach((fieldRef) => {
      const normalizedRef = normalizeDataQueryFieldRef(fieldRef);
      if (!normalizedRef || !validRefs.has(normalizedRef) || seen.has(normalizedRef)) return;
      seen.add(normalizedRef);
      dedup.push(normalizedRef);
    });
    dataQuerySelectedFieldDraft = dedup;
  }

  function syncDataQuerySelectedFieldDraft(forceFromCommitted = false) {
    sanitizeDataQuerySelectedFields();
    if (forceFromCommitted || !Array.isArray(dataQuerySelectedFieldDraft) || !dataQuerySelectedFieldDraft.length) {
      dataQuerySelectedFieldDraft = [...(Array.isArray(dataQueryState.selectedFields) ? dataQueryState.selectedFields : [])];
    }
    sanitizeDataQuerySelectedFieldDraft();
  }

  function syncDataQuerySelectFieldsPanelCollapsed() {
    if (!dataQuerySelectFieldsPanel || !dataQueryToggleSelectFieldsBtn) return;
    const isCollapsed = Boolean(dataQueryState.selectFieldsCollapsed);
    dataQuerySelectFieldsPanel.classList.toggle("is-collapsed", isCollapsed);
    dataQueryToggleSelectFieldsBtn.classList.toggle("is-collapsed", isCollapsed);
    dataQueryToggleSelectFieldsBtn.setAttribute("aria-expanded", isCollapsed ? "false" : "true");
    dataQueryToggleSelectFieldsBtn.setAttribute("title", isCollapsed ? "展开查询字段" : "收起查询字段");
    dataQueryToggleSelectFieldsBtn.setAttribute("aria-label", isCollapsed ? "展开查询字段" : "收起查询字段");
  }

  function sanitizeDataQueryOutputFields(columns = latestDataQueryResult?.columns) {
    const validKeys = new Set((Array.isArray(columns) ? columns : []).map((column) => normalizeDataQueryResultColumnKey(column?.key)).filter(Boolean));
    const dedup = [];
    const seen = new Set();
    (Array.isArray(dataQueryState.outputFields) ? dataQueryState.outputFields : []).forEach((fieldKey) => {
      const normalizedKey = normalizeDataQueryResultColumnKey(fieldKey);
      if (!normalizedKey || !validKeys.has(normalizedKey) || seen.has(normalizedKey)) return;
      seen.add(normalizedKey);
      dedup.push(normalizedKey);
    });
    dataQueryState.outputFields = dedup;
  }

  function sanitizeDataQueryResultFieldDraft(columns = latestDataQueryResult?.columns) {
    const validKeys = new Set((Array.isArray(columns) ? columns : []).map((column) => normalizeDataQueryResultColumnKey(column?.key)).filter(Boolean));
    const dedup = [];
    const seen = new Set();
    (Array.isArray(dataQueryResultFieldDraft) ? dataQueryResultFieldDraft : []).forEach((fieldKey) => {
      const normalizedKey = normalizeDataQueryResultColumnKey(fieldKey);
      if (!normalizedKey || !validKeys.has(normalizedKey) || seen.has(normalizedKey)) return;
      seen.add(normalizedKey);
      dedup.push(normalizedKey);
    });
    dataQueryResultFieldDraft = dedup;
  }

  function syncDataQueryResultFieldDraft(columns = latestDataQueryResult?.columns, forceFromCommitted = false) {
    const allColumnKeys = (Array.isArray(columns) ? columns : []).map((column) => normalizeDataQueryResultColumnKey(column?.key)).filter(Boolean);
    sanitizeDataQueryOutputFields(columns);
    if (!dataQueryState.outputFieldsConfigured) {
      dataQueryState.outputFields = [...allColumnKeys];
      dataQueryResultFieldDraft = [...allColumnKeys];
      return;
    }
    if (forceFromCommitted || !Array.isArray(dataQueryResultFieldDraft) || !dataQueryResultFieldDraft.length) {
      dataQueryResultFieldDraft = [...(Array.isArray(dataQueryState.outputFields) ? dataQueryState.outputFields : [])];
    }
    sanitizeDataQueryResultFieldDraft(columns);
  }

  function getActiveDataQueryResultColumns(columns = latestDataQueryResult?.columns) {
    const allColumns = Array.isArray(columns) ? columns : [];
    return allColumns;
  }

  function syncDataQueryResultFieldsPanelCollapsed() {
    if (!dataQueryResultFieldsPanel || !dataQueryToggleResultFieldsBtn) return;
    const isCollapsed = Boolean(dataQueryState.resultFieldsCollapsed);
    dataQueryResultFieldsPanel.classList.toggle("is-collapsed", isCollapsed);
    dataQueryToggleResultFieldsBtn.textContent = isCollapsed ? "展开" : "折叠";
  }

  function getDataQueryCommonJoinFields() {
    const mainSchema = dataQuerySchemaCache[dataQueryState.mainTable] || [];
    const joinSchema = dataQuerySchemaCache[dataQueryState.joinTable] || [];
    const joinFieldSet = new Set(joinSchema.map((column) => String(column?.name || "").trim().toUpperCase()).filter(Boolean));
    return mainSchema
      .map((column) => String(column?.name || "").trim())
      .filter((name) => name && joinFieldSet.has(name.toUpperCase()));
  }

  function syncAutoSelectedJoinFields() {
    if (!dataQueryState.mainTable || !dataQueryState.joinTable) return;
    const commonFields = getDataQueryCommonJoinFields();
    if (!commonFields.length) return;
    if (!Array.isArray(dataQueryState.joinConditions) || !dataQueryState.joinConditions.length) {
      dataQueryState.joinConditions = [createEmptyDataQueryJoinCondition()];
    }
    const currentJoinCondition = dataQueryState.joinConditions[0] || createEmptyDataQueryJoinCondition();
    const currentMain = String(currentJoinCondition.mainField || "").trim();
    const currentJoin = String(currentJoinCondition.joinField || "").trim();
    if (currentMain && currentJoin) return;
    const preferred = commonFields.find((fieldName) => /^(ID|OBJVERS|TRANID|IOBJNM|FIELDNM|TABNAME)$/i.test(fieldName)) || commonFields[0];
    currentJoinCondition.mainField = currentMain || preferred;
    currentJoinCondition.joinField = currentJoin || preferred;
    dataQueryState.joinConditions[0] = currentJoinCondition;
  }

  function buildDataQueryPayload(limitOverride) {
    sanitizeDataQuerySelectedFields();
    return {
      main_table: dataQueryState.mainTable,
      join_table: dataQueryState.joinTable,
      join_type: dataQueryState.joinTable ? dataQueryState.joinType : "left",
      select_fields: Array.isArray(dataQueryState.selectedFields)
        ? dataQueryState.selectedFields.map((fieldRef) => normalizeDataQueryFieldRef(fieldRef)).filter(Boolean)
        : [],
      main_join_field: String(dataQueryState.joinConditions?.[0]?.mainField || "").trim(),
      join_join_field: String(dataQueryState.joinConditions?.[0]?.joinField || "").trim(),
      join_conditions: (Array.isArray(dataQueryState.joinConditions) ? dataQueryState.joinConditions : [])
        .map((joinItem) => ({
          main_field: String(joinItem?.mainField || "").trim(),
          join_field: String(joinItem?.joinField || "").trim(),
        }))
        .filter((joinItem) => joinItem.main_field && joinItem.join_field),
      limit: Number.isFinite(limitOverride) ? limitOverride : normalizeDataQueryLimit(dataQueryState.limit),
      offset: 0,
      filters: (Array.isArray(dataQueryState.filters) ? dataQueryState.filters : []).map((filterItem) => ({
        field_ref: String(filterItem?.fieldRef || "").trim(),
        operator: String(filterItem?.operator || "in").trim().toLowerCase(),
        values: splitDataQueryValuesText(filterItem?.valuesText || ""),
        range_start: String(filterItem?.rangeStart || "").trim(),
        range_end: String(filterItem?.rangeEnd || "").trim(),
      })),
    };
  }

  const importSchemas = {};
  const importSchemaMeta = {};
  const importSchemaPromises = {};

  const logicManagedFields = {
    rstran: {
      SOURCESYS: "__LOGIC_SOURCENAME_SPLIT_LAST__",
      SOURCE: "__LOGIC_SOURCENAME_SPLIT_FIRST__"
    }
  };

  const logicRuleDesc = {
    rstran: "逻辑规则: SOURCENAME按空格拆分，前段->SOURCE，后段->SOURCESYS"
  };

  const bwObjectFixedSourceOptions = [
    "IOBJ InfoObject",
    "ADSO Data model",
    "ELEM Query",
    "HCPR Composite Provider",
    "RSDS Datasource",
    "TRCS Transfermation"
  ];
  const PATH_EXPORT_TEMPLATE_URL = "./Assets/Download%20template.xlsx";
  const PATH_EXPORT_TEMPLATE_SHEET = "Aligned Mapping";
  const PATH_EXPORT_BLOCK_WIDTH = 11;
  const PATH_EXPORT_DEFAULT_STEP_TITLE_ROW = 1;
  const PATH_EXPORT_DEFAULT_TRANSFORMATION_ROW = 2;
  const PATH_EXPORT_DEFAULT_ROUTINE_START_ROW = 8;
  const PATH_EXPORT_DEFAULT_HEADER_ROW = 13;
  const EXPORT_SAVE_PICKER_DISABLED_KEY = "df-export-save-picker-disabled";
  function esc(str) {
    return String(str)
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;");
  }

  function isSavePickerDisabled() {
    try {
      return String(window.localStorage.getItem(EXPORT_SAVE_PICKER_DISABLED_KEY) || "") === "1";
    } catch {
      return false;
    }
  }

  function setSavePickerDisabled(disabled) {
    try {
      if (disabled) {
        window.localStorage.setItem(EXPORT_SAVE_PICKER_DISABLED_KEY, "1");
      } else {
        window.localStorage.removeItem(EXPORT_SAVE_PICKER_DISABLED_KEY);
      }
    } catch {
      // Ignore storage failures in restrictive browser contexts.
    }
  }

  function escAttr(str) {
    return esc(str)
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");
  }

  function renderCopyIconButton(copyText, copyLabel, extraClass = "") {
    const normalizedText = String(copyText || "").trim();
    if (!normalizedText) return "";
    const normalizedLabel = String(copyLabel || "内容").trim() || "内容";
    const className = ["mapping-copy-btn", extraClass].filter(Boolean).join(" ");
    return `<button type="button" class="${className}" data-copy-text="${escAttr(normalizedText)}" data-copy-label="${escAttr(normalizedLabel)}" aria-label="复制${escAttr(normalizedLabel)}"><span class="mapping-copy-icon" aria-hidden="true"></span></button>`;
  }

  function getObjectTypeIconPath(objectType) {
    const normalized = String(objectType || "").trim().toUpperCase();
    const iconMap = {
      RSDS: "Assets/Icons/DataSource.png",
      ADSO: "Assets/Icons/ADSO.png",
      TRCS: "Assets/Icons/InfoSource.png",
      HRCP: "Assets/Icons/CompositeProvider.png",
      HCPR: "Assets/Icons/CompositeProvider.png",
      DEST: "Assets/Icons/OpenHub.png"
    };
    return iconMap[normalized] || "";
  }

  function renderObjectTypeIcon(objectType, objectName) {
    const iconPath = getObjectTypeIconPath(objectType);
    if (!iconPath) return "";
    const normalizedType = String(objectType || "").trim().toUpperCase() || "OBJECT";
    const normalizedName = String(objectName || "对象").trim() || "对象";
    return `<span class="mapping-object-type-icon-wrap" aria-hidden="true"><img class="mapping-object-type-icon" src="${escAttr(iconPath)}" alt="" title="${escAttr(`${normalizedType}: ${normalizedName}`)}" /></span>`;
  }

  function renderTranIdIcon() {
    return `<span class="mapping-tran-id-icon-wrap" aria-hidden="true"><img class="mapping-tran-id-icon" src="Assets/Icons/Transformation.png" alt="" /></span>`;
  }

  function renderContextCopyText(value, valueClassName, copyLabel = "字段名") {
    const normalizedValue = String(value || "").trim();
    if (!normalizedValue) return "";
    const normalizedLabel = String(copyLabel || "内容").trim() || "内容";
    return `<span class="${valueClassName} mapping-context-copy-value" data-copy-text="${escAttr(normalizedValue)}" data-copy-label="${escAttr(normalizedLabel)}" title="右键复制${escAttr(normalizedLabel)}">${esc(normalizedValue)}</span>`;
  }

  function normalizeLogicKind(value) {
    return String(value || "").trim().toUpperCase();
  }

  function formatLogicKindLabel(kind) {
    const normalized = normalizeLogicKind(kind);
    const labelMap = {
      START: "Start Routine",
      END: "End Routine",
      EXPERT: "Expert Routine",
      GLOBAL: "Global",
      FORMULA: "Formula",
      NORMAL: "Routine",
      ROUTINE: "Routine",
      CONSTANT: "Constant"
    };
    return labelMap[normalized] || normalized || "Logic";
  }

  function getLogicEntryContentRaw(entry) {
    return String(entry?.content_raw || "");
  }

  function getLogicEntryContentDisplay(entry) {
    return String(entry?.content_display ?? entry?.content_raw ?? "");
  }

  function getLogicEntryLanguage(entry) {
    const normalized = String(entry?.language || "plaintext").trim().toLowerCase();
    return normalized || "plaintext";
  }

  function getLogicEntryDisplayTitle(entry) {
    const title = String(entry?.title || "").trim();
    const kindLabel = formatLogicKindLabel(entry?.kind);
    if (!title) return kindLabel;
    if (title.toUpperCase() === normalizeLogicKind(entry?.kind)) return kindLabel;
    if (title === kindLabel) return kindLabel;
    return title;
  }

  function getLogicEntryNavEyebrow(entry) {
    return "";
  }

  function getLogicEntryTitle(entry) {
    return getLogicEntryDisplayTitle(entry);
  }

  function getLogicEntrySummary(entry) {
    const parts = [
      entry?.tran_id ? `TRANID: ${entry.tran_id}` : "",
      Number(entry?.rule_id || 0) ? `RULEID: ${entry.rule_id}` : "",
      Number(entry?.step_id || 0) ? `STEPID: ${entry.step_id}` : "",
      entry?.on_hana ? `ON_HANA: ${entry.on_hana}` : "",
      entry?.source_table ? String(entry.source_table).trim().toUpperCase() : ""
    ].filter(Boolean);
    return parts.join(" | ");
  }

  function getAceModeForLanguage(language) {
    const normalized = String(language || "plaintext").trim().toLowerCase();
    if (normalized === "abap") return "ace/mode/abap";
    if (normalized === "javascript" || normalized === "js") return "ace/mode/javascript";
    if (normalized === "json") return "ace/mode/json";
    if (normalized === "sql") return "ace/mode/sql";
    if (normalized === "xml") return "ace/mode/xml";
    return "ace/mode/text";
  }

  function ensureLogicViewerAceEditor() {
    if (logicViewerAceEditor) return logicViewerAceEditor;
    const aceApi = window.ace;
    const editorHost = document.getElementById("logicViewerEditor");
    if (!aceApi || !editorHost) return null;
    logicViewerAceEditor = aceApi.edit(editorHost, {
      readOnly: true,
      highlightActiveLine: true,
      highlightGutterLine: false,
      showPrintMargin: false,
      showFoldWidgets: true,
      showLineNumbers: true,
      showGutter: true,
      tabSize: 2,
      useSoftTabs: true,
      wrap: true,
      fontSize: 13,
      scrollPastEnd: 0.15,
      theme: "ace/theme/tomorrow_night_bright",
      mode: "ace/mode/text"
    });
    logicViewerAceEditor.setOptions({
      animatedScroll: true,
      behavioursEnabled: false,
      displayIndentGuides: true,
      dragEnabled: false,
      enableBasicAutocompletion: false,
      enableLiveAutocompletion: false,
      enableMultiselect: false,
      enableSnippets: false,
      highlightSelectedWord: true,
      newLineMode: "unix",
      showInvisibles: false,
      useWorker: false
    });
    logicViewerAceEditor.renderer.setPadding(16);
    logicViewerAceEditor.renderer.setScrollMargin(12, 12, 0, 0);
    logicViewerAceEditor.session.setUseWrapMode(true);
    logicViewerAceEditor.session.setNewLineMode("unix");
    logicViewerAceEditor.container.setAttribute("aria-label", "Transformation logic editor");
    logicViewerAceEditor.container.setAttribute("role", "textbox");
    logicViewerAceEditor.container.setAttribute("aria-readonly", "true");
    return logicViewerAceEditor;
  }

  function estimateLogicViewerModalGeometry(entries, defaultIndex = 0) {
    const viewportWidth = Math.max(640, window.innerWidth - 24);
    const viewportHeight = Math.max(420, window.innerHeight - 24);
    const normalizedEntries = Array.isArray(entries) ? entries.filter(Boolean) : [];
    const navWidth = normalizedEntries.length > 1 ? 84 : 76;
    const shellWidth = clampNumber(820, 700, viewportWidth);
    const shellHeight = clampNumber(470, 420, viewportHeight);

    return {
      navWidth,
      width: shellWidth,
      height: shellHeight
    };
  }

  function applyLogicViewerInitialGeometry(entries, defaultIndex = 0) {
    if (!logicViewerModalShell) return;
    const geometry = estimateLogicViewerModalGeometry(entries, defaultIndex);
    logicViewerModalShell.classList.remove("maximized");
    logicViewerModalShell.style.width = `${Math.round(geometry.width)}px`;
    logicViewerModalShell.style.height = `${Math.round(geometry.height)}px`;
    logicViewerModalShell.style.setProperty("--logic-viewer-nav-width", `${Math.round(geometry.navWidth)}px`);
    logicViewerModalShell.style.setProperty("--dialog-shell-dx", "0px");
    logicViewerModalShell.style.setProperty("--dialog-shell-dy", "0px");
    logicViewerModalShell.dataset.restoreWidth = logicViewerModalShell.style.width;
    logicViewerModalShell.dataset.restoreHeight = logicViewerModalShell.style.height;
  }

  function resizeLogicViewerEditor() {
    if (!logicViewerAceEditor) return;
    window.requestAnimationFrame(() => {
      logicViewerAceEditor.resize();
    });
  }

  function renderLogicEntryCode(entry) {
    const content = getLogicEntryContentDisplay(entry);
    const language = getLogicEntryLanguage(entry);
    logicViewerCode.className = `logic-viewer-code language-${escAttr(language)}`;
    logicViewerCode.textContent = content;
    const aceEditor = ensureLogicViewerAceEditor();
    if (aceEditor) {
      aceEditor.session.setMode(getAceModeForLanguage(language));
      aceEditor.setValue(content, -1);
      aceEditor.clearSelection();
      aceEditor.session.setScrollTop(0);
      aceEditor.session.setScrollLeft(0);
      logicViewerPre.classList.toggle("is-empty", !content);
      resizeLogicViewerEditor();
      return;
    }
    if (!content) {
      logicViewerCode.textContent = "";
      return;
    }
    logicViewerCode.textContent = content;
    if (window.hljs && typeof window.hljs.highlightElement === "function") {
      try {
        window.hljs.highlightElement(logicViewerCode);
      } catch {
        // Keep plain-text rendering if syntax highlight fails.
      }
    }
  }

  function renderLogicActionButton(label, attrs = {}, extraClass = "", title = "") {
    const normalizedLabel = String(label || "查看").trim() || "查看";
    const attrText = Object.entries(attrs)
      .map(([key, value]) => `${key}="${escAttr(String(value ?? ""))}"`)
      .join(" ");
    const className = ["mapping-logic-action-btn", extraClass].filter(Boolean).join(" ");
    const titleAttr = title ? ` title="${escAttr(title)}"` : "";
    return `<button type="button" class="${className}" ${attrText}${titleAttr}>${esc(normalizedLabel)}</button>`;
  }

  function getStepLogicTriggerLabel(segment) {
    const stepLogicGroups = Array.isArray(segment?.step_logic) ? segment.step_logic : [];
    const entries = stepLogicGroups.flatMap((group) => Array.isArray(group?.entries) ? group.entries : []);
    const kinds = new Set(entries.map((entry) => normalizeLogicKind(entry?.kind)).filter(Boolean));

    if (kinds.has("EXPERT")) return "Expert Routine";
    if (kinds.has("START") && kinds.has("END")) return "Start and End Routine";
    if (kinds.has("START")) return "Start Routine";
    if (kinds.has("END")) return "End Routine";
    if (kinds.has("GLOBAL")) return "Global Routine";
    return "No routine";
  }

  function renderStepLogicTrigger(segment) {
    const entryCount = Number(segment?.step_logic_entry_count || 0);
    const hasTranIds = Array.isArray(segment?.tran_ids) && segment.tran_ids.some((item) => String(item || "").trim());
    const canTryLazyLoad = !activePathMappingFeatures.logic && hasTranIds;
    const label = getStepLogicTriggerLabel(segment);
    const attrs = (entryCount > 0 || canTryLazyLoad)
      ? { "data-step-logic-open": String(segment?.index || "") }
      : { disabled: "disabled", "aria-disabled": "true" };
    const extraClass = (entryCount > 0 || canTryLazyLoad) ? "mapping-logic-action-btn-step" : "mapping-logic-action-btn-step is-disabled";
    const title = (entryCount > 0 || canTryLazyLoad) ? "查看转换级程序" : "当前 Step 没有例程";
    return renderLogicActionButton(label, attrs, extraClass, title);
  }

  function findSegmentByDisplayIndex(segmentDisplayIndex) {
    const normalizedSegments = Array.isArray(activePathMappingResult?.segments) ? activePathMappingResult.segments : [];
    return normalizedSegments.find((segment, index) => String(segment?.index || index + 1) === String(segmentDisplayIndex || "").trim()) || null;
  }

  function getRuleLogicEntriesForSegment(segmentIndex, tranId, ruleId, stepId) {
    const segments = Array.isArray(activePathMappingResult?.segments) ? activePathMappingResult.segments : [];
    const segment = segments[segmentIndex] || null;
    if (!segment) return [];
    const rows = Array.isArray(segment.rows) ? segment.rows : [];
    const matchedRow = rows.find((row) => (
      String(row?.tran_id || "").trim() === String(tranId || "").trim() &&
      Number(row?.rule_id || 0) === Number(ruleId || 0) &&
      Number(row?.step_id || 0) === Number(stepId || 0)
    ));
    return Array.isArray(matchedRow?.logic_entries) ? matchedRow.logic_entries : [];
  }

  function renderLogicViewer() {
    if (!logicViewerTitle || !logicViewerMeta || !logicViewerNav || !logicViewerEntryMeta || !logicViewerCode || !logicViewerPre || !copyLogicViewerBtn) return;

    logicViewerTitle.textContent = String(activeLogicViewerContext?.title || "转换逻辑查看").trim() || "转换逻辑查看";
    logicViewerMeta.textContent = String(activeLogicViewerContext?.meta || "").trim();

    if (!activeLogicViewerEntries.length) {
      logicViewerNav.innerHTML = '<div class="logic-viewer-nav-empty">没有可显示的程序、公式或常量。</div>';
      logicViewerEntryMeta.textContent = "";
      logicViewerCode.className = "logic-viewer-code language-plaintext";
      logicViewerCode.textContent = "";
      const aceEditor = ensureLogicViewerAceEditor();
      if (aceEditor) {
        aceEditor.session.setMode("ace/mode/text");
        aceEditor.setValue("", -1);
        aceEditor.clearSelection();
        resizeLogicViewerEditor();
      }
      copyLogicViewerBtn.disabled = true;
      return;
    }

    activeLogicViewerIndex = Math.max(0, Math.min(activeLogicViewerIndex, activeLogicViewerEntries.length - 1));
    const activeEntry = activeLogicViewerEntries[activeLogicViewerIndex] || null;

    logicViewerNav.innerHTML = activeLogicViewerEntries.map((entry, index) => {
      const isActive = index === activeLogicViewerIndex;
      const contentLength = getLogicEntryContentRaw(entry).length;
      const eyebrow = getLogicEntryNavEyebrow(entry);
      const title = getLogicEntryDisplayTitle(entry);
      return `
        <button
          type="button"
          class="logic-viewer-nav-item${isActive ? " is-active" : ""}"
          data-logic-viewer-index="${index}"
          title="${escAttr(getLogicEntryTitle(entry))}"
        >
          ${eyebrow ? `<span class="logic-viewer-nav-kind">${esc(eyebrow)}</span>` : ""}
          <span class="logic-viewer-nav-title">${esc(title)}</span>
          <span class="logic-viewer-nav-meta">${esc(contentLength ? `${formatCount(contentLength)} chars` : "空内容")}</span>
        </button>
      `;
    }).join("");

    logicViewerEntryMeta.innerHTML = "";

    renderLogicEntryCode(activeEntry);
    copyLogicViewerBtn.disabled = !getLogicEntryContentRaw(activeEntry);
  }

  async function openLogicViewer(entries, context = {}, defaultIndex = 0) {
    activeLogicViewerEntries = Array.isArray(entries) ? entries.filter(Boolean) : [];
    activeLogicViewerIndex = Math.max(0, Number(defaultIndex) || 0);
    activeLogicViewerContext = context || {};
    applyLogicViewerInitialGeometry(activeLogicViewerEntries, activeLogicViewerIndex);
    showBlockingModal(logicViewerModal);
    try {
      await ensureAceAssetsLoaded();
    } catch {
      showToast("代码查看器依赖加载失败，已回退到纯文本显示。", "warning");
    }
    renderLogicViewer();
    logicViewerModalShell?.focus();
    resizeLogicViewerEditor();
  }

  function closeLogicViewer() {
    activeLogicViewerEntries = [];
    activeLogicViewerIndex = 0;
    activeLogicViewerContext = null;
    const aceEditor = ensureLogicViewerAceEditor();
    if (aceEditor) {
      aceEditor.setValue("", -1);
      aceEditor.clearSelection();
    }
    if (logicViewerModalShell) {
      logicViewerModalShell.classList.remove("maximized");
      logicViewerModalShell.style.removeProperty("--dialog-shell-dx");
      logicViewerModalShell.style.removeProperty("--dialog-shell-dy");
    }
    hideBlockingModal(logicViewerModal);
  }

  async function openSegmentLogicViewer(segmentDisplayIndex) {
    const segment = findSegmentByDisplayIndex(segmentDisplayIndex);
    if (!segment) {
      showToast("未找到当前 Step 的程序内容。", "error");
      return;
    }
    let lazyLogicLoaded = false;
    let stepLogicGroups = Array.isArray(segment.step_logic) ? segment.step_logic : [];
    let entries = stepLogicGroups.flatMap((group) => Array.isArray(group?.entries) ? group.entries : []);
    if (!entries.length && activePathMappingResult && !activePathMappingFeatures.logic) {
      try {
        await ensurePathMappingFeatures({ logic: true }, "正在加载 Step 程序详情...");
        lazyLogicLoaded = true;
      } catch (error) {
        const rawMsg = String(error?.message || "").trim();
        showToast(`加载 Step 程序失败。${rawMsg ? ` 详情: ${rawMsg}` : ""}`, "error");
        return;
      }
      if (lazyLogicLoaded && activePathMappingResult) {
        renderAlignedPathMapping(activePathMappingResult);
      }
      const refreshedSegment = findSegmentByDisplayIndex(segmentDisplayIndex);
      stepLogicGroups = Array.isArray(refreshedSegment?.step_logic) ? refreshedSegment.step_logic : [];
      entries = stepLogicGroups.flatMap((group) => Array.isArray(group?.entries) ? group.entries : []);
    }
    if (!entries.length) {
      showToast("当前 Step 没有转换级程序。", "error");
      return;
    }
    await openLogicViewer(entries, {
      title: `Step ${segment.index || segmentDisplayIndex} 转换级程序`,
      meta: `${segment.source || "--"} -> ${segment.target || "--"} | TRANID: ${(Array.isArray(segment.tran_ids) ? segment.tran_ids.join(", ") : "--") || "--"}`
    });
  }

  async function openRuleLogicViewer(segmentIndex, tranId, ruleId, stepId) {
    const segments = Array.isArray(activePathMappingResult?.segments) ? activePathMappingResult.segments : [];
    const segment = segments[segmentIndex] || null;
    if (!segment) {
      showToast("未找到当前规则的内容。", "error");
      return;
    }
    let lazyLogicLoaded = false;
    let entries = getRuleLogicEntriesForSegment(segmentIndex, tranId, ruleId, stepId);
    if (!entries.length && activePathMappingResult && !activePathMappingFeatures.logic) {
      try {
        await ensurePathMappingFeatures({ logic: true }, "正在加载规则逻辑详情...");
        lazyLogicLoaded = true;
      } catch (error) {
        const rawMsg = String(error?.message || "").trim();
        showToast(`加载规则逻辑失败。${rawMsg ? ` 详情: ${rawMsg}` : ""}`, "error");
        return;
      }
      if (lazyLogicLoaded && activePathMappingResult) {
        renderAlignedPathMapping(activePathMappingResult);
      }
      entries = getRuleLogicEntriesForSegment(segmentIndex, tranId, ruleId, stepId);
    }
    if (!entries.length) {
      showToast("当前规则没有公式、程序或常量内容。", "error");
      return;
    }
    const rows = Array.isArray(segment.rows) ? segment.rows : [];
    const matchedRow = rows.find((row) => (
      String(row?.tran_id || "").trim() === String(tranId || "").trim() &&
      Number(row?.rule_id || 0) === Number(ruleId || 0) &&
      Number(row?.step_id || 0) === Number(stepId || 0)
    ));
    const fieldLabel = [String(matchedRow?.source_field || "").trim(), String(matchedRow?.target_field || "").trim()].filter(Boolean).join(" -> ");
    await openLogicViewer(entries, {
      title: `Step ${segment.index || segmentIndex + 1} | Rule ${ruleId}${fieldLabel ? ` | ${fieldLabel}` : ""}`,
      meta: `${segment.source || "--"} -> ${segment.target || "--"} | ${fieldLabel || "无字段映射标签"}`
    });
  }

  function markKeyword(text, keyword) {
    const t = String(text);
    if (!keyword) return esc(t);
    const idx = t.toLowerCase().indexOf(keyword.toLowerCase());
    if (idx < 0) return esc(t);
    const a = esc(t.slice(0, idx));
    const b = esc(t.slice(idx, idx + keyword.length));
    const c = esc(t.slice(idx + keyword.length));
    return `${a}<span class="mark">${b}</span>${c}`;
  }

  function formatCount(value) {
    const count = Number(value ?? 0);
    if (!Number.isFinite(count)) return "0";
    return count.toLocaleString("en-US");
  }

  function parseDisplayedCount(value) {
    const digits = String(value || "").replace(/[^\d.-]/g, "");
    const parsed = Number(digits || 0);
    return Number.isFinite(parsed) ? parsed : 0;
  }

  function getImportTableDisplayName(tableName) {
    const normalized = String(tableName || "").trim().toLowerCase();
    const card = importCardElements[normalized];
    const rawTitle = String(card?.dataset.title || "").trim();
    if (rawTitle) {
      const match = rawTitle.match(/（([A-Z0-9_]+)\)?$/i) || rawTitle.match(/\(([A-Z0-9_]+)\)?$/i);
      return match?.[1] || rawTitle;
    }
    return normalized.toUpperCase();
  }

  function getImportTableCount(statusPayload, tableName) {
    const normalized = String(tableName || "").trim().toLowerCase();
    const statusCount = Number(statusPayload?.[normalized]?.last_count);
    if (Number.isFinite(statusCount)) return statusCount;
    const fallbackText = importStatusElements[normalized]?.count?.textContent || "0";
    return parseDisplayedCount(fallbackText);
  }

  function applyImportStatusPayload(payload) {
    latestImportStatusPayload = payload && typeof payload === "object" ? payload : {};

    Object.entries(importStatusElements).forEach(([tableName, refs]) => {
      const time = latestImportStatusPayload?.[tableName]?.last_update || "--";
      const count = getImportTableCount(latestImportStatusPayload, tableName);
      if (refs.time) refs.time.textContent = time;
      if (refs.count) refs.count.textContent = formatCount(count);
      const card = importCardElements[tableName];
      if (card) {
        const hasImportedData = count > 0;
        card.classList.toggle("is-imported", hasImportedData);
        card.classList.toggle("is-empty", !hasImportedData);
        card.dataset.importState = hasImportedData ? "imported" : "empty";
        card.dataset.importStateLabel = hasImportedData ? "已导入" : "未导入";
      }
    });
  }

  async function refreshImportStatusSnapshot() {
    const payload = await fetchImportStatus();
    applyImportStatusPayload(payload);
    return latestImportStatusPayload;
  }

  async function checkRebuildDependencies(configKey) {
    const config = rebuildDependencyConfigs[configKey];
    if (!config) return true;

    let statusPayload = latestImportStatusPayload;
    try {
      statusPayload = await refreshImportStatusSnapshot();
    } catch {
      statusPayload = latestImportStatusPayload;
    }

    const missingRequired = (config.requiredTables || []).filter((tableName) => getImportTableCount(statusPayload, tableName) <= 0);
    if (missingRequired.length) {
      showToast(
        `${config.actionLabel}前缺少前提表数据，请先导入：${missingRequired.map(getImportTableDisplayName).join("、")}。`,
        "error",
        {
          title: "缺少前提表",
          blocking: true,
          actions: true,
          requireClose: true,
          showCopy: false
        }
      );
      return false;
    }

    const missingOptional = (config.optionalTables || []).filter((tableName) => getImportTableCount(statusPayload, tableName) <= 0);
    if (!missingOptional.length) {
      return true;
    }

    const decision = await showToastAndWait(
      `${config.actionLabel}可继续执行，但以下补充表当前为空：${missingOptional.map(getImportTableDisplayName).join("、")}。继续后结果可能不完整，是否继续？`,
      "warning",
      {
        title: "补充表为空",
        primaryLabel: "继续执行",
        primaryValue: "continue",
        secondaryLabel: "取消",
        secondaryValue: "cancel",
        dismissValue: "cancel",
        showCopy: false
      }
    );
    return decision === "continue";
  }

  function setPathSelectionCollapsed(collapsed) {
    const nextCollapsed = !!collapsed;
    isPathSelectionCollapsed = nextCollapsed;
    if (pathSelectionPanel) {
      pathSelectionPanel.classList.toggle("is-collapsed", nextCollapsed);
    }
    if (pathSelectionStage) {
      pathSelectionStage.classList.toggle("hidden", nextCollapsed);
    }
    if (togglePathSelectionBtn) {
      togglePathSelectionBtn.classList.toggle("is-collapsed", nextCollapsed);
      togglePathSelectionBtn.setAttribute("aria-expanded", nextCollapsed ? "false" : "true");
      togglePathSelectionBtn.setAttribute("title", nextCollapsed ? "展开 Path Selection" : "收起 Path Selection");
      togglePathSelectionBtn.setAttribute("aria-label", nextCollapsed ? "展开 Path Selection" : "收起 Path Selection");
    }
  }

  function setAutoCollapsePathSelection(enabled) {
    autoCollapsePathSelection = enabled !== false;
    if (autoCollapsePathBtn) {
      autoCollapsePathBtn.classList.toggle("is-on", autoCollapsePathSelection);
      autoCollapsePathBtn.classList.toggle("is-off", !autoCollapsePathSelection);
      autoCollapsePathBtn.setAttribute("aria-pressed", autoCollapsePathSelection ? "true" : "false");
      autoCollapsePathBtn.setAttribute("title", autoCollapsePathSelection ? "已开启自动收起" : "已关闭自动收起");
      const stateEl = autoCollapsePathBtn.querySelector(".path-auto-collapse-state");
      if (stateEl) {
        stateEl.textContent = autoCollapsePathSelection ? "ON" : "OFF";
      }
    }
  }

  function hideToast(result = null) {
    if (!appToast) return;
    appToast.classList.add("hidden");
    appToast.classList.remove("is-blocking");
    appToast.classList.remove("error");
    appToast.classList.remove("warning");
    appToast.classList.remove("with-actions");
    appToast.setAttribute("aria-hidden", "true");
    appToast.setAttribute("aria-modal", "false");
    if (appToastTitle) {
      if (appToastTitleText) {
        appToastTitleText.textContent = "";
      }
      appToastTitle.classList.add("hidden");
    }
    toastIsBlocking = false;
    if (typeof toastCloseResolver === "function") {
      const resolver = toastCloseResolver;
      toastCloseResolver = null;
      resolver(result ?? toastDismissValue);
    }
    toastDismissValue = "close";
    if (toastTimer) {
      clearTimeout(toastTimer);
      toastTimer = null;
    }
  }

  function focusToastPrimaryAction() {
    if (appToastPrimaryBtn && !appToastPrimaryBtn.classList.contains("hidden")) {
      appToastPrimaryBtn.focus();
      return;
    }
    if (appToastCloseBtn) {
      appToastCloseBtn.focus();
      return;
    }
    if (appToastCard) {
      appToastCard.focus();
    }
  }

  function showToast(message, variant = "success", options = {}) {
    if (!appToast) return;
    if (toastTimer) {
      clearTimeout(toastTimer);
      toastTimer = null;
    }
    if (!options.preserveResolver) {
      toastCloseResolver = null;
    }

    const text = String(message || "").trim();
    const title = String(options.title || "").trim();
    lastToastMessage = text;
    const isBlocking = Boolean(options.blocking);
    const requireClose = options.requireClose !== false && isBlocking;
    const primaryLabel = String(options.primaryLabel || "").trim();
    const secondaryLabel = String(options.secondaryLabel || "").trim();
    const showDecisionActions = Boolean(primaryLabel || secondaryLabel);
    const showActions = Boolean(options.actions) || requireClose || variant === "error" || showDecisionActions;
    const showCopy = options.showCopy !== false;
    const showClose = options.showClose !== false && !showDecisionActions;
    toastDismissValue = String(options.dismissValue || options.closeValue || (showDecisionActions ? "cancel" : "close"));

    appToast.classList.remove("error");
    appToast.classList.remove("warning");
    appToast.classList.remove("with-actions");
    if (variant === "error") {
      appToast.classList.add("error");
    } else if (variant === "warning") {
      appToast.classList.add("warning");
    }
    if (appToastText) {
      appToastText.textContent = text;
    } else {
      appToast.textContent = text;
    }
    if (appToastTitle) {
      if (appToastTitleText) {
        appToastTitleText.textContent = title;
      }
      appToastTitle.classList.toggle("hidden", !title);
    }
    if (appToastActions) {
      appToastActions.classList.toggle("hidden", !showActions);
    }
    if (appToastCopyBtn) {
      appToastCopyBtn.textContent = "复制";
      appToastCopyBtn.classList.toggle("hidden", !showActions || !showCopy);
    }
    if (appToastSecondaryBtn) {
      appToastSecondaryBtn.textContent = secondaryLabel || "取消";
      appToastSecondaryBtn.dataset.toastResult = String(options.secondaryValue || "secondary");
      appToastSecondaryBtn.classList.toggle("hidden", !secondaryLabel);
    }
    if (appToastPrimaryBtn) {
      appToastPrimaryBtn.textContent = primaryLabel || "继续";
      appToastPrimaryBtn.dataset.toastResult = String(options.primaryValue || "primary");
      appToastPrimaryBtn.classList.toggle("hidden", !primaryLabel);
    }
    if (appToastCloseBtn) {
      appToastCloseBtn.textContent = String(options.closeLabel || "关闭");
      appToastCloseBtn.classList.toggle("hidden", !showActions || !showClose);
      appToastCloseBtn.dataset.toastResult = toastDismissValue;
    }
    appToast.classList.toggle("with-actions", showActions);
    appToast.classList.toggle("is-blocking", isBlocking);
    appToast.setAttribute("aria-hidden", "false");
    appToast.setAttribute("aria-modal", isBlocking ? "true" : "false");
    toastIsBlocking = isBlocking;
    appToast.classList.remove("hidden");

    const holdMs = Number(options.holdMs);
    const resolvedHoldMs = Number.isFinite(holdMs)
      ? holdMs
      : variant === "error"
        ? 20000
        : variant === "warning"
          ? 9600
          : 5200;
    if (!requireClose && resolvedHoldMs > 0) {
      toastTimer = window.setTimeout(() => {
        hideToast();
      }, resolvedHoldMs);
    }

    if (isBlocking) {
      window.setTimeout(() => {
        focusToastPrimaryAction();
      }, 0);
    }
  }

  function showToastAndWait(message, variant = "success", options = {}) {
    return new Promise((resolve) => {
      toastCloseResolver = resolve;
      showToast(message, variant, {
        ...options,
        preserveResolver: true,
        blocking: true,
        requireClose: true,
        actions: true
      });
    });
  }

  function ensureModalLoadingOverlay(modal) {
    if (!modal) return null;
    let overlay = modal.querySelector(".modal-loading");
    if (overlay) return overlay;

    overlay = document.createElement("div");
    overlay.className = "modal-loading hidden";
    overlay.innerHTML = `
      <div class="modal-loading-card">
        <div class="modal-loading-spinner" aria-hidden="true"></div>
        <div class="modal-loading-text">处理中...</div>
      </div>
    `;
    modal.appendChild(overlay);
    return overlay;
  }

  function setModalLoading(modal, isBusy, text = "处理中...") {
    const overlay = ensureModalLoadingOverlay(modal);
    if (!overlay) return;
    const textNode = overlay.querySelector(".modal-loading-text");
    if (textNode) {
      textNode.textContent = text;
    }
    overlay.classList.toggle("hidden", !isBusy);
  }

  async function withModalLoading(modal, text, runner) {
    setModalLoading(modal, true, text);
    try {
      return await runner();
    } finally {
      setModalLoading(modal, false);
    }
  }

  function setAppLoading(isBusy, text = "处理中...") {
    if (!appLoadingOverlay) return;
    if (appLoadingText) {
      appLoadingText.textContent = String(text || "处理中...").trim() || "处理中...";
    }
    if (isBusy) {
      syncAppLoadingZIndex();
    }
    appLoadingOverlay.classList.toggle("hidden", !isBusy);
    appLoadingOverlay.setAttribute("aria-hidden", isBusy ? "false" : "true");
    document.body.classList.toggle("has-app-loading", !!isBusy);
    if (!isBusy) {
      appLoadingOverlay.style.removeProperty("z-index");
    }
  }

  function waitForNextPaint() {
    return new Promise((resolve) => {
      window.requestAnimationFrame(() => {
        window.requestAnimationFrame(resolve);
      });
    });
  }

  async function withAppLoading(text, runner) {
    appLoadingDepth += 1;
    setAppLoading(true, text);
    await waitForNextPaint();
    try {
      return await runner();
    } finally {
      appLoadingDepth = Math.max(0, appLoadingDepth - 1);
      if (appLoadingDepth === 0) {
        setAppLoading(false);
      }
    }
  }

  function setupDialogDragAndResize(modal, shell) {
    if (!modal || !shell) return;
    const head = shell.querySelector(".import-head");
    if (!head) return;

    shell.classList.add("dialog-shell");

    let resizeHandle = shell.querySelector(".dialog-resize-handle");
    if (!resizeHandle) {
      resizeHandle = document.createElement("div");
      resizeHandle.className = "dialog-resize-handle";
      resizeHandle.title = "拖动调整窗口大小";
      resizeHandle.setAttribute("aria-hidden", "true");
      shell.appendChild(resizeHandle);
    }

    let dragStartX = 0;
    let dragStartY = 0;
    let startDx = 0;
    let startDy = 0;
    const viewportMargin = 12;
    const minVisibleBottom = 176;

    const getOffset = (name) => {
      const raw = shell.style.getPropertyValue(name).trim();
      const parsed = Number.parseFloat(raw || "0");
      return Number.isFinite(parsed) ? parsed : 0;
    };

    const clampOffset = (dx, dy, rect = shell.getBoundingClientRect()) => {
      const baseLeft = (window.innerWidth - rect.width) / 2;
      const baseTop = (window.innerHeight - rect.height) / 2;
      const minDx = Math.min(0, viewportMargin - baseLeft);
      const maxDx = Math.max(0, window.innerWidth - viewportMargin - (baseLeft + rect.width));
      const minDy = Math.min(0, viewportMargin - baseTop);
      const maxDy = Math.max(0, window.innerHeight - minVisibleBottom - baseTop);
      return {
        dx: Math.max(minDx, Math.min(maxDx, dx)),
        dy: Math.max(minDy, Math.min(maxDy, dy))
      };
    };

    const applyClampedOffset = (dx, dy, rect) => {
      const clamped = clampOffset(dx, dy, rect);
      shell.style.setProperty("--dialog-shell-dx", `${clamped.dx}px`);
      shell.style.setProperty("--dialog-shell-dy", `${clamped.dy}px`);
    };

    const keepShellInViewport = (rect = shell.getBoundingClientRect()) => {
      if (shell.classList.contains("maximized")) return;
      applyClampedOffset(getOffset("--dialog-shell-dx"), getOffset("--dialog-shell-dy"), rect);
    };

    const onDragMove = (event) => {
      const nextDx = startDx + (event.clientX - dragStartX);
      const nextDy = startDy + (event.clientY - dragStartY);
      applyClampedOffset(nextDx, nextDy);
    };

    const onDragUp = () => {
      shell.classList.remove("dragging");
      window.removeEventListener("pointermove", onDragMove);
      window.removeEventListener("pointerup", onDragUp);
    };

    head.addEventListener("pointerdown", (event) => {
      const blockDrag = event.target.closest("button, input, select, .window-controls");
      if (blockDrag || shell.classList.contains("maximized")) return;

      dragStartX = event.clientX;
      dragStartY = event.clientY;
      startDx = getOffset("--dialog-shell-dx");
      startDy = getOffset("--dialog-shell-dy");

      shell.classList.add("dragging");
      window.addEventListener("pointermove", onDragMove);
      window.addEventListener("pointerup", onDragUp);
    });

    const minWidth = 520;
    const minHeight = 260;
    let resizeStartX = 0;
    let resizeStartY = 0;
    let resizeStartW = 0;
    let resizeStartH = 0;

    const onResizeMove = (event) => {
      const maxWidth = window.innerWidth - 32;
      const maxHeight = window.innerHeight - 32;
      const width = Math.max(minWidth, Math.min(maxWidth, resizeStartW + (event.clientX - resizeStartX)));
      const height = Math.max(minHeight, Math.min(maxHeight, resizeStartH + (event.clientY - resizeStartY)));
      shell.style.width = `${width}px`;
      shell.style.height = `${height}px`;
      shell.dataset.restoreWidth = shell.style.width;
      shell.dataset.restoreHeight = shell.style.height;
      keepShellInViewport();
    };

    const onResizeUp = () => {
      shell.classList.remove("resizing");
      window.removeEventListener("pointermove", onResizeMove);
      window.removeEventListener("pointerup", onResizeUp);
    };

    resizeHandle.addEventListener("pointerdown", (event) => {
      if (shell.classList.contains("maximized")) return;
      event.preventDefault();
      const rect = shell.getBoundingClientRect();
      resizeStartX = event.clientX;
      resizeStartY = event.clientY;
      resizeStartW = rect.width;
      resizeStartH = rect.height;

      shell.classList.add("resizing");
      window.addEventListener("pointermove", onResizeMove);
      window.addEventListener("pointerup", onResizeUp);
    });

    modal.addEventListener("pointerdown", () => bringBlockingModalToFront(modal));
    window.addEventListener("resize", keepShellInViewport);
  }

  function clearImportProgressTimer() {
    if (importProgressTimer) {
      window.clearInterval(importProgressTimer);
      importProgressTimer = null;
    }
  }

  function ensureWorkbenchCardBusyOverlay(card) {
    if (!card) return null;
    let overlay = card.querySelector(".import-card-busy-overlay");
    if (overlay) return overlay;

    overlay = document.createElement("span");
    overlay.className = "import-card-busy-overlay";
    overlay.setAttribute("aria-hidden", "true");
    overlay.innerHTML = `
      <span class="import-card-busy-spinner"></span>
      <span class="import-card-busy-text">处理中...</span>
    `;
    card.appendChild(overlay);
    return overlay;
  }

  function setWorkbenchCardSelected(card) {
    if (activeWorkbenchCard && activeWorkbenchCard !== card) {
      activeWorkbenchCard.classList.remove("is-selected");
    }
    activeWorkbenchCard = card || null;
    if (activeWorkbenchCard) {
      activeWorkbenchCard.classList.add("is-selected");
    }
  }

  function setWorkbenchCardLoading(card, isBusy, label = "处理中...") {
    if (!card) return;
    const overlay = ensureWorkbenchCardBusyOverlay(card);
    const textNode = overlay?.querySelector(".import-card-busy-text");
    if (textNode) {
      textNode.textContent = String(label || "处理中...").trim() || "处理中...";
    }
    card.classList.toggle("is-loading", !!isBusy);
    card.setAttribute("aria-busy", isBusy ? "true" : "false");
  }

  async function withWorkbenchCardLoading(card, label, runner) {
    if (!card) {
      return runner();
    }
    setWorkbenchCardLoading(card, true, label);
    await waitForNextPaint();
    try {
      return await runner();
    } finally {
      setWorkbenchCardLoading(card, false);
    }
  }

  function getImportCardElement(tableName) {
    return importCardElements[String(tableName || "").trim().toLowerCase()] || null;
  }

  function setAllImportCardsLoading(isBusy, label = "正在刷新导入状态...") {
    importCards.forEach((card) => {
      setWorkbenchCardLoading(card, isBusy, label);
    });
  }

  async function withImportCardsRefreshLoading(label, runner) {
    importCardRefreshDepth += 1;
    setAllImportCardsLoading(true, label);
    await waitForNextPaint();
    try {
      return await runner();
    } finally {
      importCardRefreshDepth = Math.max(0, importCardRefreshDepth - 1);
      if (importCardRefreshDepth === 0) {
        setAllImportCardsLoading(false);
      }
    }
  }

  function resetImportProgress() {
    clearImportProgressTimer();
    importProgressValue = 0;
    if (importProgressBar) {
      importProgressBar.style.width = "0%";
      const track = importProgressBar.parentElement;
      if (track) track.setAttribute("aria-valuenow", "0");
    }
    if (importProgressText) importProgressText.textContent = "处理中...";
    if (importProgressWrap) importProgressWrap.classList.add("hidden");
  }

  function setImportBusyState(isBusy, label = "处理中...") {
    setWorkbenchCardLoading(getImportCardElement(activeImportTable), isBusy, label);
    if (importModalShell) {
      importModalShell.classList.toggle("is-busy", isBusy);
    }
    [importFileInput, importSheetSelect, importHeaderRowSelect, autoMapBtn, clearImportTableBtn, confirmImportBtn].forEach((el) => {
      if (!el) return;
      el.disabled = isBusy;
    });

    if (!isBusy) {
      resetImportProgress();
      return;
    }

    if (importProgressWrap) importProgressWrap.classList.remove("hidden");
    if (importProgressText) importProgressText.textContent = label;

    clearImportProgressTimer();
    importProgressValue = 8;
    if (importProgressBar) {
      importProgressBar.style.width = `${importProgressValue}%`;
      const track = importProgressBar.parentElement;
      if (track) track.setAttribute("aria-valuenow", String(Math.round(importProgressValue)));
    }

    importProgressTimer = window.setInterval(() => {
      if (!importProgressBar) return;
      const inc = 2 + Math.random() * 6;
      importProgressValue = Math.min(92, importProgressValue + inc);
      importProgressBar.style.width = `${importProgressValue}%`;
      const track = importProgressBar.parentElement;
      if (track) track.setAttribute("aria-valuenow", String(Math.round(importProgressValue)));
    }, 220);
  }

  function completeImportBusyState() {
    clearImportProgressTimer();
    if (importProgressBar) {
      importProgressValue = 100;
      importProgressBar.style.width = "100%";
      const track = importProgressBar.parentElement;
      if (track) track.setAttribute("aria-valuenow", "100");
    }

    window.setTimeout(() => {
      setImportBusyState(false);
    }, 260);
  }

  function setImportProgressValue(value, label = "处理中...") {
    const normalized = Math.max(0, Math.min(100, Number(value) || 0));
    clearImportProgressTimer();
    if (importProgressWrap) importProgressWrap.classList.remove("hidden");
    if (importProgressText) importProgressText.textContent = label;
    if (importProgressBar) {
      importProgressBar.style.width = `${normalized}%`;
      const track = importProgressBar.parentElement;
      if (track) track.setAttribute("aria-valuenow", String(Math.round(normalized)));
    }
  }

  function saveHomeState() {
    const payload = {
      pathSource: String(startModelInput?.value || "").trim(),
      pathSourceSystem: String(startSourceSystemInput?.value || "").trim(),
      pathTarget: String(endModelInput?.value || "").trim(),
      pathTranId: String(tranIdInput?.value || "").trim(),
      autoCollapsePathSelection
    };
    localStorage.setItem(HOME_STATE_KEY, JSON.stringify(payload));
  }

  function getFieldMappingRebuildMessage(scope = "general") {
    if (scope === "path") {
      return "路径字段映射功能已暂时冻结，准备按新方案重构。";
    }
    if (scope === "import") {
      return "导入字段映射功能已暂时冻结，准备按新方案重构。";
    }
    return "字段映射功能已暂时冻结，准备按新方案重构。";
  }

  function applyFieldMappingRebuildModeUi() {
    if (PATH_MAPPING_REBUILD_MODE && pathSearchBtn) {
      pathSearchBtn.textContent = "功能重构中";
      pathSearchBtn.title = getFieldMappingRebuildMessage("path");
    }

    if (PATH_MAPPING_REBUILD_MODE) {
      pathResultActionButtons.forEach((button) => {
        button.disabled = true;
        button.title = getFieldMappingRebuildMessage("path");
      });
    }

    if (IMPORT_MAPPING_REBUILD_MODE) {
      importCards.forEach((card) => {
        card.title = getFieldMappingRebuildMessage("import");
      });
    }
  }

  function getCurrentImportMappingStats() {
    if (!activeImportTable) {
      return {
        total: 0,
        mapped: 0,
        unmapped: 0,
        isComplete: true
      };
    }

    const visibleDbFields = getVisibleDbFields(activeImportTable);
    const total = visibleDbFields.length;
    const mapping = readMappingFromUI();
    const mapped = visibleDbFields.reduce((count, dbField) => {
      const value = String(mapping[dbField] || "").trim();
      if (!value || value.startsWith("__LOGIC_")) return count;
      return count + 1;
    }, 0);
    const unmapped = Math.max(0, total - mapped);

    return {
      total,
      mapped,
      unmapped,
      isComplete: unmapped === 0
    };
  }

  function getImportDisplayTitle(tableName) {
    const normalized = String(tableName || "").trim().toLowerCase();
    const matchedCard = importCards.find((card) => String(card.dataset.table || "").trim().toLowerCase() === normalized);
    return matchedCard?.dataset.title || String(tableName || "").trim().toUpperCase() || "--";
  }

  function formatImportFailureReason(rawMsg) {
    const message = String(rawMsg || "").trim();
    if (!message) {
      return `请确认后端服务已启动（${importStatusApiBase}）。`;
    }
    if (/no rows to import/i.test(message)) {
      return "文件未读取到可导入数据行。请确认有表头并至少包含1行数据。";
    }
    if (/未读取到可导入数据行/.test(message)) {
      return message;
    }
    if (/request_timeout/i.test(message)) {
      return "导入超时：数据量较大或网络较慢，请稍后重试或拆分文件后再导入。";
    }
    if (/internal server error|status 500/i.test(message)) {
      return "后端服务内部错误，请稍后重试；若持续失败，请查看服务日志。";
    }
    return message;
  }

  function formatImportSuccessToast(result, stats) {
    const tableLabel = getImportDisplayTitle(result?.table_name || activeImportTable);
    const affectedRows = Number(result?.affected_rows ?? 0);
    const insertedRows = Number(result?.inserted_rows ?? 0);
    const updatedRows = Number(result?.updated_rows ?? 0);
    const totalRows = Number(result?.db_count ?? result?.last_count ?? 0);
    let base = `本次导入成功。表：${tableLabel}。处理条目：${formatCount(affectedRows)}，新增：${formatCount(insertedRows)}，更新：${formatCount(updatedRows)}，当前总条数：${formatCount(totalRows)}。`;
    if (affectedRows > 0 && insertedRows === 0 && updatedRows > 0) {
      base += " 本次没有新增数据，只更新了已存在主键的记录。";
    }
    if (stats?.unmapped > 0) {
      return `${base}注意：仍有 ${formatCount(stats.unmapped)} 个字段未映射，未映射字段已按空值导入。`;
    }
    return base;
  }

  function formatImportFailureToast(tableName, reason) {
    const tableLabel = getImportDisplayTitle(tableName);
    return `本次导入失败。表：${tableLabel}。导入条目：0。失败原因：${reason}`;
  }

  function sanitizePathInputHistoryItems(items) {
    const result = [];
    const seen = new Set();
    (Array.isArray(items) ? items : []).forEach((item) => {
      const normalized = normalizePathToken(item);
      if (!normalized || seen.has(normalized)) return;
      seen.add(normalized);
      result.push(normalized);
    });
    return result.slice(0, PATH_INPUT_HISTORY_MAX);
  }

  function savePathInputHistory() {
    try {
      window.localStorage.setItem(PATH_INPUT_HISTORY_KEY, JSON.stringify(pathInputHistory));
    } catch {
      // Ignore storage failures in restrictive browser contexts.
    }
  }

  function renderPathInputHistoryLists() {
    const mappings = [
      [startModelHistoryList, pathInputHistory.source],
      [startSourceSystemHistoryList, pathInputHistory.sourceSystem],
      [endModelHistoryList, pathInputHistory.target],
      [tranIdHistoryList, pathInputHistory.tranId]
    ];

    mappings.forEach(([listEl, items]) => {
      if (!listEl) return;
      listEl.innerHTML = sanitizePathInputHistoryItems(items)
        .map((item) => `<option value="${escAttr(item)}"></option>`)
        .join("");
    });
  }

  function loadPathInputHistory() {
    try {
      const raw = window.localStorage.getItem(PATH_INPUT_HISTORY_KEY);
      if (!raw) {
        renderPathInputHistoryLists();
        return;
      }
      const parsed = JSON.parse(raw) || {};
      pathInputHistory = {
        source: sanitizePathInputHistoryItems(parsed.source),
        sourceSystem: sanitizePathInputHistoryItems(parsed.sourceSystem),
        target: sanitizePathInputHistoryItems(parsed.target),
        tranId: sanitizePathInputHistoryItems(parsed.tranId)
      };
    } catch {
      pathInputHistory = {
        source: [],
        sourceSystem: [],
        target: [],
        tranId: []
      };
    }
    renderPathInputHistoryLists();
  }

  function pushPathInputHistory(bucket, value) {
    const key = String(bucket || "").trim();
    if (!key || !Object.prototype.hasOwnProperty.call(pathInputHistory, key)) return;
    const normalized = normalizePathToken(value);
    if (!normalized) return;
    pathInputHistory[key] = [normalized]
      .concat((pathInputHistory[key] || []).filter((item) => item !== normalized))
      .slice(0, PATH_INPUT_HISTORY_MAX);
    savePathInputHistory();
    renderPathInputHistoryLists();
  }

  function restoreHomeState() {
    let state = {};
    const raw = localStorage.getItem(HOME_STATE_KEY);
    if (raw) {
      try {
        state = JSON.parse(raw) || {};
      } catch {
        state = {};
      }
    }

    if (startModelInput) {
      startModelInput.value = String(state.pathSource || "").trim();
    }
    if (startSourceSystemInput) {
      startSourceSystemInput.value = String(state.pathSourceSystem || "").trim();
    }
    if (endModelInput) {
      endModelInput.value = String(state.pathTarget || "").trim();
    }
    if (tranIdInput) {
      tranIdInput.value = String(state.pathTranId || "").trim();
    }
    setAutoCollapsePathSelection(state.autoCollapsePathSelection !== false);
  }

  function saveDataQueryState() {
    try {
      window.localStorage.setItem(
        DATA_QUERY_STATE_KEY,
        JSON.stringify({
          mainTable: dataQueryState.mainTable,
          joinTable: dataQueryState.joinTable,
          joinType: dataQueryState.joinType,
          mainJoinField: String(dataQueryState.joinConditions?.[0]?.mainField || "").trim(),
          joinJoinField: String(dataQueryState.joinConditions?.[0]?.joinField || "").trim(),
          joinConditions: (Array.isArray(dataQueryState.joinConditions) ? dataQueryState.joinConditions : []).map((joinItem) => ({
            id: String(joinItem?.id || "").trim(),
            mainField: String(joinItem?.mainField || "").trim(),
            joinField: String(joinItem?.joinField || "").trim(),
          })),
          selectedFields: (Array.isArray(dataQueryState.selectedFields) ? dataQueryState.selectedFields : [])
            .map((fieldRef) => normalizeDataQueryFieldRef(fieldRef))
            .filter(Boolean),
          outputFields: (Array.isArray(dataQueryState.outputFields) ? dataQueryState.outputFields : [])
            .map((fieldKey) => normalizeDataQueryResultColumnKey(fieldKey))
            .filter(Boolean),
          outputFieldsConfigured: Boolean(dataQueryState.outputFieldsConfigured),
          resultFieldsCollapsed: Boolean(dataQueryState.resultFieldsCollapsed),
          selectFieldsCollapsed: Boolean(dataQueryState.selectFieldsCollapsed),
          limit: normalizeDataQueryLimit(dataQueryState.limit),
          panelCollapsed: isDataQueryPanelCollapsed,
          filters: (Array.isArray(dataQueryState.filters) ? dataQueryState.filters : []).map((filterItem) => ({
            id: String(filterItem?.id || "").trim(),
            fieldRef: String(filterItem?.fieldRef || "").trim(),
            operator: String(filterItem?.operator || "in").trim().toLowerCase(),
            valuesText: String(filterItem?.valuesText || ""),
            rangeStart: String(filterItem?.rangeStart || ""),
            rangeEnd: String(filterItem?.rangeEnd || ""),
          })),
        })
      );
    } catch {
      // Ignore storage failures in restrictive browser contexts.
    }
  }

  function restoreDataQueryState() {
    try {
      const raw = window.localStorage.getItem(DATA_QUERY_STATE_KEY);
      if (!raw) return;
      const parsed = JSON.parse(raw) || {};
      dataQueryState = {
        mainTable: normalizeDataQueryTableName(parsed.mainTable),
        joinTable: normalizeDataQueryTableName(parsed.joinTable),
        joinType: ["left", "inner", "right"].includes(String(parsed.joinType || "").trim().toLowerCase())
          ? String(parsed.joinType || "left").trim().toLowerCase()
          : "left",
        joinConditions: (Array.isArray(parsed.joinConditions) ? parsed.joinConditions : []).map((joinItem) => ({
          id: String(joinItem?.id || "").trim() || createEmptyDataQueryJoinCondition().id,
          mainField: String(joinItem?.mainField || "").trim(),
          joinField: String(joinItem?.joinField || "").trim(),
        })),
        selectedFields: (Array.isArray(parsed.selectedFields) ? parsed.selectedFields : [])
          .map((fieldRef) => normalizeDataQueryFieldRef(fieldRef))
          .filter(Boolean),
        outputFields: (Array.isArray(parsed.outputFields) ? parsed.outputFields : [])
          .map((fieldKey) => normalizeDataQueryResultColumnKey(fieldKey))
          .filter(Boolean),
        outputFieldsConfigured: Boolean(parsed.outputFieldsConfigured),
        resultFieldsCollapsed: Boolean(parsed.resultFieldsCollapsed),
        selectFieldsCollapsed: Boolean(parsed.selectFieldsCollapsed),
        limit: normalizeDataQueryLimit(parsed.limit),
        panelCollapsed: Boolean(parsed.panelCollapsed ?? parsed.configCollapsed),
        filters: (Array.isArray(parsed.filters) ? parsed.filters : []).map((filterItem) => ({
          id: String(filterItem?.id || "").trim() || createEmptyDataQueryFilter().id,
          fieldRef: String(filterItem?.fieldRef || "").trim(),
          operator: String(filterItem?.operator || "in").trim().toLowerCase() === "range" ? "range" : "in",
          valuesText: String(filterItem?.valuesText || ""),
          rangeStart: String(filterItem?.rangeStart || ""),
          rangeEnd: String(filterItem?.rangeEnd || ""),
        })),
      };
      if (!dataQueryState.joinConditions.length && (parsed.mainJoinField || parsed.joinJoinField)) {
        dataQueryState.joinConditions = [{
          id: createEmptyDataQueryJoinCondition().id,
          mainField: String(parsed.mainJoinField || "").trim(),
          joinField: String(parsed.joinJoinField || "").trim(),
        }];
      }
    } catch {
      dataQueryState = {
        mainTable: "",
        joinTable: "",
        joinType: "left",
        joinConditions: [],
        selectedFields: [],
        outputFields: [],
        outputFieldsConfigured: false,
        resultFieldsCollapsed: false,
        selectFieldsCollapsed: false,
        limit: DATA_QUERY_DEFAULT_LIMIT,
        panelCollapsed: false,
        filters: [],
      };
    }
    isDataQueryPanelCollapsed = Boolean(dataQueryState.panelCollapsed);
  }

  function destroyDataQueryGrid() {
    if (dataQueryGridApi && typeof dataQueryGridApi.destroy === "function") {
      dataQueryGridApi.destroy();
    }
    dataQueryGridApi = null;
    if (dataQueryGrid) {
      dataQueryGrid.innerHTML = "";
    }
  }

  function clearDataQueryResult(message = "请选择主表；系统会先抽取 10 条预览数据。") {
    destroyDataQueryGrid();
    latestDataQueryResult = null;
    latestDataQueryResultMode = "preview";
    dataQueryResultFieldDraft = [];
    dataQuerySelectedFieldDraft = [];
    dataQueryState.outputFields = [];
    dataQueryState.outputFieldsConfigured = false;
    dataQueryState.resultFieldsCollapsed = false;
    if (dataQueryResultSummary) {
      dataQueryResultSummary.textContent = message;
    }
    if (dataQueryResultFieldsPanel) {
      dataQueryResultFieldsPanel.classList.add("hidden");
    }
    if (dataQueryResultFieldsList) {
      dataQueryResultFieldsList.innerHTML = "";
    }
    syncDataQueryResultFieldsPanelCollapsed();
    if (dataQueryResultEmpty) {
      dataQueryResultEmpty.textContent = message;
      dataQueryResultEmpty.classList.remove("hidden");
    }
    if (dataQueryGridShell) {
      dataQueryGridShell.classList.add("hidden");
    }
    if (exportDataQueryResultBtn) {
      exportDataQueryResultBtn.classList.add("hidden");
    }
  }

  function setDataQueryPanelCollapsed(isCollapsed) {
    isDataQueryPanelCollapsed = Boolean(isCollapsed);
    dataQueryState.panelCollapsed = isDataQueryPanelCollapsed;
    if (dataQueryBuilderPanel) {
      dataQueryBuilderPanel.classList.toggle("is-collapsed", isDataQueryPanelCollapsed);
    }
    if (toggleDataQueryConfigBtn) {
      toggleDataQueryConfigBtn.classList.toggle("is-collapsed", isDataQueryPanelCollapsed);
      toggleDataQueryConfigBtn.setAttribute("aria-expanded", isDataQueryPanelCollapsed ? "false" : "true");
      toggleDataQueryConfigBtn.title = isDataQueryPanelCollapsed ? "展开查询面板" : "收起查询面板";
      toggleDataQueryConfigBtn.setAttribute("aria-label", isDataQueryPanelCollapsed ? "展开查询面板" : "收起查询面板");
    }
    saveDataQueryState();
  }

  function setDataQueryBusy(isBusy, mode = "") {
    dataQueryInFlight = Boolean(isBusy);
    if (dataQueryPreviewBtn) {
      dataQueryPreviewBtn.disabled = Boolean(isBusy);
      dataQueryPreviewBtn.textContent = isBusy && mode === "preview" ? "预览中..." : "预览 10 条";
    }
    if (dataQueryRunBtn) {
      dataQueryRunBtn.disabled = Boolean(isBusy);
      dataQueryRunBtn.textContent = isBusy && mode === "run" ? "查询中..." : "执行查询";
    }
  }

  function getDataQueryTableLabel(tableName) {
    const normalized = normalizeDataQueryTableName(tableName);
    const matched = dataQueryTables.find((item) => normalizeDataQueryTableName(item?.name) === normalized);
    return String(matched?.label || normalized || "--").trim();
  }

  function renderDataQueryTableOptions() {
    const optionHtml = (Array.isArray(dataQueryTables) ? dataQueryTables : [])
      .map((item) => {
        const name = normalizeDataQueryTableName(item?.name);
        const label = String(item?.label || name || "").trim();
        if (!name) return "";
        return `<option value="${escAttr(name)}">${esc(label)} (${esc(name)})</option>`;
      })
      .join("");
    if (dataQueryMainTableSelect) {
      dataQueryMainTableSelect.innerHTML = `<option value="">请选择主表</option>${optionHtml}`;
      dataQueryMainTableSelect.value = dataQueryState.mainTable || "";
    }
    if (dataQueryJoinTableSelect) {
      dataQueryJoinTableSelect.innerHTML = `<option value="">不关联</option>${optionHtml}`;
      dataQueryJoinTableSelect.value = dataQueryState.joinTable || "";
    }
    if (dataQueryJoinTypeSelect) {
      dataQueryJoinTypeSelect.value = dataQueryState.joinType || "left";
      dataQueryJoinTypeSelect.disabled = !dataQueryState.joinTable;
    }
  }

  function renderDataQueryJoinFieldOptions() {
    if (!dataQueryJoinConditionsList) return;
    if (!dataQueryState.joinTable) {
      dataQueryJoinConditionsList.innerHTML = '<div class="database-query-join-condition-empty">未选择关联表时无需配置关联字段。</div>';
      return;
    }

    const renderFieldOptions = (tableName, placeholder) => {
      const schema = dataQuerySchemaCache[normalizeDataQueryTableName(tableName)] || [];
      const options = schema
        .map((column) => {
          const columnName = String(column?.name || "").trim();
          const comment = String(column?.comment || "").trim();
          if (!columnName) return "";
          const label = comment ? `${columnName} | ${comment}` : columnName;
          return `<option value="${escAttr(columnName)}">${esc(label)}</option>`;
        })
        .join("");
      return `<option value="">${esc(placeholder)}</option>${options}`;
    };

    if (!Array.isArray(dataQueryState.joinConditions) || !dataQueryState.joinConditions.length) {
      dataQueryState.joinConditions = [createEmptyDataQueryJoinCondition()];
    }
    const mainOptionsHtml = renderFieldOptions(dataQueryState.mainTable, "请选择主表字段");
    const joinOptionsHtml = renderFieldOptions(dataQueryState.joinTable, "请选择关联字段");
    dataQueryJoinConditionsList.innerHTML = dataQueryState.joinConditions.map((joinItem) => `
      <div class="database-query-join-condition-row" data-join-id="${escAttr(joinItem?.id || "")}">
        <select class="glass-input" data-join-input="mainField">
          ${mainOptionsHtml}
        </select>
        <div class="database-query-join-condition-equals">=</div>
        <select class="glass-input" data-join-input="joinField">
          ${joinOptionsHtml}
        </select>
        <button class="glass-btn tiny" type="button" data-join-action="remove" ${dataQueryState.joinConditions.length <= 1 ? "disabled" : ""}>删除</button>
      </div>
    `).join("");

    dataQueryJoinConditionsList.querySelectorAll('[data-join-input="mainField"]').forEach((selectEl) => {
      const row = selectEl.closest('[data-join-id]');
      const joinId = String(row?.dataset.joinId || "").trim();
      const matched = dataQueryState.joinConditions.find((item) => String(item?.id || "").trim() === joinId);
      selectEl.value = String(matched?.mainField || "").trim();
    });
    dataQueryJoinConditionsList.querySelectorAll('[data-join-input="joinField"]').forEach((selectEl) => {
      const row = selectEl.closest('[data-join-id]');
      const joinId = String(row?.dataset.joinId || "").trim();
      const matched = dataQueryState.joinConditions.find((item) => String(item?.id || "").trim() === joinId);
      selectEl.value = String(matched?.joinField || "").trim();
    });
  }

  function updateDataQueryConfigHint() {
    if (!dataQueryConfigHint) return;
    if (!dataQueryState.mainTable) {
      dataQueryConfigHint.textContent = "请选择主表后开始预览。";
      return;
    }
    if (dataQueryState.joinTable && !isDataQueryJoinConfigured()) {
      dataQueryConfigHint.textContent = "已选择关联表，请补齐所有关联字段后再预览。";
      return;
    }
    const mainLabel = getDataQueryTableLabel(dataQueryState.mainTable);
    if (!dataQueryState.joinTable) {
      dataQueryConfigHint.textContent = `当前模式：单表查询 ${mainLabel}。系统会先自动预览 10 条。`;
      return;
    }
    const joinLabel = getDataQueryTableLabel(dataQueryState.joinTable);
    dataQueryConfigHint.textContent = `当前模式：${mainLabel} ${String(dataQueryState.joinType || "left").toUpperCase()} JOIN ${joinLabel}。系统会先自动预览 10 条。`;
  }

  function renderDataQueryFilters() {
    if (!dataQueryFiltersList) return;
    const fieldOptions = buildDataQueryFieldOptions();
    const optionHtml = fieldOptions
      .map((option) => {
        const suffix = option.comment ? ` | ${option.comment}` : "";
        return `<option value="${escAttr(option.value)}">${esc(option.label)}${esc(suffix)}</option>`;
      })
      .join("");

    if (!Array.isArray(dataQueryState.filters) || !dataQueryState.filters.length) {
      dataQueryFiltersList.innerHTML = `<div class="database-query-filter-row is-empty">暂无筛选条件。可按字段增加多值、通配符或区间筛选。</div>`;
      return;
    }

    dataQueryFiltersList.innerHTML = dataQueryState.filters.map((filterItem) => {
      const id = String(filterItem?.id || "").trim();
      const operator = String(filterItem?.operator || "in").trim().toLowerCase() === "range" ? "range" : "in";
      const fieldRef = String(filterItem?.fieldRef || "").trim();
      const rangeValues = `
        <div class="database-query-filter-values database-query-filter-values-range">
          <input class="glass-input" data-filter-input="rangeStart" type="text" value="${escAttr(filterItem?.rangeStart || "")}" placeholder="起始值" />
          <input class="glass-input" data-filter-input="rangeEnd" type="text" value="${escAttr(filterItem?.rangeEnd || "")}" placeholder="结束值" />
        </div>
      `;
      const multiValues = `
        <div class="database-query-filter-values">
          <div class="database-query-filter-values-main">
            <textarea class="database-query-filter-textarea" data-filter-input="valuesText" placeholder="可直接粘贴多个值；按换行、逗号、分号或 Tab 拆分。支持 * 和 ? 通配符（匹配不区分大小写），例如 OA*。">${esc(filterItem?.valuesText || "")}</textarea>
            <div class="database-query-filter-toolbar">
              <button class="glass-btn tiny" type="button" data-filter-action="paste">从剪贴板粘贴</button>
              <button class="glass-btn tiny" type="button" data-filter-action="clear-values">清空值</button>
            </div>
          </div>
        </div>
      `;
      return `
        <div class="database-query-filter-row" data-filter-id="${escAttr(id)}">
          <div class="database-query-filter-left">
            <div class="database-query-filter-select-pair">
              <select class="glass-input" data-filter-input="fieldRef">
                <option value="">请选择筛选字段</option>
                ${optionHtml}
              </select>
              <select class="glass-input" data-filter-input="operator">
                <option value="in"${operator === "in" ? " selected" : ""}>多单值</option>
                <option value="range"${operator === "range" ? " selected" : ""}>区间</option>
              </select>
            </div>
          </div>
          ${operator === "range" ? rangeValues : multiValues}
          <button class="glass-btn tiny database-query-filter-remove" type="button" data-filter-action="remove">删除</button>
        </div>
      `;
    }).join("");

    dataQueryFiltersList.querySelectorAll('[data-filter-input="fieldRef"]').forEach((selectEl) => {
      const row = selectEl.closest('[data-filter-id]');
      const filterId = String(row?.dataset.filterId || "").trim();
      const matched = dataQueryState.filters.find((item) => String(item?.id || "").trim() === filterId);
      selectEl.value = String(matched?.fieldRef || "").trim();
    });
  }

  function renderDataQuerySelectFields() {
    if (!dataQuerySelectFieldsList) return;
    const options = buildDataQueryFieldOptions();
    sanitizeDataQuerySelectedFieldDraft();
    const selectedRefSet = new Set(Array.isArray(dataQuerySelectedFieldDraft) ? dataQuerySelectedFieldDraft : []);
    const keyword = String(dataQuerySelectFieldSearchInput?.value || "").trim().toLowerCase();

    if (!dataQueryState.mainTable) {
      dataQuerySelectFieldsList.innerHTML = '<div class="database-query-select-fields-empty">请先选择主表后再选择查询字段。</div>';
      return;
    }

    const filteredOptions = options.filter((option) => {
      if (!keyword) return true;
      const searchText = [option.value, option.label, option.comment].map((item) => String(item || "").toLowerCase()).join(" ");
      return searchText.includes(keyword);
    });
    if (!filteredOptions.length) {
      dataQuerySelectFieldsList.innerHTML = '<div class="database-query-select-fields-empty">没有匹配的字段，请调整搜索关键字。</div>';
      return;
    }

    dataQuerySelectFieldsList.innerHTML = filteredOptions.map((option) => {
      const fieldRef = String(option?.value || "").trim();
      const isChecked = selectedRefSet.has(fieldRef);
      const fieldText = String(option?.fieldText || "").trim();
      const fieldName = String(option?.fieldName || "").trim();
      return `
        <label class="database-query-select-fields-item">
          <input type="checkbox" data-select-field-ref="${escAttr(fieldRef)}" ${isChecked ? "checked" : ""} />
          <span class="database-query-select-fields-item-text">
            <strong>${esc(option?.label || fieldRef)}</strong>
            ${fieldText ? `<span>${esc(fieldText)}</span>` : (fieldName ? `<span>${esc(fieldName)}</span>` : "")}
          </span>
        </label>
      `;
    }).join("");
    syncDataQuerySelectFieldsPanelCollapsed();
  }

  function renderDataQueryResultFieldSelector(result = latestDataQueryResult) {
    if (!dataQueryResultFieldsPanel || !dataQueryResultFieldsList) return;
    const columns = Array.isArray(result?.columns) ? result.columns : [];
    if (!columns.length) {
      dataQueryResultFieldsPanel.classList.add("hidden");
      dataQueryResultFieldsList.innerHTML = "";
      return;
    }

    sanitizeDataQueryOutputFields(columns);
    sanitizeDataQueryResultFieldDraft(columns);
    const draftKeySet = new Set(Array.isArray(dataQueryResultFieldDraft) ? dataQueryResultFieldDraft : []);

    dataQueryResultFieldsPanel.classList.remove("hidden");
    syncDataQueryResultFieldsPanelCollapsed();
    dataQueryResultFieldsList.innerHTML = columns.map((column) => {
      const columnKey = normalizeDataQueryResultColumnKey(column?.key);
      const isChecked = draftKeySet.has(columnKey);
      const fieldText = String(column?.field_text || "").trim();
      return `
        <label class="database-query-select-fields-item">
          <input type="checkbox" data-result-field-key="${escAttr(columnKey)}" ${isChecked ? "checked" : ""} />
          <span class="database-query-select-fields-item-text">
            <strong>${esc(column?.label || columnKey)}</strong>
            ${fieldText ? `<span>${esc(fieldText)}</span>` : (column?.comment ? `<span>${esc(column.comment)}</span>` : "")}
          </span>
        </label>
      `;
    }).join("");
  }

  async function ensureDataQueryTablesLoaded() {
    if (Array.isArray(dataQueryTables) && dataQueryTables.length) {
      return dataQueryTables;
    }
    if (!dataQueryTablesLoadPromise) {
      dataQueryTablesLoadPromise = fetchDataQueryTables()
        .then((payload) => {
          dataQueryTables = Array.isArray(payload?.tables) ? payload.tables : [];
          renderDataQueryTableOptions();
          return dataQueryTables;
        })
        .finally(() => {
          dataQueryTablesLoadPromise = null;
        });
    }
    return dataQueryTablesLoadPromise;
  }

  async function ensureDataQuerySchemaLoaded(tableName) {
    const normalizedTableName = normalizeDataQueryTableName(tableName);
    if (!normalizedTableName) return [];
    if (Array.isArray(dataQuerySchemaCache[normalizedTableName])) {
      return dataQuerySchemaCache[normalizedTableName];
    }
    if (!dataQuerySchemaLoadPromises[normalizedTableName]) {
      dataQuerySchemaLoadPromises[normalizedTableName] = fetchDataQuerySchema(normalizedTableName)
        .then((payload) => {
          dataQuerySchemaCache[normalizedTableName] = Array.isArray(payload?.columns) ? payload.columns : [];
          return dataQuerySchemaCache[normalizedTableName];
        })
        .finally(() => {
          delete dataQuerySchemaLoadPromises[normalizedTableName];
        });
    }
    return dataQuerySchemaLoadPromises[normalizedTableName];
  }

  async function refreshDataQueryMetadata() {
    await ensureDataQueryTablesLoaded();
    if (dataQueryState.mainTable) {
      await ensureDataQuerySchemaLoaded(dataQueryState.mainTable);
    }
    if (dataQueryState.joinTable) {
      if (!Array.isArray(dataQueryState.joinConditions) || !dataQueryState.joinConditions.length) {
        dataQueryState.joinConditions = [createEmptyDataQueryJoinCondition()];
      }
      await ensureDataQuerySchemaLoaded(dataQueryState.joinTable);
      syncAutoSelectedJoinFields();
    } else {
      dataQueryState.joinConditions = [];
    }
    renderDataQueryTableOptions();
    renderDataQueryJoinFieldOptions();
    syncDataQuerySelectedFieldDraft(true);
    renderDataQuerySelectFields();
    renderDataQueryFilters();
    updateDataQueryConfigHint();
    saveDataQueryState();
  }

  function renderDataQueryGrid(result, mode = "preview") {
    const agGridLib = window.agGrid || null;
    destroyDataQueryGrid();
    if (!dataQueryGrid || !dataQueryGridShell || !dataQueryResultEmpty) return;

    const rows = Array.isArray(result?.rows) ? result.rows : [];
    const isNewResult = latestDataQueryResult !== result;
    if (isNewResult) {
      syncDataQueryResultFieldDraft(result?.columns, true);
    }
    const columns = getActiveDataQueryResultColumns(result?.columns);
    latestDataQueryResult = result;
    latestDataQueryResultMode = mode === "run" ? "run" : "preview";
    renderDataQueryResultFieldSelector(result);
    const summaryPrefix = mode === "preview" ? "预览" : "查询";
    const summaryText = `${summaryPrefix}完成：返回 ${formatCount(rows.length)} 条，主表 ${getDataQueryTableLabel(result?.main_table)}${result?.join_table ? `，关联表 ${getDataQueryTableLabel(result?.join_table)}` : ""}${result?.has_more ? "，还有更多未加载结果。" : "。"}`;

    if (dataQueryResultSummary) {
      dataQueryResultSummary.textContent = summaryText;
    }

    if (!rows.length || !columns.length) {
      dataQueryResultEmpty.textContent = rows.length && !columns.length
        ? "当前未选择任何输出字段。请在上方勾选字段后点击确定。"
        : `${summaryPrefix}结果为空。请调整表、关联条件或筛选项后重试。`;
      dataQueryResultEmpty.classList.remove("hidden");
      dataQueryGridShell.classList.add("hidden");
      if (exportDataQueryResultBtn) {
        exportDataQueryResultBtn.classList.add("hidden");
      }
      return;
    }

    dataQueryResultEmpty.classList.add("hidden");
    dataQueryGridShell.classList.remove("hidden");
    if (exportDataQueryResultBtn) {
      exportDataQueryResultBtn.classList.remove("hidden");
    }

    if (!agGridLib) {
      dataQueryGrid.innerHTML = `<div style="padding:16px;color:#ffd7d7;">AG Grid 未加载，无法渲染查询结果。</div>`;
      return;
    }

    const gridOptions = {
      defaultColDef: {
        sortable: true,
        filter: true,
        resizable: true,
        minWidth: 140,
        cellDataType: false,
      },
      rowHeight: 30,
      headerHeight: 34,
      animateRows: false,
      suppressRowTransform: true,
      suppressContextMenu: true,
      columnDefs: columns.map((column) => ({
        headerName: String(column?.label || column?.key || "").trim(),
        field: String(column?.key || "").trim(),
        headerTooltip: [String(column?.label || "").trim(), String(column?.comment || "").trim()].filter(Boolean).join(" | "),
        tooltipField: String(column?.key || "").trim(),
      })),
      rowData: rows,
      overlayNoRowsTemplate: '<span style="padding:12px;display:inline-block;">当前条件下没有可展示的数据。</span>',
      onCellContextMenu: (params) => {
        params?.event?.preventDefault?.();
        params?.event?.stopPropagation?.();
        void copyDataQueryCellValue(params?.value);
      }
    };

    if (typeof agGridLib.createGrid === "function") {
      dataQueryGridApi = agGridLib.createGrid(dataQueryGrid, gridOptions);
    } else if (typeof agGridLib.Grid === "function") {
      new agGridLib.Grid(dataQueryGrid, gridOptions);
      dataQueryGridApi = gridOptions.api || null;
    }

    if (dataQueryGridApi && typeof dataQueryGridApi.autoSizeAllColumns === "function") {
      window.requestAnimationFrame(() => {
        try {
          if (typeof dataQueryGridApi.doLayout === "function") {
            dataQueryGridApi.doLayout();
          }
          dataQueryGridApi.autoSizeAllColumns();
        } catch {
          // Keep AG Grid default widths if auto-size fails.
        }
      });
    }
  }

  function normalizeDataQueryCellText(value) {
    if (value === null || value === undefined) return "";
    if (typeof value === "string") return value;
    if (typeof value === "number" || typeof value === "boolean" || typeof value === "bigint") {
      return String(value);
    }
    if (value instanceof Date) {
      return value.toISOString();
    }
    try {
      return JSON.stringify(value);
    } catch {
      return String(value);
    }
  }

  async function copyDataQueryCellValue(value) {
    const copyText = normalizeDataQueryCellText(value).trim();
    if (!copyText) {
      showToast("当前单元格内容为空。", "error");
      return;
    }
    try {
      await writeTextToClipboard(copyText);
      const preview = copyText.length > 60 ? `${copyText.slice(0, 57)}...` : copyText;
      showToast(`已复制单元格内容：${preview}`);
    } catch (error) {
      const rawMsg = String(error?.message || "").trim();
      showToast(`复制失败。${rawMsg ? ` 详情: ${rawMsg}` : ""}`, "error");
    }
  }

  async function executeDataQuery(mode = "preview", options = {}) {
    const normalizedMode = mode === "run" ? "run" : "preview";
    const isSilent = Boolean(options?.silent);
    if (!dataQueryState.mainTable) {
      clearDataQueryResult("请选择主表；系统会先抽取 10 条预览数据。");
      if (!isSilent) {
        showToast("请先选择主表。", "error");
      }
      return;
    }
    if (!isDataQueryJoinConfigured()) {
      clearDataQueryResult("已选择关联表，请补齐关联字段后再预览。", "error");
      if (!isSilent) {
        showToast("已选择关联表，请补齐所有关联字段。", "error");
      }
      return;
    }

    const requestId = ++dataQueryRequestSeq;
    const payload = buildDataQueryPayload(normalizedMode === "preview" ? DATA_QUERY_PREVIEW_LIMIT : normalizeDataQueryLimit(dataQueryState.limit));
    setDataQueryBusy(true, normalizedMode);
    try {
      const result = await runDataQueryRequest(payload);
      if (requestId !== dataQueryRequestSeq) {
        return;
      }
      renderDataQueryGrid(result, normalizedMode);
      if (normalizedMode === "run") {
        setDataQueryPanelCollapsed(true);
      }
      saveDataQueryState();
      if (!isSilent && normalizedMode === "run") {
        showToast(`查询完成，返回 ${formatCount(result?.returned_count || 0)} 条记录。`);
      }
    } catch (error) {
      const rawMsg = String(error?.message || "").trim();
      clearDataQueryResult(`查询失败：${rawMsg || "请稍后重试。"}`);
      if (!isSilent) {
        showToast(`数据库查询失败。${rawMsg ? ` 详情: ${rawMsg}` : ""}`, "error");
      }
    } finally {
      if (requestId === dataQueryRequestSeq) {
        setDataQueryBusy(false, normalizedMode);
      }
    }
  }

  function scheduleDataQueryPreview() {
    if (dataQueryPreviewTimer) {
      window.clearTimeout(dataQueryPreviewTimer);
      dataQueryPreviewTimer = 0;
    }
    if (!dataQueryState.mainTable) {
      clearDataQueryResult("请选择主表；系统会先抽取 10 条预览数据。");
      return;
    }
    if (!isDataQueryJoinConfigured()) {
      clearDataQueryResult("已选择关联表，请补齐关联字段后再预览。");
      return;
    }
    dataQueryPreviewTimer = window.setTimeout(() => {
      dataQueryPreviewTimer = 0;
      void executeDataQuery("preview", { silent: true });
    }, 180);
  }

  async function hydrateDataQueryWorkspace() {
    await refreshDataQueryMetadata();
    if (dataQueryLimitInput) {
      dataQueryLimitInput.value = String(normalizeDataQueryLimit(dataQueryState.limit));
    }
    setDataQueryPanelCollapsed(isDataQueryPanelCollapsed);
    scheduleDataQueryPreview();
  }

  async function exportDataQueryResult() {
    if (!latestDataQueryResult) {
      showToast("当前没有可导出的查询结果。", "error");
      return;
    }

    try {
      await ensureExcelJsLoaded();
    } catch {
      showToast("Excel 导出依赖加载失败，请刷新后重试。", "error");
      return;
    }

    const workbook = new window.ExcelJS.Workbook();
    const worksheet = workbook.addWorksheet("Query Result");
    const columns = getActiveDataQueryResultColumns(latestDataQueryResult?.columns);
    const rows = Array.isArray(latestDataQueryResult?.rows) ? latestDataQueryResult.rows : [];
    const headerLabels = columns.map((column) => String(column?.label || column?.key || "").trim());
    worksheet.addRow(headerLabels);
    rows.forEach((row) => {
      worksheet.addRow(columns.map((column) => row?.[String(column?.key || "").trim()] ?? ""));
    });
    worksheet.getRow(1).font = { bold: true };
    worksheet.views = [{ state: "frozen", ySplit: 1 }];
    worksheet.columns.forEach((column) => {
      column.width = Math.min(
        36,
        Math.max(
          14,
          String(column.header || "").length + 4
        )
      );
    });

    const mainName = String(latestDataQueryResult?.main_table || "query").replace(/[^A-Za-z0-9_-]+/g, "_");
    const joinName = String(latestDataQueryResult?.join_table || "").replace(/[^A-Za-z0-9_-]+/g, "_");
    const fileName = joinName ? `data_query_${mainName}_join_${joinName}.xlsx` : `data_query_${mainName}.xlsx`;
    const buffer = await workbook.xlsx.writeBuffer();
    const blob = new Blob([buffer], { type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" });

    const hasSavePicker = typeof window.showSaveFilePicker === "function" && !isSavePickerDisabled();
    let shouldFallbackToAnchorDownload = !hasSavePicker;
    if (hasSavePicker) {
      try {
        const fileHandle = await window.showSaveFilePicker({
          suggestedName: fileName,
          types: [
            {
              description: "Excel Workbook",
              accept: {
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": [".xlsx"]
              }
            }
          ]
        });
        const writable = await fileHandle.createWritable();
        await writable.write(blob);
        await writable.close();
        showToast(`已保存导出文件：${fileName}`);
        return;
      } catch (error) {
        const errorName = String(error?.name || "").trim();
        if (errorName === "AbortError") {
          return;
        }
        if (errorName === "NotAllowedError") {
          setSavePickerDisabled(true);
          shouldFallbackToAnchorDownload = true;
        } else {
          throw new Error(`系统保存失败（${errorName || "UnknownError"}）：${String(error?.message || "当前环境不支持该保存流程")}`);
        }
      }
    }

    if (shouldFallbackToAnchorDownload) {
      const url = window.URL.createObjectURL(blob);
      const anchor = document.createElement("a");
      anchor.href = url;
      anchor.download = fileName;
      document.body.appendChild(anchor);
      anchor.click();
      document.body.removeChild(anchor);
      window.setTimeout(() => window.URL.revokeObjectURL(url), 1000);
      showToast(`已导出查询结果：${fileName}`);
    }
  }

  function setupDataQueryUi() {
    restoreDataQueryState();
    syncDataQuerySelectedFieldDraft(true);
    renderDataQueryTableOptions();
    renderDataQueryJoinFieldOptions();
    renderDataQuerySelectFields();
    renderDataQueryFilters();
    updateDataQueryConfigHint();
    setDataQueryPanelCollapsed(isDataQueryPanelCollapsed);
    syncDataQuerySelectFieldsPanelCollapsed();
    syncDataQueryResultFieldsPanelCollapsed();
    clearDataQueryResult();

    if (dataQueryMainTableSelect) {
      dataQueryMainTableSelect.addEventListener("change", async () => {
        dataQueryState.mainTable = normalizeDataQueryTableName(dataQueryMainTableSelect.value);
        if (!dataQueryState.mainTable) {
          dataQueryState.joinTable = "";
          dataQueryState.joinConditions = [];
        }
        await refreshDataQueryMetadata();
        scheduleDataQueryPreview();
      });
    }
    if (dataQueryJoinTableSelect) {
      dataQueryJoinTableSelect.addEventListener("change", async () => {
        dataQueryState.joinTable = normalizeDataQueryTableName(dataQueryJoinTableSelect.value);
        dataQueryState.joinConditions = dataQueryState.joinTable ? [createEmptyDataQueryJoinCondition()] : [];
        await refreshDataQueryMetadata();
        scheduleDataQueryPreview();
      });
    }
    if (dataQueryJoinTypeSelect) {
      dataQueryJoinTypeSelect.addEventListener("change", () => {
        dataQueryState.joinType = String(dataQueryJoinTypeSelect.value || "left").trim().toLowerCase() || "left";
        saveDataQueryState();
        scheduleDataQueryPreview();
      });
    }
    if (dataQueryAddJoinConditionBtn) {
      dataQueryAddJoinConditionBtn.addEventListener("click", () => {
        if (!dataQueryState.joinTable) {
          showToast("请先选择关联表。", "error");
          return;
        }
        dataQueryState.joinConditions = [...(dataQueryState.joinConditions || []), createEmptyDataQueryJoinCondition()];
        renderDataQueryJoinFieldOptions();
        saveDataQueryState();
      });
    }
    if (dataQueryClearJoinConditionsBtn) {
      dataQueryClearJoinConditionsBtn.addEventListener("click", () => {
        dataQueryState.joinConditions = dataQueryState.joinTable ? [createEmptyDataQueryJoinCondition()] : [];
        renderDataQueryJoinFieldOptions();
        saveDataQueryState();
        scheduleDataQueryPreview();
      });
    }
    if (dataQueryLimitInput) {
      dataQueryLimitInput.addEventListener("change", () => {
        dataQueryState.limit = normalizeDataQueryLimit(dataQueryLimitInput.value);
        dataQueryLimitInput.value = String(dataQueryState.limit);
        saveDataQueryState();
      });
    }
    if (dataQuerySelectAllResultFieldsBtn) {
      dataQuerySelectAllResultFieldsBtn.addEventListener("click", () => {
        const columns = Array.isArray(latestDataQueryResult?.columns) ? latestDataQueryResult.columns : [];
        dataQueryResultFieldDraft = columns.map((column) => normalizeDataQueryResultColumnKey(column?.key)).filter(Boolean);
        sanitizeDataQueryResultFieldDraft(columns);
        renderDataQueryResultFieldSelector();
      });
    }
    if (dataQueryClearResultFieldsBtn) {
      dataQueryClearResultFieldsBtn.addEventListener("click", () => {
        dataQueryResultFieldDraft = [];
        renderDataQueryResultFieldSelector();
      });
    }
    if (dataQueryApplyResultFieldsBtn) {
      dataQueryApplyResultFieldsBtn.addEventListener("click", () => {
        sanitizeDataQueryResultFieldDraft();
        dataQueryState.outputFields = [...dataQueryResultFieldDraft];
        dataQueryState.outputFieldsConfigured = true;
        dataQueryState.resultFieldsCollapsed = dataQueryResultFieldDraft.length > 0;
        if (latestDataQueryResult) {
          renderDataQueryGrid(latestDataQueryResult, latestDataQueryResultMode);
        } else {
          renderDataQueryResultFieldSelector();
        }
        saveDataQueryState();
      });
    }
    if (dataQueryToggleResultFieldsBtn) {
      dataQueryToggleResultFieldsBtn.addEventListener("click", () => {
        dataQueryState.resultFieldsCollapsed = !dataQueryState.resultFieldsCollapsed;
        syncDataQueryResultFieldsPanelCollapsed();
        saveDataQueryState();
      });
    }
    if (dataQueryResultFieldsList) {
      dataQueryResultFieldsList.addEventListener("change", (event) => {
        const target = event.target;
        if (!(target instanceof HTMLInputElement) || target.type !== "checkbox") return;
        const fieldKey = normalizeDataQueryResultColumnKey(target.dataset.resultFieldKey || "");
        if (!fieldKey) return;
        const outputFieldSet = new Set(Array.isArray(dataQueryResultFieldDraft) ? dataQueryResultFieldDraft : []);
        if (target.checked) {
          outputFieldSet.add(fieldKey);
        } else {
          outputFieldSet.delete(fieldKey);
        }
        dataQueryResultFieldDraft = Array.from(outputFieldSet);
        sanitizeDataQueryResultFieldDraft();
        renderDataQueryResultFieldSelector();
      });
    }
    if (dataQuerySelectFieldSearchInput) {
      dataQuerySelectFieldSearchInput.addEventListener("input", () => {
        renderDataQuerySelectFields();
      });
    }
    if (dataQuerySelectAllFieldsBtn) {
      dataQuerySelectAllFieldsBtn.addEventListener("click", () => {
        dataQuerySelectedFieldDraft = buildDataQueryFieldOptions().map((item) => String(item?.value || "").trim()).filter(Boolean);
        sanitizeDataQuerySelectedFieldDraft();
        renderDataQuerySelectFields();
      });
    }
    if (dataQueryClearSelectedFieldsBtn) {
      dataQueryClearSelectedFieldsBtn.addEventListener("click", () => {
        dataQuerySelectedFieldDraft = [];
        renderDataQuerySelectFields();
      });
    }
    if (dataQueryApplySelectFieldsBtn) {
      dataQueryApplySelectFieldsBtn.addEventListener("click", () => {
        sanitizeDataQuerySelectedFieldDraft();
        dataQueryState.selectedFields = [...dataQuerySelectedFieldDraft];
        dataQueryState.selectFieldsCollapsed = true;
        syncDataQuerySelectFieldsPanelCollapsed();
        saveDataQueryState();
        scheduleDataQueryPreview();
      });
    }
    if (dataQueryToggleSelectFieldsBtn) {
      dataQueryToggleSelectFieldsBtn.addEventListener("click", () => {
        dataQueryState.selectFieldsCollapsed = !dataQueryState.selectFieldsCollapsed;
        syncDataQuerySelectFieldsPanelCollapsed();
        saveDataQueryState();
      });
    }
    if (dataQuerySelectFieldsList) {
      dataQuerySelectFieldsList.addEventListener("change", (event) => {
        const target = event.target;
        if (!(target instanceof HTMLInputElement) || target.type !== "checkbox") return;
        const fieldRef = normalizeDataQueryFieldRef(target.dataset.selectFieldRef || "");
        if (!fieldRef) return;
        const selectedFieldSet = new Set(Array.isArray(dataQuerySelectedFieldDraft) ? dataQuerySelectedFieldDraft : []);
        if (target.checked) {
          selectedFieldSet.add(fieldRef);
        } else {
          selectedFieldSet.delete(fieldRef);
        }
        dataQuerySelectedFieldDraft = Array.from(selectedFieldSet);
        sanitizeDataQuerySelectedFieldDraft();
        renderDataQuerySelectFields();
      });
    }
    if (dataQueryAddFilterBtn) {
      dataQueryAddFilterBtn.addEventListener("click", () => {
        dataQueryState.filters = [...(dataQueryState.filters || []), createEmptyDataQueryFilter()];
        renderDataQueryFilters();
        saveDataQueryState();
      });
    }
    if (dataQueryClearFiltersBtn) {
      dataQueryClearFiltersBtn.addEventListener("click", () => {
        dataQueryState.filters = [];
        renderDataQueryFilters();
        saveDataQueryState();
      });
    }
    if (dataQueryPreviewBtn) {
      dataQueryPreviewBtn.addEventListener("click", () => {
        void executeDataQuery("preview");
      });
    }
    if (dataQueryRunBtn) {
      dataQueryRunBtn.addEventListener("click", () => {
        void executeDataQuery("run");
      });
    }
    if (toggleDataQueryConfigBtn) {
      toggleDataQueryConfigBtn.addEventListener("click", () => {
        setDataQueryPanelCollapsed(!isDataQueryPanelCollapsed);
      });
    }
    if (exportDataQueryResultBtn) {
      exportDataQueryResultBtn.addEventListener("click", () => {
        void exportDataQueryResult();
      });
    }
    if (dataQueryJoinConditionsList) {
      dataQueryJoinConditionsList.addEventListener("change", (event) => {
        const target = event.target;
        const row = target?.closest?.("[data-join-id]");
        const joinId = String(row?.dataset.joinId || "").trim();
        if (!joinId) return;
        const joinItem = dataQueryState.joinConditions.find((item) => String(item?.id || "").trim() === joinId);
        if (!joinItem) return;
        const inputKey = String(target?.dataset?.joinInput || "").trim();
        if (!inputKey) return;
        joinItem[inputKey] = String(target?.value || "").trim();
        saveDataQueryState();
        scheduleDataQueryPreview();
      });
      dataQueryJoinConditionsList.addEventListener("click", (event) => {
        const trigger = event.target.closest("[data-join-action]");
        if (!trigger || trigger.disabled) return;
        const row = trigger.closest("[data-join-id]");
        const joinId = String(row?.dataset.joinId || "").trim();
        if (!joinId) return;
        const action = String(trigger.dataset.joinAction || "").trim();
        if (action !== "remove") return;
        dataQueryState.joinConditions = (dataQueryState.joinConditions || []).filter((item) => String(item?.id || "").trim() !== joinId);
        if (dataQueryState.joinTable && !dataQueryState.joinConditions.length) {
          dataQueryState.joinConditions = [createEmptyDataQueryJoinCondition()];
        }
        renderDataQueryJoinFieldOptions();
        saveDataQueryState();
        scheduleDataQueryPreview();
      });
    }
    if (dataQueryFiltersList) {
      dataQueryFiltersList.addEventListener("change", (event) => {
        const target = event.target;
        const row = target?.closest?.("[data-filter-id]");
        const filterId = String(row?.dataset.filterId || "").trim();
        if (!filterId) return;
        const filterItem = dataQueryState.filters.find((item) => String(item?.id || "").trim() === filterId);
        if (!filterItem) return;
        const inputKey = String(target?.dataset?.filterInput || "").trim();
        if (!inputKey) return;
        filterItem[inputKey] = String(target?.value || "");
        if (inputKey === "operator") {
          if (String(filterItem.operator || "").trim().toLowerCase() === "range") {
            filterItem.valuesText = "";
          } else {
            filterItem.rangeStart = "";
            filterItem.rangeEnd = "";
          }
          renderDataQueryFilters();
        }
        saveDataQueryState();
      });
      dataQueryFiltersList.addEventListener("input", (event) => {
        const target = event.target;
        const row = target?.closest?.("[data-filter-id]");
        const filterId = String(row?.dataset.filterId || "").trim();
        if (!filterId) return;
        const filterItem = dataQueryState.filters.find((item) => String(item?.id || "").trim() === filterId);
        if (!filterItem) return;
        const inputKey = String(target?.dataset?.filterInput || "").trim();
        if (!inputKey) return;
        filterItem[inputKey] = String(target?.value || "");
        saveDataQueryState();
      });
      dataQueryFiltersList.addEventListener("click", async (event) => {
        const trigger = event.target.closest("[data-filter-action]");
        if (!trigger) return;
        const row = trigger.closest("[data-filter-id]");
        const filterId = String(row?.dataset.filterId || "").trim();
        if (!filterId) return;
        const filterIndex = dataQueryState.filters.findIndex((item) => String(item?.id || "").trim() === filterId);
        if (filterIndex < 0) return;
        const action = String(trigger.dataset.filterAction || "").trim();
        if (action === "remove") {
          dataQueryState.filters.splice(filterIndex, 1);
          renderDataQueryFilters();
          saveDataQueryState();
          return;
        }
        if (action === "clear-values") {
          dataQueryState.filters[filterIndex].valuesText = "";
          renderDataQueryFilters();
          saveDataQueryState();
          return;
        }
        if (action === "paste") {
          try {
            const text = await readTextFromClipboard();
            dataQueryState.filters[filterIndex].valuesText = text;
            renderDataQueryFilters();
            saveDataQueryState();
            showToast(`已粘贴 ${formatCount(splitDataQueryValuesText(text).length)} 个筛选值。`);
          } catch (error) {
            showToast(`读取剪贴板失败：${error?.message || "未知错误"}`, "error");
          }
        }
      });
    }
  }

  function showLoginGate() {
    if (!loginGate) return;
    showBlockingModal(loginGate);
    if (loginErrorText) {
      loginErrorText.textContent = "";
      loginErrorText.classList.add("hidden");
    }
    if (userMenuBtn) userMenuBtn.classList.add("hidden");
    if (userMenuPanel) userMenuPanel.classList.add("hidden");
  }

  function hideLoginGate() {
    if (!loginGate) return;
    hideBlockingModal(loginGate);
    if (userMenuBtn) userMenuBtn.classList.remove("hidden");
  }

  function refreshUserMenu() {
    if (!currentUser) {
      if (isAuthBootstrapInFlight) {
        if (userMenuBtn) userMenuBtn.classList.add("hidden");
        if (userMenuWho) userMenuWho.textContent = "--";
        if (userMenuPanel) userMenuPanel.classList.add("hidden");
        hideLoginGate();
        return;
      }
      if (userMenuBtn) {
        userMenuBtn.textContent = "登录";
        userMenuBtn.classList.remove("hidden");
      }
      if (userMenuWho) userMenuWho.textContent = "--";
      if (userMenuPanel) userMenuPanel.classList.add("hidden");
      showLoginGate();
      return;
    }
    if (userMenuBtn) userMenuBtn.textContent = `${currentUser.username}`;
    if (userMenuWho) userMenuWho.textContent = `${currentUser.username} (${currentUser.role})`;
    if (openUserAdminBtn) {
      openUserAdminBtn.classList.toggle("hidden", currentUser.role !== "admin");
    }
    hideLoginGate();
  }

  function renderUserRows(rows) {
    if (!userAdminTableBody) return;
    userAdminTableBody.innerHTML = rows.map((row) => {
      const isLocked = Boolean(row.is_locked);
      const hasTempLock = Boolean(row.temp_lock_until);
      const isLockAction = !isLocked && !hasTempLock;
      const lockActionLabel = isLockAction ? "锁定用户" : "解锁用户";
      const isCurrentLoginUser = String(row.username || "").trim().toLowerCase() === String(currentUser?.username || "").trim().toLowerCase();
      const disableToggleLock = isLockAction && isCurrentLoginUser;
      const status = isLocked
        ? "锁定"
        : (hasTempLock ? `临时限制至 ${String(row.temp_lock_until).replace("T", " ").slice(0, 19)}` : "正常");
      const lastLogin = row.last_login_at ? row.last_login_at.replace("T", " ").slice(0, 19) : "--";
      return `
        <tr data-username="${esc(row.username)}" data-role="${esc(row.role)}" class="${isLocked ? "" : "mapped-row"}">
          <td>${esc(row.username)}</td>
          <td>${esc(row.role)}</td>
          <td>${esc(status)}</td>
          <td>${esc(lastLogin)}</td>
          <td>
            <div class="user-action-buttons">
              <button type="button" class="glass-btn tiny danger-btn js-reset-user-password">修改密码</button>
              <button type="button" class="glass-btn tiny danger-btn${isLockAction ? " primary-lock-btn" : ""} js-toggle-lock-user${disableToggleLock ? " is-disabled" : ""}" data-lock-action="${isLockAction ? "lock" : "unlock"}" ${disableToggleLock ? "disabled" : ""}>${lockActionLabel}</button>
              <button type="button" class="glass-btn tiny danger-btn js-delete-user">删除用户</button>
            </div>
          </td>
        </tr>
      `;
    }).join("");
  }

  async function refreshUserAdminTable() {
    const payload = await adminListUsers();
    renderUserRows(Array.isArray(payload?.users) ? payload.users : []);
  }

  function setupAuthUi() {
    const clearLoginCountdown = () => {
      if (loginLockCountdownTimer) {
        window.clearInterval(loginLockCountdownTimer);
        loginLockCountdownTimer = null;
      }
      loginLockRemainingSeconds = 0;
    };

    const formatRemain = (seconds) => {
      const s = Math.max(0, Math.floor(seconds));
      const mm = String(Math.floor(s / 60)).padStart(2, "0");
      const ss = String(s % 60).padStart(2, "0");
      return `${mm}:${ss}`;
    };

    const startLoginCountdown = (seconds) => {
      clearLoginCountdown();
      loginLockRemainingSeconds = Math.max(1, Number(seconds) || 0);

      const tick = () => {
        if (!loginErrorText) {
          clearLoginCountdown();
          return;
        }
        loginErrorText.textContent = `登录失败次数过多，请 ${formatRemain(loginLockRemainingSeconds)} 后再试`;
        if (loginLockRemainingSeconds <= 0) {
          clearLoginCountdown();
          loginShell?.classList.remove("has-error");
          loginErrorText.classList.add("hidden");
          loginErrorText.textContent = "";
          return;
        }
        loginLockRemainingSeconds -= 1;
      };

      tick();
      loginLockCountdownTimer = window.setInterval(tick, 1000);
    };

    const setLoginError = (message) => {
      if (!loginErrorText) return;
      const text = String(message || "").trim();
      if (!text) {
        clearLoginCountdown();
        loginErrorText.textContent = "";
        loginErrorText.classList.add("hidden");
        loginShell?.classList.remove("has-error");
        return;
      }

      const lockMatch = text.match(/请\s*(\d+)\s*分钟后再试/);
      if (lockMatch) {
        const mins = Number(lockMatch[1]);
        const seconds = Number.isFinite(mins) ? mins * 60 : 0;
        loginErrorText.classList.remove("hidden");
        loginShell?.classList.add("has-error");
        startLoginCountdown(seconds);
        return;
      }

      clearLoginCountdown();
      loginErrorText.textContent = text;
      loginErrorText.classList.remove("hidden");
      loginShell?.classList.add("has-error");
    };

    const submitLogin = async () => {
      const username = String(loginUsername?.value || "").trim();
      const password = String(loginPassword?.value || "");
      if (!username || !password) {
        setLoginError("请输入用户名和密码");
        return;
      }
      try {
        setLoginError("");
        await withModalLoading(loginGate, "登录中...", async () => {
          // Invalidate any older bootstrapAuth result still in flight.
          authStateEpoch += 1;
          await authLogin(username, password);
          currentUser = await authMe();
        });
        if (loginPassword) loginPassword.value = "";
        resetPasswordInputs(loginPassword);
        refreshUserMenu();
        showToast("登录成功");
        saveHomeState();
        await refreshImportCardTimes();
      } catch (err) {
        const msg = String(err?.message || "未知错误");
        if (/401|unauthorized|未登录|未认证/i.test(msg)) {
          storeAuthToken("");
        }
        if (/failed to fetch/i.test(msg)) {
          setLoginError(`登录失败：无法连接后端服务（${importStatusApiBase}）。请确认后端已启动且 CORS 白名单包含当前前端地址。`);
        } else if (/401|unauthorized|未登录|未认证/i.test(msg)) {
          setLoginError("登录失败：会话未建立，请确认前端与后端同域策略/CORS 与 Cookie 设置后重试。");
        } else {
          setLoginError(`登录失败：${msg}`);
        }
      }
    };

    if (loginSubmitBtn) {
      loginSubmitBtn.addEventListener("click", submitLogin);
    }

    [loginPassword, currentPasswordInput, newPasswordInput, newUserPasswordInput, adminResetUserPasswordInput].forEach((inputEl) => {
      enhancePasswordInput(inputEl);
    });

    [loginUsername, loginPassword].forEach((input) => {
      if (!input) return;
      input.addEventListener("keydown", (event) => {
        if (event.key !== "Enter") return;
        event.preventDefault();
        void submitLogin();
      });
    });

    if (loginPassword) {
      enhancePasswordInput(loginPassword, { id: "loginShowPassword" });
    }

    if (loginCancelBtn) {
      loginCancelBtn.addEventListener("click", () => {
        if (loginUsername) loginUsername.value = "";
        if (loginPassword) loginPassword.value = "";
        resetPasswordInputs(loginPassword);
        setLoginError("");
        showLoginGate();
      });
    }

    if (userMenuBtn) {
      userMenuBtn.addEventListener("click", () => {
        if (!currentUser) {
          showLoginGate();
          return;
        }
        if (userMenuPanel) userMenuPanel.classList.toggle("hidden");
      });
    }

    document.addEventListener("click", (event) => {
      if (!userMenuPanel || !userMenuBtn) return;
      if (userMenuPanel.contains(event.target) || userMenuBtn.contains(event.target)) return;
      userMenuPanel.classList.add("hidden");
    });

    if (appToastCloseBtn) {
      appToastCloseBtn.addEventListener("click", () => {
        hideToast(appToastCloseBtn.dataset.toastResult || null);
      });
    }

    if (appToastSecondaryBtn) {
      appToastSecondaryBtn.addEventListener("click", () => {
        hideToast(appToastSecondaryBtn.dataset.toastResult || "secondary");
      });
    }

    if (appToastPrimaryBtn) {
      appToastPrimaryBtn.addEventListener("click", () => {
        hideToast(appToastPrimaryBtn.dataset.toastResult || "primary");
      });
    }

    if (appToast) {
      appToast.addEventListener("click", (event) => {
        if (!toastIsBlocking) return;
        event.stopPropagation();
      });
    }

    if (appToastCopyBtn) {
      appToastCopyBtn.addEventListener("click", async () => {
        if (!lastToastMessage) return;
        try {
          await navigator.clipboard.writeText(lastToastMessage);
          appToastCopyBtn.textContent = "已复制√";
        } catch {
          appToastCopyBtn.textContent = "复制失败";
        }
      });
    }

    if (logoutBtn) {
      logoutBtn.addEventListener("click", async () => {
        try {
          await authLogout();
        } catch {
          // Ignore logout call failure and still clear local state.
        }
        storeAuthToken("");
        currentUser = null;
        refreshUserMenu();
      });
    }

    if (changePasswordBtn) {
      changePasswordBtn.addEventListener("click", () => {
        showBlockingModal(changePasswordModal);
        if (currentPasswordInput) currentPasswordInput.value = "";
        if (newPasswordInput) newPasswordInput.value = "";
        resetPasswordInputs(currentPasswordInput, newPasswordInput);
      });
    }

    if (closeChangePasswordModal) {
      closeChangePasswordModal.addEventListener("click", () => {
        resetPasswordInputs(currentPasswordInput, newPasswordInput);
        hideBlockingModal(changePasswordModal);
      });
    }

    if (maxChangePasswordModal && changePasswordModalShell) {
      maxChangePasswordModal.addEventListener("click", () => {
        const isMaximized = changePasswordModalShell.classList.contains("maximized");
        if (!isMaximized) {
          if (changePasswordModalShell.style.width) {
            changePasswordModalShell.dataset.restoreWidth = changePasswordModalShell.style.width;
          }
          if (changePasswordModalShell.style.height) {
            changePasswordModalShell.dataset.restoreHeight = changePasswordModalShell.style.height;
          }
          changePasswordModalShell.classList.add("maximized");
          changePasswordModalShell.style.width = "";
          changePasswordModalShell.style.height = "";
          maxChangePasswordModal.setAttribute("title", "还原");
        } else {
          changePasswordModalShell.classList.remove("maximized");
          if (changePasswordModalShell.dataset.restoreWidth) {
            changePasswordModalShell.style.width = changePasswordModalShell.dataset.restoreWidth;
          }
          if (changePasswordModalShell.dataset.restoreHeight) {
            changePasswordModalShell.style.height = changePasswordModalShell.dataset.restoreHeight;
          }
          maxChangePasswordModal.setAttribute("title", "最大化");
        }
      });
    }

    if (submitChangePasswordBtn) {
      submitChangePasswordBtn.addEventListener("click", async () => {
        const oldPass = String(currentPasswordInput?.value || "");
        const newPass = String(newPasswordInput?.value || "");
        if (!oldPass || !newPass) {
          showToast("请输入当前密码和新密码", "error");
          return;
        }
        try {
          await withModalLoading(changePasswordModal, "正在修改密码...", async () => {
            await authChangePassword(oldPass, newPass);
          });
          showToast("密码修改成功");
          resetPasswordInputs(currentPasswordInput, newPasswordInput);
          hideBlockingModal(changePasswordModal);
        } catch (err) {
          showToast(`修改失败：${err?.message || "未知错误"}`, "error");
        }
      });
    }

    if (openUserAdminBtn) {
      openUserAdminBtn.addEventListener("click", async () => {
        if (!currentUser || currentUser.role !== "admin") return;
        showBlockingModal(userAdminModal);
        try {
          await withModalLoading(userAdminModal, "正在加载用户...", async () => {
            await refreshUserAdminTable();
          });
        } catch (err) {
          showToast(`加载用户失败：${err?.message || "未知错误"}`, "error");
        }
      });
    }

    if (closeUserAdminModal) {
      closeUserAdminModal.addEventListener("click", () => hideBlockingModal(userAdminModal));
    }

    if (maxUserAdminModal && userAdminModalShell) {
      maxUserAdminModal.addEventListener("click", () => {
        const isMaximized = userAdminModalShell.classList.contains("maximized");
        if (!isMaximized) {
          if (userAdminModalShell.style.width) {
            userAdminModalShell.dataset.restoreWidth = userAdminModalShell.style.width;
          }
          if (userAdminModalShell.style.height) {
            userAdminModalShell.dataset.restoreHeight = userAdminModalShell.style.height;
          }
          userAdminModalShell.classList.add("maximized");
          userAdminModalShell.style.width = "";
          userAdminModalShell.style.height = "";
          maxUserAdminModal.setAttribute("title", "还原");
        } else {
          userAdminModalShell.classList.remove("maximized");
          if (userAdminModalShell.dataset.restoreWidth) {
            userAdminModalShell.style.width = userAdminModalShell.dataset.restoreWidth;
          }
          if (userAdminModalShell.dataset.restoreHeight) {
            userAdminModalShell.style.height = userAdminModalShell.dataset.restoreHeight;
          }
          maxUserAdminModal.setAttribute("title", "最大化");
        }
      });
    }

    const closeCreateUserModalPanel = () => {
      resetPasswordInputs(newUserPasswordInput);
      hideBlockingModal(createUserModal);
    };

    const closeAdminResetUserModalPanel = () => {
      hideBlockingModal(adminResetUserModal);
      selectedAdminResetUsername = "";
      if (adminResetUserPasswordInput) adminResetUserPasswordInput.value = "";
      resetPasswordInputs(adminResetUserPasswordInput);
    };

    if (closeCreateUserModal) {
      closeCreateUserModal.addEventListener("click", closeCreateUserModalPanel);
    }

    if (maxCreateUserModal && createUserModalShell) {
      maxCreateUserModal.addEventListener("click", () => {
        const isMaximized = createUserModalShell.classList.contains("maximized");
        if (!isMaximized) {
          if (createUserModalShell.style.width) {
            createUserModalShell.dataset.restoreWidth = createUserModalShell.style.width;
          }
          if (createUserModalShell.style.height) {
            createUserModalShell.dataset.restoreHeight = createUserModalShell.style.height;
          }
          createUserModalShell.classList.add("maximized");
          createUserModalShell.style.width = "";
          createUserModalShell.style.height = "";
          maxCreateUserModal.setAttribute("title", "还原");
        } else {
          createUserModalShell.classList.remove("maximized");
          if (createUserModalShell.dataset.restoreWidth) {
            createUserModalShell.style.width = createUserModalShell.dataset.restoreWidth;
          }
          if (createUserModalShell.dataset.restoreHeight) {
            createUserModalShell.style.height = createUserModalShell.dataset.restoreHeight;
          }
          maxCreateUserModal.setAttribute("title", "最大化");
        }
      });
    }

    if (cancelCreateUserBtn) {
      cancelCreateUserBtn.addEventListener("click", closeCreateUserModalPanel);
    }

    if (closeAdminResetUserModal) {
      closeAdminResetUserModal.addEventListener("click", closeAdminResetUserModalPanel);
    }

    if (cancelAdminResetUserBtn) {
      cancelAdminResetUserBtn.addEventListener("click", closeAdminResetUserModalPanel);
    }

    if (maxAdminResetUserModal && adminResetUserModalShell) {
      maxAdminResetUserModal.addEventListener("click", () => {
        const isMaximized = adminResetUserModalShell.classList.contains("maximized");
        if (!isMaximized) {
          if (adminResetUserModalShell.style.width) {
            adminResetUserModalShell.dataset.restoreWidth = adminResetUserModalShell.style.width;
          }
          if (adminResetUserModalShell.style.height) {
            adminResetUserModalShell.dataset.restoreHeight = adminResetUserModalShell.style.height;
          }
          adminResetUserModalShell.classList.add("maximized");
          adminResetUserModalShell.style.width = "";
          adminResetUserModalShell.style.height = "";
          maxAdminResetUserModal.setAttribute("title", "还原");
        } else {
          adminResetUserModalShell.classList.remove("maximized");
          if (adminResetUserModalShell.dataset.restoreWidth) {
            adminResetUserModalShell.style.width = adminResetUserModalShell.dataset.restoreWidth;
          }
          if (adminResetUserModalShell.dataset.restoreHeight) {
            adminResetUserModalShell.style.height = adminResetUserModalShell.dataset.restoreHeight;
          }
          maxAdminResetUserModal.setAttribute("title", "最大化");
        }
      });
    }

    if (submitAdminResetUserBtn) {
      submitAdminResetUserBtn.addEventListener("click", async () => {
        const username = String(selectedAdminResetUsername || "").trim();
        const newPassword = String(adminResetUserPasswordInput?.value || "");
        if (!username) {
          showToast("未选择用户", "error");
          return;
        }
        if (!newPassword) {
          showToast("请输入新密码", "error");
          return;
        }
        try {
          await withModalLoading(adminResetUserModal, "正在修改密码...", async () => {
            await adminResetPassword(username, newPassword);
          });
          showToast(`用户 ${username} 密码修改成功`);
          closeAdminResetUserModalPanel();
          await withModalLoading(userAdminModal, "正在刷新列表...", async () => {
            await refreshUserAdminTable();
          });
        } catch (err) {
          showToast(`修改失败：${err?.message || "未知错误"}`, "error");
        }
      });
    }

    if (refreshUsersBtn) {
      refreshUsersBtn.addEventListener("click", async () => {
        try {
          await withModalLoading(userAdminModal, "正在刷新列表...", async () => {
            await refreshUserAdminTable();
          });
        } catch (err) {
          showToast(`刷新失败：${err?.message || "未知错误"}`, "error");
        }
      });
    }

    if (createUserBtn) {
      createUserBtn.addEventListener("click", () => {
        if (newUserNameInput) newUserNameInput.value = "";
        if (newUserPasswordInput) newUserPasswordInput.value = "";
        if (newUserRoleSelect) newUserRoleSelect.value = "user";
        resetPasswordInputs(newUserPasswordInput);
        showBlockingModal(createUserModal);
      });
    }

    if (submitCreateUserBtn) {
      submitCreateUserBtn.addEventListener("click", async () => {
        const username = String(newUserNameInput?.value || "").trim();
        const password = String(newUserPasswordInput?.value || "");
        const role = String(newUserRoleSelect?.value || "user");
        if (!username || !password) {
          showToast("请输入用户名和初始密码", "error");
          return;
        }
        try {
          await withModalLoading(createUserModal, "正在创建用户...", async () => {
            await adminCreateUser(username, password, role);
          });
          if (newUserNameInput) newUserNameInput.value = "";
          if (newUserPasswordInput) newUserPasswordInput.value = "";
          resetPasswordInputs(newUserPasswordInput);
          closeCreateUserModalPanel();
          await withModalLoading(userAdminModal, "正在刷新列表...", async () => {
            await refreshUserAdminTable();
          });
          showToast("用户创建成功");
        } catch (err) {
          showToast(`创建失败：${err?.message || "未知错误"}`, "error");
        }
      });
    }

    if (userAdminTableBody) {
      userAdminTableBody.addEventListener("click", async (event) => {
        const row = event.target.closest("tr[data-username]");
        if (!row) return;
        const username = row.dataset.username || "";
        const role = row.dataset.role || "";

        if (event.target.closest(".js-reset-user-password")) {
          selectedAdminResetUsername = username;
          if (adminResetUserNameInput) adminResetUserNameInput.value = username;
          if (adminResetUserRoleInput) adminResetUserRoleInput.value = role;
          if (adminResetUserPasswordInput) adminResetUserPasswordInput.value = "";
          resetPasswordInputs(adminResetUserPasswordInput);
          showBlockingModal(adminResetUserModal);
          return;
        }

        if (event.target.closest(".js-toggle-lock-user")) {
          const lockButton = event.target.closest(".js-toggle-lock-user");
          if (lockButton?.hasAttribute("disabled")) return;
          const action = lockButton?.dataset.lockAction === "lock" ? "lock" : "unlock";
          const actionLabel = action === "lock" ? "锁定" : "解锁";
          const ok = window.confirm(`确认${actionLabel}用户 ${username} 吗？`);
          if (!ok) return;
          try {
            await adminToggleLock(username, action === "lock");
            await refreshUserAdminTable();
            showToast(`已${actionLabel}用户 ${username}`);
          } catch (err) {
            showToast(`${actionLabel}失败：${err?.message || "未知错误"}`, "error");
          }
          return;
        }

        if (event.target.closest(".js-delete-user")) {
          const ok = window.confirm(`确认删除用户 ${username} 吗？`);
          if (!ok) return;
          try {
            await adminDeleteUser(username);
            await refreshUserAdminTable();
            showToast(`已删除用户 ${username}`);
          } catch (err) {
            showToast(`删除失败：${err?.message || "未知错误"}`, "error");
          }
        }
      });
    }
  }

  async function bootstrapAuth() {
    const epoch = authStateEpoch;
    try {
      const me = await authMe();
      if (epoch !== authStateEpoch) return;
      if (currentUser) return;
      currentUser = me;
    } catch {
      if (epoch !== authStateEpoch) return;
      if (currentUser) return;
      storeAuthToken("");
      currentUser = null;
    } finally {
      isAuthBootstrapInFlight = false;
      refreshUserMenu();
    }
  }

  function switchWorkspaceTab(tabName) {
    if (toastIsBlocking) return;
    const normalized = String(tabName || "home").trim().toLowerCase() || "home";
    const isHome = normalized === "home";
    const isDataQuery = normalized === "dataquery";
    const isImport = normalized === "import";
    if (modeQuery) {
      modeQuery.classList.toggle("active", isHome);
      modeQuery.setAttribute("aria-selected", isHome ? "true" : "false");
    }
    if (modeDataQuery) {
      modeDataQuery.classList.toggle("active", isDataQuery);
      modeDataQuery.setAttribute("aria-selected", isDataQuery ? "true" : "false");
    }
    if (modeUpload) {
      modeUpload.classList.toggle("active", isImport);
      modeUpload.setAttribute("aria-selected", isImport ? "true" : "false");
    }
    if (queryWorkspace) {
      queryWorkspace.classList.toggle("is-active", isHome);
      queryWorkspace.classList.toggle("hidden", !isHome);
    }
    if (dataQueryWorkspace) {
      dataQueryWorkspace.classList.toggle("is-active", isDataQuery);
      dataQueryWorkspace.classList.toggle("hidden", !isDataQuery);
    }
    if (uploadWorkspace) {
      uploadWorkspace.classList.toggle("is-active", isImport);
      uploadWorkspace.classList.toggle("hidden", !isImport);
    }
    if (isImport) {
      if (!importStatusLoadedOnce || importStatusLoadPromise) {
        void withImportCardsRefreshLoading("正在刷新导入状态...", async () => ensureImportCardTimesLoaded());
      }
    }
    if (isDataQuery) {
      void hydrateDataQueryWorkspace();
    }
  }

  function normalizePathToken(value) {
    return String(value || "").trim().toUpperCase();
  }

  async function fetchPathSelection(sourceName, sourceSystem, targetName, tranId) {
    const resp = await apiFetch(`${importStatusApiBase}/path-selection/search`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      timeoutMs: 60000,
      body: JSON.stringify({
        source_name: sourceName,
        source_system: sourceSystem,
        target_name: targetName,
        tran_id: tranId
      })
    });
    if (!resp.ok) {
      throw new Error(parseErrorText(await resp.text(), `status ${resp.status}`));
    }
    return resp.json();
  }

  async function fetchPathMapping(segments, options = {}) {
    const includeLogic = Boolean(options?.includeLogic);
    const includeText = Boolean(options?.includeText);
    const timeoutMs = includeLogic || includeText ? 120000 : 90000;
    const resp = await apiFetch(`${importStatusApiBase}/path-selection/mapping`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      timeoutMs,
      body: JSON.stringify({
        segments,
        include_logic: includeLogic,
        include_text: includeText
      })
    });
    if (!resp.ok) {
      throw new Error(parseErrorText(await resp.text(), `status ${resp.status}`));
    }
    return resp.json();
  }

  async function ensurePathMappingFeatures(required = {}, loadingText = "正在补充路径映射数据...") {
    const needLogic = Boolean(required?.logic) && !activePathMappingFeatures.logic;
    const needText = Boolean(required?.text) && !activePathMappingFeatures.text;
    if (!needLogic && !needText) {
      return activePathMappingResult;
    }

    const segments = getActivePathMappingRequestSegments();
    if (!segments.length) {
      throw new Error("当前没有可用于补充加载的路径段信息。");
    }

    const nextResult = await withAppLoading(loadingText, async () => fetchPathMapping(segments, {
      includeLogic: activePathMappingFeatures.logic || needLogic,
      includeText: activePathMappingFeatures.text || needText
    }));
    activePathMappingResult = nextResult;
    activePathMappingFeatures = getPathMappingFeaturesFromPayload(nextResult);
    return nextResult;
  }

  function setPathSearchBusy(isBusy) {
    pathSearchInFlight = !!isBusy;
    if (pathSearchBtn) {
      pathSearchBtn.disabled = !!isBusy;
      pathSearchBtn.textContent = isBusy ? "查询中..." : "查询";
    }
    if (confirmPathSelectionBtn) {
      confirmPathSelectionBtn.disabled = isBusy || !selectedPathCandidateId;
    }
    if (pathGraphCanvas) {
      pathGraphCanvas.classList.toggle("is-loading", !!isBusy);
    }
  }

  function renderPathGraphEmptyState(title, text) {
    if (!pathGraphCanvas) return;
    pathGraphCanvas.classList.remove("has-results");
    pathGraphCanvas.innerHTML = `
      <div class="path-graph-placeholder">
        <span class="path-graph-placeholder-title">${esc(title)}</span>
        <span class="path-graph-placeholder-text">${esc(text)}</span>
      </div>
    `;
  }

  function getSelectedPathCandidate() {
    const items = activePathSearchResult?.candidate_paths;
    if (!Array.isArray(items) || !items.length) return null;
    return items.find((item) => item.id === selectedPathCandidateId) || null;
  }

  function getAppliedPathCandidate() {
    const items = activePathSearchResult?.candidate_paths;
    if (!Array.isArray(items) || !items.length) return null;
    return items.find((item) => item.id === appliedPathCandidateId) || null;
  }

  function renderPathSelectionSummaryState() {
    if (!pathSelectionSummary) return;

    const result = activePathSearchResult;

    if (!result) {
      pathSelectionSummary.innerHTML = "";
      return;
    }
    pathSelectionSummary.innerHTML = "";
  }

  function renderPathCandidates() {
    if (!pathGraphCanvas) return;

    const result = activePathSearchResult;
    const items = Array.isArray(result?.candidate_paths) ? result.candidate_paths : [];
    if (!items.length) {
      renderPathGraphEmptyState("No Path Found", "当前条件下未找到候选路径，请调整查询条件后重试。");
      renderPathSelectionSummaryState();
      return;
    }

    pathGraphCanvas.classList.add("has-results");
    pathGraphCanvas.innerHTML = `
      <div class="path-candidate-list">
        ${items.map((path) => {
          const isSelected = path.id === selectedPathCandidateId;
          const routeHtml = Array.isArray(path.node_sequence)
            ? path.node_sequence.map((node, index) => `
                <span class="path-node-pill" title="${escAttr(node.object_name || node.id || "")}">${esc(node.id || "")}</span>
                ${index < path.node_sequence.length - 1 ? '<span class="path-route-arrow">→</span>' : ""}
              `).join("")
            : "";
          return `
            <button class="path-candidate-card${isSelected ? ' is-selected' : ''}" type="button" data-path-id="${escAttr(path.id || "")}" title="点击选择该路径并加载字段映射" aria-label="点击选择 Path ${escAttr(String(path.index || ""))} 并加载字段映射">
              <span class="path-candidate-inline">
                <span class="path-candidate-head">
                  <span class="path-candidate-title">Path ${esc(String(path.index || ""))}</span>
                  <span class="path-candidate-count">${formatCount(path.segment_count || 0)} segments</span>
                </span>
                <span class="path-candidate-route">${routeHtml}</span>
              </span>
            </button>
          `;
        }).join("")}
      </div>
    `;
    renderPathSelectionSummaryState();
  }

  function renderEmptyPathResult(message = "请先点击一条候选路径，直接加载下方字段映射结果。") {
    if (!mappingPanelsGrid) return;
    const finalMessage = PATH_MAPPING_REBUILD_MODE ? getFieldMappingRebuildMessage("path") : message;
    destroyActiveAgGrid();
    activePathMappingResult = null;
    activeAlignedRowsForView = [];
    mappingPanelsGrid.innerHTML = `
      <article class="mapping-segment-card soft-panel mapping-empty-card">
        <div class="mapping-segment-meta">
          <div>${PATH_MAPPING_REBUILD_MODE ? "Field Mapping Rebuild" : "Path Result"}</div>
          <div>${esc(finalMessage)}</div>
        </div>
        <table class="mapping-preview-table">
          <tbody>
            <tr><td colspan="7">${esc(finalMessage)}</td></tr>
          </tbody>
        </table>
      </article>
    `;
  }

  function normalizeAlignedMappingKey(value) {
    return String(value || "").trim();
  }

  function cloneAlignedMappingRow(row, segmentCount) {
    const clone = Array.from({ length: segmentCount }, () => null);
    for (let index = 0; index < segmentCount; index += 1) {
      clone[index] = row?.[index] || null;
    }
    return clone;
  }

  function isSupplementedAlignedCell(cell) {
    if (!cell) return false;
    const rowKind = String(cell?.row_kind || "").trim().toLowerCase();
    if (rowKind === "target_only") return true;
    const targetField = String(cell?.target_field || "").trim();
    const sourceField = String(cell?.source_field || "").trim();
    const rule = String(cell?.rule || "").trim();
    const aggr = String(cell?.aggr || "").trim();
    return Boolean(targetField) && !sourceField && !rule && !aggr;
  }

  function getAlignedRowPrimaryCell(row, segmentCount) {
    for (let segmentIndex = segmentCount - 1; segmentIndex >= 0; segmentIndex -= 1) {
      const cell = row?.[segmentIndex] || null;
      const hasContent = Boolean(
        String(cell?.source_field || "").trim() ||
        String(cell?.target_field || "").trim() ||
        String(cell?.rule || "").trim() ||
        String(cell?.aggr || "").trim()
      );
      if (hasContent) {
        return {
          segmentIndex,
          cell
        };
      }
    }
    return {
      segmentIndex: -1,
      cell: null
    };
  }

  function getAlignedRowCategoryRank(cell) {
    if (!cell) return 3;
    const rowKind = String(cell?.row_kind || "").trim().toLowerCase();
    if (rowKind === "source_only") return 1;
    return 0;
  }

  function getAlignedRowRuleRank(cell) {
    if (!cell) return 3;
    const rule = String(cell?.rule || "").trim();
    if (rule) return 0;
    if (isSupplementedAlignedCell(cell)) return 2;
    return 0;
  }

  function buildAlignedSegmentRows(segments) {
    const normalizedSegments = Array.isArray(segments) ? segments : [];
    if (!normalizedSegments.length) return [];

    const segmentCount = normalizedSegments.length;
    const lastSegmentIndex = segmentCount - 1;
    const lastRows = Array.isArray(normalizedSegments[lastSegmentIndex]?.rows) ? normalizedSegments[lastSegmentIndex].rows : [];
    let alignedRows = lastRows.length
      ? lastRows.map((cell) => {
          const row = Array.from({ length: segmentCount }, () => null);
          row[lastSegmentIndex] = cell;
          return row;
        })
      : [Array.from({ length: segmentCount }, () => null)];

    for (let segmentIndex = lastSegmentIndex - 1; segmentIndex >= 0; segmentIndex -= 1) {
      const currentRows = Array.isArray(normalizedSegments[segmentIndex]?.rows) ? normalizedSegments[segmentIndex].rows : [];
      const nextIndex = segmentIndex + 1;
      const nextRowsByKey = new Map();
      const nextRowsWithoutKey = [];

      alignedRows.forEach((row) => {
        const key = normalizeAlignedMappingKey(row?.[nextIndex]?.source_field);
        if (!key) {
          nextRowsWithoutKey.push(cloneAlignedMappingRow(row, segmentCount));
          return;
        }
        if (!nextRowsByKey.has(key)) {
          nextRowsByKey.set(key, []);
        }
        nextRowsByKey.get(key).push(cloneAlignedMappingRow(row, segmentCount));
      });

      const currentRowsByKey = new Map();
      const currentRowsWithoutKey = [];
      currentRows.forEach((cell) => {
        const key = normalizeAlignedMappingKey(cell?.target_field);
        if (!key) {
          currentRowsWithoutKey.push(cell);
          return;
        }
        if (!currentRowsByKey.has(key)) {
          currentRowsByKey.set(key, []);
        }
        currentRowsByKey.get(key).push(cell);
      });

      const nextAlignedRows = [];
      const consumedKeys = new Set();

      alignedRows.forEach((row) => {
        const key = normalizeAlignedMappingKey(row?.[nextIndex]?.source_field);
        if (!key || consumedKeys.has(key)) {
          return;
        }
        consumedKeys.add(key);
        const rightGroup = nextRowsByKey.get(key) || [];
        const leftGroup = currentRowsByKey.get(key) || [];
        if (!leftGroup.length) {
          rightGroup.forEach((rightRow) => {
            nextAlignedRows.push(cloneAlignedMappingRow(rightRow, segmentCount));
          });
          return;
        }
        leftGroup.forEach((leftCell) => {
          rightGroup.forEach((rightRow) => {
            const mergedRow = cloneAlignedMappingRow(rightRow, segmentCount);
            mergedRow[segmentIndex] = leftCell;
            nextAlignedRows.push(mergedRow);
          });
        });
      });

      currentRows.forEach((cell) => {
        const key = normalizeAlignedMappingKey(cell?.target_field);
        if (!key || consumedKeys.has(key)) {
          return;
        }
        consumedKeys.add(key);
        const row = Array.from({ length: segmentCount }, () => null);
        row[segmentIndex] = cell;
        nextAlignedRows.push(row);
      });

      currentRowsWithoutKey.forEach((cell) => {
        const row = Array.from({ length: segmentCount }, () => null);
        row[segmentIndex] = cell;
        nextAlignedRows.push(row);
      });

      nextRowsWithoutKey.forEach((row) => {
        nextAlignedRows.push(cloneAlignedMappingRow(row, segmentCount));
      });

      alignedRows = nextAlignedRows.length ? nextAlignedRows : [Array.from({ length: segmentCount }, () => null)];
    }

    return alignedRows
      .map((row, originalIndex) => ({ row, originalIndex }))
      .sort((left, right) => {
        const leftPrimary = getAlignedRowPrimaryCell(left.row, segmentCount);
        const rightPrimary = getAlignedRowPrimaryCell(right.row, segmentCount);
        if (leftPrimary.segmentIndex !== rightPrimary.segmentIndex) {
          return rightPrimary.segmentIndex - leftPrimary.segmentIndex;
        }

        const leftCategory = getAlignedRowCategoryRank(leftPrimary.cell);
        const rightCategory = getAlignedRowCategoryRank(rightPrimary.cell);
        if (leftCategory !== rightCategory) {
          return leftCategory - rightCategory;
        }

        const leftRuleRank = getAlignedRowRuleRank(leftPrimary.cell);
        const rightRuleRank = getAlignedRowRuleRank(rightPrimary.cell);
        if (leftRuleRank !== rightRuleRank) {
          return leftRuleRank - rightRuleRank;
        }

        const leftCell = leftPrimary.cell || left.row[lastSegmentIndex];
        const rightCell = rightPrimary.cell || right.row[lastSegmentIndex];
        const leftPos = Number.parseInt(String(leftCell?.ruleposit || "").trim(), 10);
        const rightPos = Number.parseInt(String(rightCell?.ruleposit || "").trim(), 10);
        const leftSort = Number.isFinite(leftPos) ? leftPos : Number.POSITIVE_INFINITY;
        const rightSort = Number.isFinite(rightPos) ? rightPos : Number.POSITIVE_INFINITY;
        if (leftSort !== rightSort) {
          return leftSort - rightSort;
        }
        return left.originalIndex - right.originalIndex;
      })
      .map((item) => item.row);
  }

  function getVisibleAlignedSegmentRows(segments) {
    const alignedRows = buildAlignedSegmentRows(segments);
    return alignedRows.filter((row) => {
      return segments.every((segment, segmentIndex) => {
        if (!pathHideNonKeyByStep[segment.index]) {
          return true;
        }
        const cell = row[segmentIndex];
        if (!cell) {
          return true;
        }
        return String(cell?.target_key || "").trim().toUpperCase() === "X";
      });
    });
  }

  function renderMappingStepToggle(segment, extraClass = "") {
    const nonKeyVisible = !pathHideNonKeyByStep[segment.index];
    const className = ["mapping-step-toggle", nonKeyVisible ? "is-active" : "", extraClass]
      .filter(Boolean)
      .join(" ");
    return `
      <label class="${className}" data-step-toggle="${escAttr(String(segment.index || ""))}">
        <span class="mapping-step-toggle-label">Non-key fields</span>
        <span class="mapping-step-toggle-state">${nonKeyVisible ? "ON" : "OFF"}</span>
        <input class="mapping-step-toggle-input" type="checkbox" ${nonKeyVisible ? "checked" : ""} />
      </label>
    `;
  }

  function renderMappingFieldTextToggle(toggleScope, isVisible, extraClass = "", label = "Field text") {
    const className = ["mapping-step-toggle", isVisible ? "is-active" : "", extraClass]
      .filter(Boolean)
      .join(" ");
    return `
      <label class="${className}" data-field-text-toggle="${escAttr(String(toggleScope || ""))}">
        <span class="mapping-step-toggle-label">${esc(label)}</span>
        <span class="mapping-step-toggle-state">${isVisible ? "ON" : "OFF"}</span>
        <input class="mapping-step-toggle-input" type="checkbox" ${isVisible ? "checked" : ""} />
      </label>
    `;
  }

  function renderMappingRuleDisplay(value, options = {}) {
    const raw = String(value || "").trim();
    const logicViewerAttrs = options?.logicViewerAttrs || null;
    const wrapWithTrigger = (markup) => {
      if (!logicViewerAttrs) return markup;
      const attrText = Object.entries(logicViewerAttrs)
        .map(([key, val]) => `${key}="${escAttr(String(val ?? ""))}"`)
        .join(" ");
      return `<button type="button" class="mapping-rule-trigger" ${attrText} title="查看规则内容">${markup}</button>`;
    };
    if (!raw) {
      if (options?.showSupplementedX) {
        return `
          <span class="mapping-rule-badge is-unmapped" aria-label="Supplemented field" title="Supplemented field">
            <svg viewBox="0 0 18 18" aria-hidden="true" focusable="false">
              <path d="M5 5 L13 13"></path>
              <path d="M13 5 L5 13"></path>
            </svg>
          </span>
        `;
      }
      return "";
    }

    const normalized = raw.toUpperCase();
    if (normalized === "DIRECT") {
      return wrapWithTrigger('<span class="mapping-rule-badge is-direct" aria-label="Direct" title="DIRECT">=</span>');
    }
    if (normalized === "TIME") {
      return wrapWithTrigger(`
        <span class="mapping-rule-badge is-time" aria-label="Time" title="TIME">
          <svg viewBox="0 0 18 18" aria-hidden="true" focusable="false">
            <circle cx="9" cy="9" r="6.25"></circle>
            <path d="M9 9 L4.4 9"></path>
            <path d="M9 9 L9 5.8"></path>
          </svg>
        </span>
      `);
    }
    if (normalized === "ROUTINE") {
      return wrapWithTrigger('<span class="mapping-rule-badge is-routine" aria-label="Routine" title="ROUTINE">&lt;/&gt;</span>');
    }
    if (normalized === "FORMULA") {
      return wrapWithTrigger('<span class="mapping-rule-badge is-formula" aria-label="Formula" title="FORMULA">f(x)</span>');
    }
    if (normalized === "ADSO") {
      return wrapWithTrigger('<span class="mapping-rule-badge is-lookup" aria-label="Lookup" title="ADSO">lookup</span>');
    }
    if (normalized === "MASTER") {
      return wrapWithTrigger('<span class="mapping-rule-badge is-master" aria-label="Master" title="MASTER">M</span>');
    }
    if (normalized === "MASTER") {
      return wrapWithTrigger('<span class="mapping-rule-badge is-master" aria-label="Master" title="MASTER">M</span>');
    }
    if (normalized === "CONSTANT") {
      return wrapWithTrigger('<span class="mapping-rule-badge is-constant" aria-label="Constant" title="CONSTANT">C</span>');
    }

    return wrapWithTrigger(esc(raw));
  }

  function renderMappingAggrDisplay(value) {
    const raw = String(value || "").trim();
    if (!raw) return "";

    const normalized = raw.toUpperCase();
    if (normalized === "MAX") {
      return '<span class="mapping-aggr-badge is-max" aria-label="Max" title="MAX">M+</span>';
    }
    if (normalized === "MIN") {
      return '<span class="mapping-aggr-badge is-min" aria-label="Min" title="MIN">M-</span>';
    }
    if (normalized === "MOV") {
      return `
        <span class="mapping-aggr-badge is-mov" aria-label="Move" title="MOV">
          <svg viewBox="0 0 28 12" aria-hidden="true" focusable="false">
            <rect x="1" y="4" width="5" height="4" rx="1.2"></rect>
            <path d="M7 6 H20"></path>
            <path d="M16.5 2.8 L22.8 6 L16.5 9.2"></path>
          </svg>
        </span>
      `;
    }
    if (normalized === "SUM") {
      return '<span class="mapping-aggr-badge is-sum" aria-label="Sum" title="SUM">∑</span>';
    }

    return `<span class="mapping-aggr-badge is-default" title="${escAttr(raw)}">${esc(raw)}</span>`;
  }

  function renderMappingFieldTypeDisplay(value) {
    const raw = String(value || "").trim();
    if (!raw) return "";

    const normalized = raw.toUpperCase();
    if (normalized === "I") {
      return `<span class="mapping-fieldtype-badge is-icon is-i" aria-label="Type I" title="TYP: ${escAttr(raw)}"><span class="mapping-fieldtype-icon-wrap"><img class="mapping-fieldtype-icon" src="Assets/Icons/MasterData.png" alt="" aria-hidden="true" /></span></span>`;
    }
    if (normalized === "K") {
      return `<span class="mapping-fieldtype-badge is-icon is-k" aria-label="Type K" title="TYP: ${escAttr(raw)}"><span class="mapping-fieldtype-icon-wrap"><img class="mapping-fieldtype-icon" src="Assets/Icons/keyfigure.png" alt="" aria-hidden="true" /></span></span>`;
    }
    if (normalized === "U") {
      return `<span class="mapping-fieldtype-badge is-icon is-u" aria-label="Type U" title="TYP: ${escAttr(raw)}"><span class="mapping-fieldtype-icon-wrap"><img class="mapping-fieldtype-icon" src="Assets/Icons/Unit.png" alt="" aria-hidden="true" /></span></span>`;
    }
    if (normalized === "F") {
      return `<span class="mapping-fieldtype-badge is-icon is-f" aria-label="Type F" title="TYP: ${escAttr(raw)}"><span class="mapping-fieldtype-icon-wrap"><img class="mapping-fieldtype-icon" src="Assets/Icons/field.png" alt="" aria-hidden="true" /></span></span>`;
    }
    return `<span class="mapping-fieldtype-badge is-default" aria-label="Type ${escAttr(normalized)}" title="TYP: ${escAttr(raw)}">${esc(raw)}</span>`;
  }

  function getMappingGroupSignature(record, segmentIndex) {
    if (!record) return "";
    const targetField = String(record[`targetField_${segmentIndex}`] || "").trim();
    const targetFieldType = String(record[`targetFieldType_${segmentIndex}`] || "").trim();
    const rule = String(record[`rule_${segmentIndex}`] || "").trim();
    const aggr = String(record[`aggr_${segmentIndex}`] || "").trim();
    if (!targetField && !targetFieldType && !rule && !aggr) return "";
    return `${targetFieldType}\u0001${targetField}\u0001${rule}\u0001${aggr}`;
  }

  function annotateMappingGroupMeta(records, segments) {
    const list = Array.isArray(records) ? records : [];
    const normalizedSegments = Array.isArray(segments) ? segments : [];
    normalizedSegments.forEach((_, segmentIndex) => {
      let groupStart = 0;
      while (groupStart < list.length) {
        const currentRecord = list[groupStart];
        const signature = getMappingGroupSignature(currentRecord, segmentIndex);
        if (!signature) {
          currentRecord[`groupPart_${segmentIndex}`] = "single";
          currentRecord[`groupCount_${segmentIndex}`] = 1;
          groupStart += 1;
          continue;
        }

        let groupEnd = groupStart + 1;
        while (groupEnd < list.length && getMappingGroupSignature(list[groupEnd], segmentIndex) === signature) {
          groupEnd += 1;
        }

        const groupCount = groupEnd - groupStart;
        for (let rowIndex = groupStart; rowIndex < groupEnd; rowIndex += 1) {
          const record = list[rowIndex];
          let part = "single";
          if (groupCount > 1) {
            if (rowIndex === groupStart) {
              part = "start";
            } else if (rowIndex === groupEnd - 1) {
              part = "end";
            } else {
              part = "middle";
            }
          }
          record[`groupPart_${segmentIndex}`] = part;
          record[`groupCount_${segmentIndex}`] = groupCount;
        }

        groupStart = groupEnd;
      }
    });
    return list;
  }

  function getMappingGroupCount(record, segmentIndex) {
    return Number(record?.[`groupCount_${segmentIndex}`] || 1);
  }

  function getMappingGroupPart(record, segmentIndex) {
    return String(record?.[`groupPart_${segmentIndex}`] || "single").trim() || "single";
  }

  function buildGroupedRowSpanCellClass(segmentIndex, alignCenter = false, columnRole = "middle") {
    return (params) => {
      const part = getMappingGroupPart(params?.data, segmentIndex);
      const count = getMappingGroupCount(params?.data, segmentIndex);
      return [
        alignCenter ? "ag-cell-center" : "",
        count > 1 ? "mapping-group-block" : "",
        count > 1 ? `mapping-group-block-row-${part}` : "",
        count > 1 ? `mapping-group-block-col-${columnRole}` : ""
      ].filter(Boolean).join(" ");
    };
  }

  function createStepFieldCellRenderer(fieldClassName = "mapping-step-field-wrap", valueClassName = "mapping-step-field-value", options = {}) {
    return class MappingStepFieldCellRenderer {
      init(params) {
        this.gui = document.createElement("span");
        this.gui.className = fieldClassName;
        this.refresh(params);
      }

      getGui() {
        return this.gui;
      }

      refresh(params) {
        const value = String(params?.value || "").trim();
        this.gui.className = fieldClassName;
        this.gui.innerHTML = value
          ? `${renderContextCopyText(value, valueClassName, String(options.contextCopyLabel || options.copyLabel || "字段名"))}${options.copyable ? renderCopyIconButton(value, String(options.copyLabel || "字段名"), "mapping-copy-btn-inline mapping-copy-btn-cell") : ""}`
          : "";
        return true;
      }
    };
  }

  function createAgGridRuleCellRenderer(segmentIndex = -1) {
    return class MappingRuleCellRenderer {
      init(params) {
        this.gui = document.createElement("span");
        this.refresh(params);
      }
      getGui() {
        return this.gui;
      }
      refresh(params) {
        this.gui.className = "mapping-rowspan-display";
        const data = params?.data || null;
        const targetField = String(data?.[`targetField_${segmentIndex}`] || "").trim();
        const sourceField = String(data?.[`stepSourceField_${segmentIndex}`] || "").trim();
        const aggr = String(data?.[`aggr_${segmentIndex}`] || "").trim();
        const rule = String(params?.value || "").trim();
        const tranId = String(data?.[`tranId_${segmentIndex}`] || "").trim();
        const ruleId = Number(data?.[`ruleId_${segmentIndex}`] || 0);
        const stepId = Number(data?.[`stepId_${segmentIndex}`] || 0);
        const logicEntryCount = Number(data?.[`logicEntryCount_${segmentIndex}`] || 0);
        const showSupplementedX = Boolean(targetField) && !sourceField && !rule && !aggr;
        const canTryLazyLoad = !activePathMappingFeatures.logic && Boolean(rule);
        this.gui.innerHTML = renderMappingRuleDisplay(rule, {
          showSupplementedX,
          logicViewerAttrs: (logicEntryCount > 0 || canTryLazyLoad) ? {
            "data-rule-logic-open": "1",
            "data-segment-index": String(segmentIndex),
            "data-tran-id": tranId,
            "data-rule-id": String(ruleId),
            "data-step-id": String(stepId)
          } : null
        });
        return true;
      }
    };
  }

  function createAgGridAggrCellRenderer(segmentIndex = -1) {
    return class MappingAggrCellRenderer {
      init(params) {
        this.gui = document.createElement("span");
        this.refresh(params);
      }
      getGui() {
        return this.gui;
      }
      refresh(params) {
        this.gui.className = "mapping-rowspan-display";
        this.gui.innerHTML = renderMappingAggrDisplay(params.value);
        return true;
      }
    };
  }

  function createAgGridFieldTypeCellRenderer() {
    return class MappingFieldTypeCellRenderer {
      init(params) {
        this.gui = document.createElement("span");
        this.refresh(params);
      }
      getGui() {
        return this.gui;
      }
      refresh(params) {
        this.gui.className = "mapping-rowspan-display";
        this.gui.innerHTML = renderMappingFieldTypeDisplay(params.value);
        return true;
      }
    };
  }

  function buildAgGridStepAccentPalette(segmentIndex) {
    return {
      bg: "rgba(92, 112, 138, 0.18)",
      border: "rgba(140, 162, 190, 0.3)",
      text: "#edf3fb"
    };
  }

  function buildAgGridStepAccentCellStyle(segmentIndex, edge) {
    const palette = buildAgGridStepAccentPalette(segmentIndex);
    return {
      backgroundColor: palette.bg,
      color: palette.text
    };
  }

  function destroyActiveAgGrid() {
    snapshotActiveAgGridState();
    if (activeAgGridHeaderSyncRaf) {
      window.cancelAnimationFrame(activeAgGridHeaderSyncRaf);
      activeAgGridHeaderSyncRaf = 0;
    }
    if (activeAgGridHeaderResizeObserver) {
      activeAgGridHeaderResizeObserver.disconnect();
      activeAgGridHeaderResizeObserver = null;
    }
    if (activeAgGridApi && typeof activeAgGridApi.destroy === "function") {
      activeAgGridApi.destroy();
    }
    if (activeAgGridBodyViewport && activeAgGridBodyScrollHandler) {
      activeAgGridBodyViewport.removeEventListener("scroll", activeAgGridBodyScrollHandler);
    }
    activeAgGridBodyViewport = null;
    activeAgGridBodyScrollHandler = null;
    activeAgGridApi = null;
  }

  function getAgGridColumnStateAccessor(api, methodName) {
    if (!api) return null;
    if (typeof api[methodName] === "function") {
      return api[methodName].bind(api);
    }
    if (api.columnApi && typeof api.columnApi[methodName] === "function") {
      return api.columnApi[methodName].bind(api.columnApi);
    }
    return null;
  }

  function snapshotActiveAgGridState() {
    if (!activeAgGridApi) return;
    const getColumnState = getAgGridColumnStateAccessor(activeAgGridApi, "getColumnState");
    if (!getColumnState) return;

    const rawState = getColumnState();
    if (!Array.isArray(rawState)) return;

    activeAgGridColumnState = rawState
      .filter((item) => item && item.colId)
      .map((item) => ({
        colId: item.colId,
        width: Number.isFinite(item.width) ? item.width : undefined,
        flex: Number.isFinite(item.flex) ? item.flex : undefined,
        hide: Boolean(item.hide),
        pinned: item.pinned || null,
        sort: item.sort || null,
        sortIndex: Number.isFinite(item.sortIndex) ? item.sortIndex : undefined
      }));
  }

  function restoreAgGridState(api) {
    if (!api || !Array.isArray(activeAgGridColumnState) || !activeAgGridColumnState.length) {
      return false;
    }

    const applyColumnState = getAgGridColumnStateAccessor(api, "applyColumnState");
    if (!applyColumnState) return false;

    try {
      return applyColumnState({
        state: activeAgGridColumnState,
        applyOrder: true
      }) !== false;
    } catch {
      return false;
    }
  }

  function setAgGridOption(api, optionName, optionValue) {
    if (!api) return;
    if (typeof api.setGridOption === "function") {
      api.setGridOption(optionName, optionValue);
      return;
    }
    if (typeof api.updateGridOptions === "function") {
      api.updateGridOptions({ [optionName]: optionValue });
    }
  }

  function syncAgGridGroupHeaderHeight(api, gridHost) {
    const host = gridHost || document.getElementById("mappingAgGrid");
    const shell = host?.closest(".mapping-ag-grid-shell") || null;
    if (!shell) return;

    const headerBlocks = [...shell.querySelectorAll(".mapping-ag-grid-group-header")];
    if (!headerBlocks.length) return;

    const groupCell = shell.querySelector(".ag-header-group-cell");
    const cellStyle = groupCell ? window.getComputedStyle(groupCell) : null;
    const paddingTop = cellStyle ? Number.parseFloat(cellStyle.paddingTop || "0") : 0;
    const paddingBottom = cellStyle ? Number.parseFloat(cellStyle.paddingBottom || "0") : 0;
    const contentHeight = headerBlocks.reduce((maxHeight, block) => {
      const blockHeight = Math.max(block.scrollHeight, block.offsetHeight, 0);
      return Math.max(maxHeight, Math.ceil(blockHeight));
    }, 0);
    const nextHeight = Math.max(44, contentHeight + Math.ceil(paddingTop + paddingBottom));

    if (Math.abs(nextHeight - activeAgGridGroupHeaderHeight) <= 1) {
      return;
    }

    activeAgGridGroupHeaderHeight = nextHeight;
    setAgGridOption(api, "groupHeaderHeight", nextHeight);
    syncMappingSequenceRailMetrics(host, document.getElementById("mappingSequenceRail"));
  }

  function scheduleAgGridGroupHeaderHeightSync(api, gridHost) {
    if (activeAgGridHeaderSyncRaf) {
      window.cancelAnimationFrame(activeAgGridHeaderSyncRaf);
    }
    activeAgGridHeaderSyncRaf = window.requestAnimationFrame(() => {
      activeAgGridHeaderSyncRaf = 0;
      syncAgGridGroupHeaderHeight(api, gridHost);
    });
  }

  function bindAgGridGroupHeaderAutoHeight(api, gridHost) {
    if (activeAgGridHeaderResizeObserver) {
      activeAgGridHeaderResizeObserver.disconnect();
      activeAgGridHeaderResizeObserver = null;
    }

    const shell = gridHost?.closest(".mapping-ag-grid-shell") || null;
    if (!shell || typeof ResizeObserver !== "function") {
      scheduleAgGridGroupHeaderHeightSync(api, gridHost);
      return;
    }

    activeAgGridHeaderResizeObserver = new ResizeObserver(() => {
      scheduleAgGridGroupHeaderHeightSync(api, gridHost);
    });
    activeAgGridHeaderResizeObserver.observe(shell);
    scheduleAgGridGroupHeaderHeightSync(api, gridHost);
  }

  function buildAgGridRowData(segments, alignedRows) {
    const records = alignedRows.map((row, rowIndex) => {
      const record = {
        __rowIndex: rowIndex + 1,
        sourceField: String(row?.[0]?.source_field || "").trim(),
        sourceText: String(row?.[0]?.source_text || "").trim(),
        sourceFieldType: String(row?.[0]?.source_fieldtype || "").trim(),
        sourceKey: String(row?.[0]?.source_key || "").trim()
      };

      segments.forEach((segment, segmentIndex) => {
        const cell = row?.[segmentIndex] || null;
        record[`rule_${segmentIndex}`] = String(cell?.rule || "").trim();
        record[`aggr_${segmentIndex}`] = String(cell?.aggr || "").trim();
        record[`tranId_${segmentIndex}`] = String(cell?.tran_id || "").trim();
        record[`ruleId_${segmentIndex}`] = Number(cell?.rule_id || 0);
        record[`stepId_${segmentIndex}`] = Number(cell?.step_id || 0);
        record[`logicEntryCount_${segmentIndex}`] = Array.isArray(cell?.logic_entries) ? cell.logic_entries.length : 0;
        record[`targetFieldType_${segmentIndex}`] = String(cell?.target_fieldtype || "").trim();
        record[`stepSourceField_${segmentIndex}`] = String(cell?.source_field || "").trim();
        record[`targetField_${segmentIndex}`] = String(cell?.target_field || "").trim();
        record[`targetText_${segmentIndex}`] = String(cell?.target_text || "").trim();
        record[`targetKey_${segmentIndex}`] = String(cell?.target_key || "").trim();
      });

      return record;
    });

    return annotateMappingGroupMeta(records, segments);
  }

  function normalizePathObjectType(value) {
    const normalized = String(value || "").trim().toUpperCase();
    if (normalized === "DATASOURCE" || normalized === "SOURCE") return "RSDS";
    return normalized;
  }

  function countUniqueKeyFields(rows, fieldNameKey, keyFlagKey) {
    const uniqueKeys = new Set();
    (Array.isArray(rows) ? rows : []).forEach((row) => {
      const keyFlag = String(row?.[keyFlagKey] || "").trim().toUpperCase();
      const fieldName = String(row?.[fieldNameKey] || "").trim();
      if (keyFlag === "X" && fieldName) {
        uniqueKeys.add(fieldName.toUpperCase());
      }
    });
    return uniqueKeys.size;
  }

  function formatWithKeyLabel(keyCount) {
    const count = Number(keyCount) || 0;
    return count > 0 ? `${count} keys` : "None";
  }

  function renderWithKeyCompact(keyCount) {
    return `<div class="mapping-step-meta-line">With Key: ${esc(formatWithKeyLabel(keyCount))}</div>`;
  }

  function buildAgGridStepHeaderMeta(segment, segmentIndex) {
    const stepToggleKey = getStepFieldTextToggleKey(segment, segmentIndex);
    const targetTextVisible = isStepTargetFieldTextVisible(segment, segmentIndex);
    const stepLabel = `Step ${String(segment.index || segmentIndex + 1)}`;
    const targetName = String(segment.target || "--").trim() || "--";
    const targetType = String(segment?.target_type || "").trim();
    const normalizedTargetType = normalizePathObjectType(targetType);
    const targetSystem = String(segment?.target_system || "").trim();
    const targetIconMarkup = renderObjectTypeIcon(targetType, targetName);
    const tranIds = Array.isArray(segment?.tran_ids)
      ? segment.tran_ids.map((item) => String(item || "").trim()).filter(Boolean)
      : [];
    const tranLabel = tranIds.length ? `TRANID: ${tranIds.join(", ")}` : "TRANID: --";
    const targetDiag = segment?.diagnostics?.target || {};
    const targetKeyCount = countUniqueKeyFields(segment?.rows, "target_field", "target_key");
    const targetWithKeyLabel = formatWithKeyLabel(targetKeyCount);
    const detailLines = [
      `<div class="mapping-step-header-stack-line mapping-step-header-target-line">${targetIconMarkup}<span class="mapping-step-route-text">${esc(targetName)}</span>${renderCopyIconButton(targetName, "目标技术名", "mapping-copy-btn-inline mapping-copy-btn-header")}</div>`
    ];

    if (normalizedTargetType === "RSDS" && targetSystem) {
      detailLines.push(`<div class="mapping-step-header-stack-line mapping-step-header-tran-line"><span class="mapping-step-tran-text">${esc(`System: ${targetSystem}`)}</span>${renderCopyIconButton(targetSystem, "Target System", "mapping-copy-btn-inline mapping-copy-btn-header")}</div>`);
    } else {
      detailLines.push(`<div class="mapping-step-header-stack-line mapping-step-header-tran-line">${renderTranIdIcon()}<span class="mapping-step-tran-text">${esc(tranLabel)}</span>${renderCopyIconButton(tranIds.join(", "), "转换ID", "mapping-copy-btn-inline mapping-copy-btn-header")}</div>`);
    }

    detailLines.push(renderMappingDiagnosticFieldStatsCompact("target_field", targetDiag, {
      withKeyLabel: normalizedTargetType === "RSDS" || normalizedTargetType === "TRCS" || normalizedTargetType === "ADSO"
        ? targetWithKeyLabel
        : ""
    }));

    return {
      stepLabel,
      routeLabel: targetName,
      routeMarkup: `<div class="mapping-step-header-lines">${detailLines.join("")}</div>`,
      tranLabel,
      tranMarkup: "",
      toggleMarkup: `${renderMappingStepToggle(segment, "mapping-step-toggle-embedded")}${renderMappingFieldTextToggle(`step:${stepToggleKey}`, targetTextVisible, "mapping-step-toggle-embedded")}${renderStepLogicTrigger(segment)}`
    };
  }

  function buildAgGridSourceHeaderMeta(segments) {
    const sourceName = String(activePathSearchResult?.source_name || segments[0]?.source || "--").trim() || "--";
    const sourceType = String(activePathSearchResult?.source_type || segments[0]?.source_type || "").trim();
    const normalizedSourceType = normalizePathObjectType(sourceType);
    const sourceSystem = String(activePathSearchResult?.source_system || "").trim() || "--";
    const sourceIconMarkup = renderObjectTypeIcon(sourceType, sourceName);
    const sourceTextVisible = isSourceFieldTextVisible();
    const sourceDiag = segments[0]?.diagnostics?.source || {};
    const sourceTranIds = Array.isArray(segments?.[0]?.tran_ids)
      ? segments[0].tran_ids.map((item) => String(item || "").trim()).filter(Boolean)
      : [];
    const sourceTranLabel = sourceTranIds.length ? `TRANID: ${sourceTranIds.join(", ")}` : "TRANID: --";
    const sourceKeyCount = countUniqueKeyFields(segments?.[0]?.rows, "source_field", "source_key");
    const sourceWithKeyLabel = formatWithKeyLabel(sourceKeyCount);
    const detailLines = [
      `<div class="mapping-step-header-stack-line mapping-step-header-target-line">${sourceIconMarkup}<span class="mapping-step-route-text">${esc(sourceName)}</span>${renderCopyIconButton(sourceName, "Source 技术名", "mapping-copy-btn-inline mapping-copy-btn-header")}</div>`
    ];

    if (normalizedSourceType === "RSDS") {
      detailLines.push(`<div class="mapping-step-header-stack-line mapping-step-header-tran-line"><span class="mapping-step-tran-text">${esc(`System: ${sourceSystem}`)}</span>${renderCopyIconButton(sourceSystem, "Source System", "mapping-copy-btn-inline mapping-copy-btn-header")}</div>`);
    } else if (normalizedSourceType === "TRCS") {
      detailLines.push(`<div class="mapping-step-header-stack-line mapping-step-header-tran-line">${renderTranIdIcon()}<span class="mapping-step-tran-text">${esc(sourceTranLabel)}</span>${renderCopyIconButton(sourceTranIds.join(", "), "转换ID", "mapping-copy-btn-inline mapping-copy-btn-header")}</div>`);
    } else if (sourceSystem && sourceSystem !== "--") {
      detailLines.push(`<div class="mapping-step-header-stack-line mapping-step-header-tran-line"><span class="mapping-step-tran-text">${esc(`System: ${sourceSystem}`)}</span>${renderCopyIconButton(sourceSystem, "Source System", "mapping-copy-btn-inline mapping-copy-btn-header")}</div>`);
    }

    detailLines.push(renderMappingDiagnosticFieldStatsCompact("source_field", sourceDiag, {
      withKeyLabel: normalizedSourceType === "RSDS" || normalizedSourceType === "TRCS"
        ? sourceWithKeyLabel
        : ""
    }));

    return {
      title: "Source",
      detail: sourceName,
      detailMarkup: `<div class="mapping-step-header-lines">${detailLines.join("")}</div>`,
      extra: "",
      extraMarkup: "",
      toggleMarkup: renderMappingFieldTextToggle("source", sourceTextVisible, "mapping-step-toggle-embedded")
    };
  }

  function createAgGridGroupHeaderComponent() {
    return class MappingGroupHeader {
      init(params) {
        const meta = params?.groupMeta || {};
        const variant = String(params?.variant || "step").trim();
        const toggleMarkup = String(meta.toggleMarkup || "");
        const detailMarkup = String(meta.detailMarkup || "").trim();
        const extraMarkup = String(meta.extraMarkup || "").trim();
        this.gui = document.createElement("div");
        this.gui.className = `mapping-ag-grid-group-header mapping-ag-grid-group-header-${escAttr(variant)}`;
        this.gui.innerHTML = `
          <div class="mapping-ag-grid-group-title">${esc(meta.title || "")}</div>
          <div class="mapping-ag-grid-group-detail">${detailMarkup || esc(meta.detail || "")}</div>
          <div class="mapping-ag-grid-group-extra">${extraMarkup || esc(meta.extra || "")}</div>
          ${toggleMarkup ? `<div class="mapping-ag-grid-group-toggle-wrap">${toggleMarkup}</div>` : ""}
        `;
      }

      getGui() {
        return this.gui;
      }
    };
  }

  function buildAgGridStepHeaderTooltip(segment, segmentIndex) {
    const meta = buildAgGridStepHeaderMeta(segment, segmentIndex);
    const tranLabel = String(meta.tranLabel || "TRANID: --").replace(/^TRANID:\s*/i, "");
    return `${meta.stepLabel}\n${meta.routeLabel}\nTRANID: ${tranLabel}`;
  }

  function buildAgGridSourceHeaderTooltip(segments) {
    const meta = buildAgGridSourceHeaderMeta(segments);
    return `${meta.title}\n${meta.detail}\n${meta.extra}`;
  }

  function buildAgGridColumnDefs(segments) {
    const sourceMeta = buildAgGridSourceHeaderMeta(segments);
    const sourceTextVisible = isSourceFieldTextVisible();
    const sourceGroup = {
      headerName: sourceMeta.title,
      headerTooltip: buildAgGridSourceHeaderTooltip(segments),
      headerGroupComponent: "mappingGroupHeader",
      headerGroupComponentParams: {
        variant: "source",
        groupMeta: sourceMeta
      },
      marryChildren: true,
      children: [
        {
          headerName: "typ",
          field: "sourceFieldType",
          pinned: "left",
          width: 44,
          minWidth: 40,
          maxWidth: 46,
          resizable: true,
          sortable: true,
          cellClass: "ag-cell-center",
          headerClass: "ag-cell-center",
          cellRenderer: createAgGridFieldTypeCellRenderer()
        },
        {
          headerName: "Source Field",
          field: "sourceField",
          pinned: "left",
          width: MAPPING_SOURCE_FIELD_MIN_WIDTH,
          minWidth: MAPPING_SOURCE_FIELD_MIN_WIDTH,
          resizable: true,
          sortable: true,
          cellRenderer: createStepFieldCellRenderer()
        },
        ...(sourceTextVisible ? [
          {
            headerName: "Text",
            field: "sourceText",
            pinned: "left",
            width: MAPPING_FIELD_TEXT_MIN_WIDTH,
            minWidth: MAPPING_FIELD_TEXT_MIN_WIDTH,
            resizable: true,
            sortable: true,
            cellRenderer: createStepFieldCellRenderer("mapping-step-field-wrap", "mapping-step-field-value", {
              contextCopyLabel: "Source Text"
            })
          }
        ] : []),
        {
          headerName: "KEY",
          field: "sourceKey",
          pinned: "left",
          width: 36,
          minWidth: 32,
          maxWidth: 38,
          resizable: true,
          sortable: true,
          cellClass: "ag-cell-center",
          headerClass: "ag-cell-center"
        }
      ]
    };

    const stepGroups = segments.map((segment, segmentIndex) => {
      const children = [];
      const targetTextVisible = isStepTargetFieldTextVisible(segment, segmentIndex);
      const targetFieldColumnRole = targetTextVisible ? "middle" : "end";
      if (pathRuleColumnVisible) {
        children.push({
          headerName: "Rule",
          field: `rule_${segmentIndex}`,
          width: 46,
          minWidth: 40,
          maxWidth: 48,
          resizable: true,
          sortable: true,
          cellClass: buildGroupedRowSpanCellClass(segmentIndex, true, "start"),
          headerClass: "ag-cell-center",
          cellStyle: () => buildAgGridStepAccentCellStyle(segmentIndex, "start"),
          cellRenderer: createAgGridRuleCellRenderer(segmentIndex)
        });
      }
      children.push({
        headerName: "Aggr.",
        field: `aggr_${segmentIndex}`,
        width: 50,
        minWidth: 44,
        maxWidth: 52,
        resizable: true,
        sortable: true,
        cellClass: buildGroupedRowSpanCellClass(segmentIndex, true, "middle"),
        headerClass: "ag-cell-center",
        cellStyle: () => buildAgGridStepAccentCellStyle(segmentIndex, "end"),
        cellRenderer: createAgGridAggrCellRenderer(segmentIndex)
      });
      children.push({
        headerName: "typ",
        field: `targetFieldType_${segmentIndex}`,
        width: 44,
        minWidth: 40,
        maxWidth: 46,
        resizable: true,
        sortable: true,
        cellClass: buildGroupedRowSpanCellClass(segmentIndex, true, "middle"),
        headerClass: "ag-cell-center",
        cellRenderer: createAgGridFieldTypeCellRenderer()
      });
      children.push({
        headerName: "Target Field",
        field: `targetField_${segmentIndex}`,
        width: MAPPING_TARGET_FIELD_MIN_WIDTH,
        minWidth: MAPPING_TARGET_FIELD_MIN_WIDTH,
        resizable: true,
        sortable: true,
        cellClass: buildGroupedRowSpanCellClass(segmentIndex, false, targetFieldColumnRole),
        cellRenderer: createStepFieldCellRenderer("mapping-target-field-wrap", "mapping-target-field-value")
      });
      if (targetTextVisible) {
        children.push({
          headerName: "Text",
          field: `targetText_${segmentIndex}`,
          width: MAPPING_FIELD_TEXT_MIN_WIDTH,
          minWidth: MAPPING_FIELD_TEXT_MIN_WIDTH,
          resizable: true,
          sortable: true,
          cellClass: buildGroupedRowSpanCellClass(segmentIndex, false, "end"),
          cellRenderer: createStepFieldCellRenderer("mapping-target-field-wrap", "mapping-target-field-value", {
            contextCopyLabel: "Target Text"
          })
        });
      }
      children.push({
        headerName: "KEY",
        field: `targetKey_${segmentIndex}`,
        width: 36,
        minWidth: 32,
        maxWidth: 38,
        resizable: true,
        sortable: true,
        cellClass: "ag-cell-center",
        headerClass: "ag-cell-center"
      });

      const stepMeta = buildAgGridStepHeaderMeta(segment, segmentIndex);
      return {
        headerName: stepMeta.stepLabel,
        headerGroupComponent: "mappingGroupHeader",
        headerGroupComponentParams: {
          variant: "step",
          groupMeta: {
            title: stepMeta.stepLabel,
            detail: stepMeta.routeLabel,
            detailMarkup: stepMeta.routeMarkup,
            extra: "",
            extraMarkup: stepMeta.tranMarkup,
            toggleMarkup: stepMeta.toggleMarkup
          }
        },
        children
      };
    });

    return [sourceGroup].concat(stepGroups);
  }

  function renderMappingSequenceRailRows(rowCount) {
    const count = Math.max(0, Number(rowCount) || 0);
    return Array.from({ length: count }, (_, index) => `
      <div class="mapping-sequence-rail-row">
        <span class="mapping-sequence-badge">${esc(String(index + 1))}</span>
      </div>
    `).join("");
  }

  function syncMappingSequenceRailMetrics(gridHost, railHost) {
    if (!gridHost || !railHost) return;
    const shell = gridHost.closest(".mapping-ag-grid-layout")?.querySelector(".mapping-ag-grid-shell") || null;
    const railHead = railHost.querySelector(".mapping-sequence-rail-head") || null;
    const railBody = railHost.querySelector(".mapping-sequence-rail-body") || null;
    const headerRoot = shell?.querySelector(".ag-header") || null;
    const bodyViewport = shell?.querySelector(".ag-body-viewport") || null;
    if (!railHead || !railBody || !headerRoot || !bodyViewport) return;

    railHead.style.height = `${Math.ceil(headerRoot.offsetHeight)}px`;
    railBody.style.height = `${Math.ceil(bodyViewport.clientHeight)}px`;
    railBody.scrollTop = bodyViewport.scrollTop;
  }

  function refreshMappingSequenceRail(api, gridHost, railHost) {
    if (!railHost) return;
    const railBody = railHost.querySelector(".mapping-sequence-rail-body") || null;
    if (!railBody) return;

    const visibleRows = getAgGridDisplayedRowCount(api, activeAlignedRowsForView);
    railBody.innerHTML = renderMappingSequenceRailRows(visibleRows);
    syncMappingSequenceRailMetrics(gridHost, railHost);
  }

  function getCurrentAlignedRowsForActions() {
    const fallbackRows = Array.isArray(activeAlignedRowsForView) ? activeAlignedRowsForView : [];
    if (!activeAgGridApi || typeof activeAgGridApi.forEachNodeAfterFilterAndSort !== "function") {
      return fallbackRows;
    }

    const orderedRows = [];
    activeAgGridApi.forEachNodeAfterFilterAndSort((node) => {
      const rowIndex = Number(node?.data?.__rowIndex || 0);
      if (!Number.isFinite(rowIndex) || rowIndex <= 0) return;
      const row = fallbackRows[rowIndex - 1];
      if (row) {
        orderedRows.push(row);
      }
    });

    return orderedRows.length ? orderedRows : fallbackRows;
  }

  function getAgGridDisplayedRowCount(api, fallbackRows = []) {
    if (!api) return Array.isArray(fallbackRows) ? fallbackRows.length : 0;
    if (typeof api.getDisplayedRowCount === "function") {
      const count = Number(api.getDisplayedRowCount());
      return Number.isFinite(count) ? count : 0;
    }
    return Array.isArray(fallbackRows) ? fallbackRows.length : 0;
  }

  function applyAgGridQuickFilter(api, value) {
    if (!api) return;
    const normalized = String(value || "").trim();
    if (typeof api.setGridOption === "function") {
      api.setGridOption("quickFilterText", normalized);
      return;
    }
    if (typeof api.setQuickFilter === "function") {
      api.setQuickFilter(normalized);
    }
  }

  function updateAgGridResultSummary({ summaryEl, sourceSummary, segmentCount, totalRows, api }) {
    if (!summaryEl) return;
    const visibleRows = getAgGridDisplayedRowCount(api, activeAlignedRowsForView);
    const filterLabel = activeMappingQuickFilter ? ` | Filter: ${activeMappingQuickFilter}` : "";
    summaryEl.textContent = `Source: ${sourceSummary} | Steps: ${formatCount(segmentCount)} | Visible Rows: ${formatCount(visibleRows)} / ${formatCount(totalRows)}${filterLabel}`;
  }

  function syncMappingResultPanelButton() {
    if (!toggleMappingResultPanelBtn || !mappingResultPanel) return;
    const isMaximized = mappingResultPanel.classList.contains("is-maximized");
    toggleMappingResultPanelBtn.textContent = isMaximized ? "恢复窗口" : "放大视图";
    toggleMappingResultPanelBtn.setAttribute("title", isMaximized ? "恢复窗口" : "放大视图");
    toggleMappingResultPanelBtn.setAttribute("aria-pressed", isMaximized ? "true" : "false");
    toggleMappingResultPanelBtn.classList.toggle("is-active", isMaximized);
  }

  function syncDataQueryResultPanelButton() {
    if (!toggleDataQueryResultPanelBtn || !dataQueryResultPanel) return;
    const isMaximized = dataQueryResultPanel.classList.contains("is-maximized");
    toggleDataQueryResultPanelBtn.textContent = isMaximized ? "恢复窗口" : "放大视图";
    toggleDataQueryResultPanelBtn.setAttribute("title", isMaximized ? "恢复窗口" : "放大视图");
    toggleDataQueryResultPanelBtn.setAttribute("aria-pressed", isMaximized ? "true" : "false");
    toggleDataQueryResultPanelBtn.classList.toggle("is-active", isMaximized);
  }

  function setMappingResultPanelMaximized(shouldMaximize) {
    if (!mappingResultPanel) return;
    const nextState = !!shouldMaximize;
    if (nextState && dataQueryResultPanel?.classList.contains("is-maximized")) {
      setDataQueryResultPanelMaximized(false);
    }
    mappingResultPanel.classList.toggle("is-maximized", nextState);
    document.body.classList.toggle("mapping-panel-maximized", nextState);
    syncMappingResultPanelButton();
    window.requestAnimationFrame(() => {
      if (activeAgGridApi && typeof activeAgGridApi.doLayout === "function") {
        activeAgGridApi.doLayout();
      }
      const gridHost = document.getElementById("mappingAgGrid");
      if (gridHost) {
        scheduleAgGridGroupHeaderHeightSync(activeAgGridApi, gridHost);
        syncMappingSequenceRailMetrics(gridHost, document.getElementById("mappingSequenceRail"));
      }
    });
  }

  function toggleMappingResultPanelMaximized() {
    if (!mappingResultPanel) return;
    setMappingResultPanelMaximized(!mappingResultPanel.classList.contains("is-maximized"));
  }

  function setDataQueryResultPanelMaximized(shouldMaximize) {
    if (!dataQueryResultPanel) return;
    const nextState = !!shouldMaximize;
    if (nextState && mappingResultPanel?.classList.contains("is-maximized")) {
      setMappingResultPanelMaximized(false);
    }
    dataQueryResultPanel.classList.toggle("is-maximized", nextState);
    document.body.classList.toggle("data-query-panel-maximized", nextState);
    syncDataQueryResultPanelButton();
    window.requestAnimationFrame(() => {
      if (dataQueryGridApi && typeof dataQueryGridApi.doLayout === "function") {
        dataQueryGridApi.doLayout();
      }
    });
  }

  function toggleDataQueryResultPanelMaximized() {
    if (!dataQueryResultPanel) return;
    setDataQueryResultPanelMaximized(!dataQueryResultPanel.classList.contains("is-maximized"));
  }

  function formatDiagnosticCount(value) {
    const count = Number(value);
    return Number.isFinite(count) ? formatCount(count) : "--";
  }

  function renderMappingDiagnosticObject(roleLabel, objectName, displayName, objectType, extraInfo = "") {
    const technicalName = String(objectName || "--").trim() || "--";
    const businessName = String(displayName || "").trim();
    const typeLabel = String(objectType || "").trim();
    const metaParts = [typeLabel, String(extraInfo || "").trim()].filter(Boolean);
    return `
      <div class="mapping-diagnostic-object">
        <div class="mapping-diagnostic-object-role">${esc(roleLabel)}</div>
        <div class="mapping-diagnostic-object-tech-row">
          <div class="mapping-diagnostic-object-tech">${esc(technicalName)}</div>
          ${technicalName && technicalName !== "--" ? renderCopyIconButton(technicalName, `${roleLabel} 技术名`, "mapping-copy-btn-inline mapping-copy-btn-header") : ""}
        </div>
        ${businessName ? `<div class="mapping-diagnostic-object-name">${esc(businessName)}</div>` : ""}
        ${metaParts.length ? `<div class="mapping-diagnostic-object-meta">${esc(metaParts.join(" | "))}</div>` : ""}
      </div>
    `;
  }

  function renderMappingDiagnosticFieldStatsCompact(fieldLabel, diagnostics, options = {}) {
    const diag = diagnostics || {};
    const uniqueCount = formatDiagnosticCount(diag.unique_field_count);
    const withKeyLabel = String(options.withKeyLabel || "").trim();
    if (!diag.comparison_available) {
      return `
        <div class="mapping-step-meta-shell">
          <div class="mapping-step-meta-lines">
            <div class="mapping-step-meta-line mapping-step-meta-line-inline">
              <span>去重计数: ${uniqueCount}</span>
              <span>对比结果: <span class="mapping-diagnostic-result-badge has-difference">未校验</span></span>
            </div>
            ${withKeyLabel ? `<div class="mapping-step-meta-line">With Key: ${esc(withKeyLabel)}</div>` : ""}
          </div>
        </div>
      `;
    }

    const diffBadgeClass = diag.has_difference
      ? "mapping-diagnostic-result-badge has-difference"
      : "mapping-diagnostic-result-badge is-match";
    const diffText = diag.has_difference ? `有差异 ${formatDiagnosticCount(diag.difference_count)} 条` : "无差异";
    const adsoTableLine = diag.adso_table_name
      ? `<div class="mapping-step-meta-line mapping-step-meta-line-inline"><span>ADSO表: ${esc(diag.adso_table_name)}</span>${renderCopyIconButton(diag.adso_table_name, "ADSO表技术名", "mapping-copy-btn-inline mapping-copy-btn-header")}${withKeyLabel ? `<span>With Key: ${esc(withKeyLabel)}</span>` : ""}</div>`
      : withKeyLabel
        ? `<div class="mapping-step-meta-line">With Key: ${esc(withKeyLabel)}</div>`
        : "";

    return `
      <div class="mapping-step-meta-shell">
        <div class="mapping-step-meta-lines">
          <div class="mapping-step-meta-line mapping-step-meta-line-inline">
            <span>去重计数: ${uniqueCount}</span>
            <span>对比结果: <span class="${diffBadgeClass}">${esc(diffText)}</span></span>
          </div>
          ${adsoTableLine}
        </div>
      </div>
    `;
  }

  function renderMappingDiagnosticFieldStats(fieldLabel, diagnostics) {
    const diag = diagnostics || {};
    const uniqueCount = formatDiagnosticCount(diag.unique_field_count);
    if (!diag.comparison_available) {
      return `
        <div class="mapping-diagnostic-stat-block">
          <div class="mapping-diagnostic-stat-title">${esc(fieldLabel)}</div>
          <div class="mapping-diagnostic-stat-line">去重计数: ${uniqueCount}</div>
          <div class="mapping-diagnostic-stat-line">对比结果: <span class="mapping-diagnostic-result-badge has-difference">未校验</span></div>
        </div>
      `;
    }

    const diffCount = formatDiagnosticCount(diag.difference_count);
    const diffBadgeClass = diag.has_difference
      ? "mapping-diagnostic-result-badge has-difference"
      : "mapping-diagnostic-result-badge is-match";
    const diffText = diag.has_difference
      ? `有差异 ${diffCount} 条（缺失 ${formatDiagnosticCount(diag.missing_count)} / 额外 ${formatDiagnosticCount(diag.extra_count)}）`
      : "无差异";
    const adsoTableLine = diag.adso_table_name
      ? `<div class="mapping-diagnostic-stat-line">ADSO取表: ${esc(diag.adso_table_name)}</div>`
      : diag.adso_table_suffix
        ? `<div class="mapping-diagnostic-stat-line">ADSO取表: A表 ${esc(String(diag.adso_table_suffix || ""))}</div>`
        : "";

    return `
      <div class="mapping-diagnostic-stat-block">
        <div class="mapping-diagnostic-stat-title">${esc(fieldLabel)}</div>
        <div class="mapping-diagnostic-stat-line">去重计数: ${uniqueCount}</div>
        <div class="mapping-diagnostic-stat-line">对比结果: <span class="${diffBadgeClass}">${esc(diffText)}</span></div>
        ${adsoTableLine}
      </div>
    `;
  }

  function renderMappingSourceDiagnostics(mappingResult) {
    return "";
  }

  function renderAgGridPathMapping(mappingResult) {
    if (!mappingPanelsGrid) return;
    destroyActiveAgGrid();

    const agGridLib = window.agGrid || null;
    if (!agGridLib) {
      renderEmptyPathResult("AG Grid 资源未加载成功，请刷新页面后重试。");
      return;
    }

    const segments = Array.isArray(mappingResult?.segments) ? mappingResult.segments : [];
    const alignedRows = getVisibleAlignedSegmentRows(segments);
    const allFieldTextVisible = areAllFieldTextTogglesVisible();
    activeAlignedRowsForView = alignedRows;
    const sourceSummary = [
      String(activePathSearchResult?.source_name || segments[0]?.source || "").trim(),
      String(activePathSearchResult?.source_system || "").trim()
    ].filter(Boolean).join(" | ") || "--";

    mappingPanelsGrid.innerHTML = `
      <article class="mapping-ag-grid-card soft-panel">
        <div class="mapping-ag-grid-topbar">
          <div id="mappingAgGridSummary" class="mapping-ag-grid-summary">Source: ${esc(sourceSummary)} | Steps: ${formatCount(segments.length)} | Visible Rows: ${formatCount(alignedRows.length)} / ${formatCount(alignedRows.length)}</div>
          <div class="mapping-ag-grid-controls">
            <div class="mapping-ag-grid-filter-wrap">
              <input id="mappingAgGridQuickFilterInput" class="glass-input mapping-ag-grid-filter-input" type="text" placeholder="Filter fields / rule / aggr / key" value="${escAttr(activeMappingQuickFilter)}" autocomplete="off" />
              <button id="mappingAgGridQuickFilterClearBtn" class="glass-btn tiny mapping-ag-grid-filter-clear${activeMappingQuickFilter ? "" : " hidden"}" type="button">Clear</button>
            </div>
            ${renderMappingFieldTextToggle("all", allFieldTextVisible, "mapping-ag-grid-global-toggle", "All fields text")}
          </div>
        </div>
        ${renderMappingSourceDiagnostics(mappingResult)}
        <div class="mapping-ag-grid-layout">
          <div id="mappingSequenceRail" class="mapping-sequence-rail" aria-label="Row sequence">
            <div class="mapping-sequence-rail-head" aria-hidden="true"></div>
            <div class="mapping-sequence-rail-body">${renderMappingSequenceRailRows(alignedRows.length)}</div>
          </div>
          <div class="ag-theme-quartz mapping-ag-grid-shell">
            <div id="mappingAgGrid" class="mapping-ag-grid"></div>
          </div>
        </div>
      </article>
    `;

    const gridHost = document.getElementById("mappingAgGrid");
    const sequenceRail = document.getElementById("mappingSequenceRail");
    if (!gridHost) return;

    const gridOptions = {
      components: {
        mappingGroupHeader: createAgGridGroupHeaderComponent()
      },
      rowSelection: {
        mode: "singleRow",
        enableClickSelection: true,
        checkboxes: false,
        headerCheckbox: false
      },
      defaultColDef: {
        sortable: true,
        resizable: true,
        suppressMovable: false,
        filter: false,
        wrapHeaderText: false,
        cellDataType: false
      },
      rowHeight: 30,
      headerHeight: 32,
      groupHeaderHeight: activeAgGridGroupHeaderHeight,
      animateRows: false,
      suppressRowTransform: true,
      suppressColumnVirtualisation: true,
      suppressDragLeaveHidesColumns: true,
      quickFilterText: activeMappingQuickFilter,
      columnDefs: buildAgGridColumnDefs(segments),
      rowData: buildAgGridRowData(segments, alignedRows),
      overlayNoRowsTemplate: '<span style="padding:12px;display:inline-block;">当前路径没有可展示的字段映射。</span>'
    };

    try {
      if (typeof agGridLib.createGrid === "function") {
        activeAgGridApi = agGridLib.createGrid(gridHost, gridOptions);
      } else if (typeof agGridLib.Grid === "function") {
        new agGridLib.Grid(gridHost, gridOptions);
        activeAgGridApi = gridOptions.api || null;
      } else {
        throw new Error("AG Grid createGrid API unavailable");
      }
    } catch (error) {
      const rawMsg = String(error?.message || error || "Unknown AG Grid error").trim();
      gridHost.innerHTML = `<div style="padding:16px;color:#ffd7d7;">AG Grid 初始化失败: ${esc(rawMsg)}</div>`;
      return;
    }

    const shouldRestoreGridState = !skipAgGridStateRestoreOnce;
    skipAgGridStateRestoreOnce = false;
    const restoredGridState = shouldRestoreGridState ? restoreAgGridState(activeAgGridApi) : false;
    const summaryEl = document.getElementById("mappingAgGridSummary");
    const quickFilterInput = document.getElementById("mappingAgGridQuickFilterInput");
    const quickFilterClearBtn = document.getElementById("mappingAgGridQuickFilterClearBtn");

    const syncAgGridToolbarState = () => {
      updateAgGridResultSummary({
        summaryEl,
        sourceSummary,
        segmentCount: segments.length,
        totalRows: alignedRows.length,
        api: activeAgGridApi
      });
      if (quickFilterClearBtn) {
        quickFilterClearBtn.classList.toggle("hidden", !activeMappingQuickFilter);
      }
      refreshMappingSequenceRail(activeAgGridApi, gridHost, sequenceRail);
    };

    if (activeAgGridApi && typeof activeAgGridApi.addEventListener === "function") {
      activeAgGridApi.addEventListener("filterChanged", syncAgGridToolbarState);
      activeAgGridApi.addEventListener("sortChanged", syncAgGridToolbarState);
      activeAgGridApi.addEventListener("modelUpdated", syncAgGridToolbarState);
    }

    if (quickFilterInput) {
      quickFilterInput.addEventListener("input", (event) => {
        activeMappingQuickFilter = String(event.target?.value || "").trim();
        applyAgGridQuickFilter(activeAgGridApi, activeMappingQuickFilter);
        syncAgGridToolbarState();
      });
    }

    if (quickFilterClearBtn) {
      quickFilterClearBtn.addEventListener("click", () => {
        activeMappingQuickFilter = "";
        if (quickFilterInput) {
          quickFilterInput.value = "";
          quickFilterInput.focus();
        }
        applyAgGridQuickFilter(activeAgGridApi, "");
        syncAgGridToolbarState();
      });
    }

    if (activeAgGridApi && typeof activeAgGridApi.refreshHeader === "function") {
      activeAgGridApi.refreshHeader();
    }

    activeAgGridBodyViewport = gridHost.closest(".mapping-ag-grid-layout")?.querySelector(".mapping-ag-grid-shell .ag-body-viewport") || null;
    activeAgGridBodyScrollHandler = () => {
      if (!sequenceRail) return;
      const railBody = sequenceRail.querySelector(".mapping-sequence-rail-body") || null;
      if (!railBody || !activeAgGridBodyViewport) return;
      railBody.scrollTop = activeAgGridBodyViewport.scrollTop;
    };
    if (activeAgGridBodyViewport && activeAgGridBodyScrollHandler) {
      activeAgGridBodyViewport.addEventListener("scroll", activeAgGridBodyScrollHandler, { passive: true });
    }

    bindAgGridGroupHeaderAutoHeight(activeAgGridApi, gridHost);

    if (!restoredGridState && activeAgGridApi) {
      window.requestAnimationFrame(() => {
        try {
          if (typeof activeAgGridApi.doLayout === "function") {
            activeAgGridApi.doLayout();
          }
          if (typeof activeAgGridApi.getAllDisplayedColumns === "function" && typeof activeAgGridApi.autoSizeColumns === "function") {
            const displayedColumns = activeAgGridApi.getAllDisplayedColumns() || [];
            const leadColumns = displayedColumns
              .slice(0, Math.min(displayedColumns.length, 4))
              .map((column) => (typeof column?.getColId === "function" ? column.getColId() : ""))
              .filter(Boolean);
            if (leadColumns.length) {
              activeAgGridApi.autoSizeColumns(leadColumns);
            }
          }
        } catch {
          // Keep default AG Grid sizing if auto-size is unavailable.
        }
        scheduleAgGridGroupHeaderHeightSync(activeAgGridApi, gridHost);
        syncAgGridToolbarState();
      });
    }

    syncAgGridToolbarState();
    window.requestAnimationFrame(() => {
      syncMappingSequenceRailMetrics(gridHost, sequenceRail);
    });
  }

  async function writeTextToClipboard(text) {
    const normalized = String(text || "");
    if (!normalized) {
      throw new Error("empty text");
    }
    if (navigator?.clipboard?.writeText) {
      await navigator.clipboard.writeText(normalized);
      return;
    }

    const helper = document.createElement("textarea");
    helper.value = normalized;
    helper.setAttribute("readonly", "readonly");
    helper.style.position = "fixed";
    helper.style.opacity = "0";
    helper.style.pointerEvents = "none";
    document.body.appendChild(helper);
    helper.focus();
    helper.select();
    const success = document.execCommand("copy");
    document.body.removeChild(helper);
    if (!success) {
      throw new Error("copy failed");
    }
  }

  async function copyLastStepSourceKeyFields() {
    const segments = Array.isArray(activePathMappingResult?.segments) ? activePathMappingResult.segments : [];
    if (!segments.length) {
      showToast("请先确定一条路径并等待字段映射加载完成。", "error");
      return;
    }

    const lastSegmentIndex = segments.length - 1;
    const alignedRows = getCurrentAlignedRowsForActions();
    const fields = alignedRows
      .filter((row) => String(row?.[lastSegmentIndex]?.target_key || "").trim().toUpperCase() === "X")
      .map((row) => String(row?.[0]?.source_field || "").trim())
      .filter(Boolean);

    const uniqueFields = [];
    const seen = new Set();
    fields.forEach((field) => {
      if (seen.has(field)) return;
      seen.add(field);
      uniqueFields.push(field);
    });

    if (!uniqueFields.length) {
      showToast("最后一个 Step 没有可复制的 key source field。", "error");
      return;
    }

    const text = uniqueFields.join(",");
    try {
      await writeTextToClipboard(text);
      showToast(`已复制 ${formatCount(uniqueFields.length)} 个 source key fields。`);
    } catch (error) {
      const rawMsg = String(error?.message || "").trim();
      showToast(`复制失败。${rawMsg ? ` 详情: ${rawMsg}` : ""}`, "error");
    }
  }

  function renderAlignedPathMapping(mappingResult) {
    if (!mappingPanelsGrid) return;
    const segments = Array.isArray(mappingResult?.segments) ? mappingResult.segments : [];
    if (!segments.length) {
      renderEmptyPathResult("当前路径没有可展示的字段映射。");
      return;
    }
    renderAgGridPathMapping(mappingResult);
  }

  function formatLogicEntriesForExcel(entries, mode = "raw") {
    const list = Array.isArray(entries) ? entries : [];
    if (!list.length) return null;
    const formatter = mode === "display" ? getLogicEntryContentDisplay : getLogicEntryContentRaw;
    const blocks = list
      .map((entry) => {
        const label = getLogicEntryTitle(entry);
        const content = formatter(entry);
        return content ? `${label}\n${content}` : label;
      })
      .filter(Boolean);
    return blocks.length ? blocks.join("\n\n----------------\n\n") : null;
  }

  function cloneExcelPayload(value) {
    if (value == null) return value;
    if (typeof window.structuredClone === "function") {
      return window.structuredClone(value);
    }
    return JSON.parse(JSON.stringify(value));
  }

  function copyExcelJsCellStyle(sourceCell, targetCell) {
    if (!sourceCell || !targetCell) return;
    targetCell.style = cloneExcelPayload(sourceCell.style || {});
  }

  function copyExcelJsColumnStyle(sourceColumn, targetColumn) {
    if (!sourceColumn || !targetColumn) return;
    targetColumn.width = sourceColumn.width;
    if (sourceColumn.hidden != null) {
      targetColumn.hidden = sourceColumn.hidden;
    }
    if (sourceColumn.style && Object.keys(sourceColumn.style).length) {
      targetColumn.style = cloneExcelPayload(sourceColumn.style);
    }
  }

  function cloneExcelCellStyle(cell) {
    if (!cell) return {};
    return cloneExcelPayload(cell.style || {}) || {};
  }

  function setExcelCellFill(cell, fill) {
    if (!cell) return;
    const nextStyle = cloneExcelCellStyle(cell);
    nextStyle.fill = cloneExcelPayload(fill);
    cell.style = nextStyle;
  }

  function getAlignedRuleContentValue(cell) {
    const rawValue = formatLogicEntriesForExcel(Array.isArray(cell?.logic_entries) ? cell.logic_entries : [], "display");
    if (!rawValue) return null;
    return String(rawValue)
      .replace(/^\s*(Formula|Routine|Constant)\s*/i, "")
      .replace(/\n\n----------------\n\n\s*(Formula|Routine|Constant)\s*/gi, "\n\n----------------\n\n")
      .trim() || null;
  }

  function buildExportStepHeaderValues(segment, segmentIndex, alignedRows) {
    const rows = Array.isArray(alignedRows) ? alignedRows : [];
    const stepSourceSystemValue = rows
      .map((row) => String(row?.[segmentIndex]?.source_system || "").trim())
      .find(Boolean) || "--";
    const tranText = Array.isArray(segment?.tran_ids)
      ? segment.tran_ids.map((item) => String(item || "").trim()).filter(Boolean).join(", ")
      : "";
    const routines = getStepRoutineEntriesByKind(segment);
    const globalEntries = Array.isArray(routines.GLOBAL) ? routines.GLOBAL : [];
    const globalFirst = globalEntries.length > 0 ? [globalEntries[0]] : [];
    const globalSecond = globalEntries.length > 1 ? globalEntries.slice(1) : [];
    const routineValues = [
      formatStepRoutineContent(routines.START),
      formatStepRoutineContent(routines.END),
      formatStepRoutineContent(routines.EXPERT),
      formatStepRoutineContent(globalFirst),
      formatStepRoutineContent(globalSecond)
    ];

    return {
      stepTitle: `Step ${segment.index || segmentIndex + 1}: ${segment.source || "--"} -> ${segment.target || "--"}`,
      transformation: tranText || "None",
      sourcesys: stepSourceSystemValue,
      sourcetype: String(segment?.source_type || "").trim() || "--",
      sourcetable: String(segment?.source || "").trim() || "--",
      targettype: String(segment?.target_type || "").trim() || "--",
      targettable: String(segment?.target || "").trim() || "--",
      startroutine: routineValues[0] || "None",
      endroutine: routineValues[1] || "None",
      expertroutine: routineValues[2] || "None",
      global1: routineValues[3] || "None",
      global2: routineValues[4] || "None",
      routineValues
    };
  }

  function normalizeAlignedExportTypeLabel(value) {
    const raw = String(value || "").trim();
    if (!raw) return null;
    const upper = raw.toUpperCase();
    if (upper === "F") return "Field";
    if (upper === "I") return "InfoObject";
    if (upper === "K") return "KeyFigure";
    if (upper === "U") return "Unit";
    return raw;
  }

  function getAlignedExportGroupSignature(cell) {
    const targetField = String(cell?.target_field || "").trim();
    const targetFieldType = String(cell?.target_fieldtype || "").trim();
    const rule = String(cell?.rule || "").trim();
    const aggr = String(cell?.aggr || "").trim();
    if (!targetField && !targetFieldType && !rule && !aggr) return "";
    return `${targetFieldType}\u0001${targetField}\u0001${rule}\u0001${aggr}`;
  }

  function collectAlignedExportGroupsForSegment(alignedRows, segmentIndex) {
    const rows = Array.isArray(alignedRows) ? alignedRows : [];
    const groups = [];
    let start = 0;
    while (start < rows.length) {
      const signature = getAlignedExportGroupSignature(rows[start]?.[segmentIndex]);
      if (!signature) {
        start += 1;
        continue;
      }
      let end = start + 1;
      while (end < rows.length && getAlignedExportGroupSignature(rows[end]?.[segmentIndex]) === signature) {
        end += 1;
      }
      if (end - start > 1) {
        groups.push({ start, end: end - 1 });
      }
      start = end;
    }
    return groups;
  }

  function applyExcelJsOuterBorder(worksheet, startRow, endRow, startCol, endCol, borderStyle) {
    for (let row = startRow; row <= endRow; row += 1) {
      for (let col = startCol; col <= endCol; col += 1) {
        const cell = worksheet.getCell(row, col);
        const existing = cell.border || {};
        const next = { ...existing };
        if (row === startRow) next.top = borderStyle;
        if (row === endRow) next.bottom = borderStyle;
        if (col === startCol) next.left = borderStyle;
        if (col === endCol) next.right = borderStyle;
        cell.border = next;
      }
    }
  }

  function getStepRoutineEntriesByKind(segment) {
    const groups = Array.isArray(segment?.step_logic) ? segment.step_logic : [];
    const entries = groups.flatMap((group) => Array.isArray(group?.entries) ? group.entries : []);
    const result = {
      START: [],
      END: [],
      EXPERT: [],
      GLOBAL: []
    };
    entries.forEach((entry) => {
      const kind = normalizeLogicKind(entry?.kind);
      if (result[kind]) {
        result[kind].push(entry);
      }
    });
    return result;
  }

  function formatStepRoutineContent(entries) {
    const list = Array.isArray(entries) ? entries : [];
    if (!list.length) return "None";
    const blocks = list.map((entry) => {
      const content = getLogicEntryContentDisplay(entry) || getLogicEntryContentRaw(entry);
      const title = getLogicEntryDisplayTitle(entry);
      const kindLabel = formatLogicKindLabel(entry?.kind);
      if (!content) return title || kindLabel;
      if (!title || title === kindLabel) return content;
      return `${title}\n${content}`;
    }).filter(Boolean);
    return blocks.length ? blocks.join("\n\n----------------\n\n") : "None";
  }

  function normalizeTemplateToken(value) {
    return String(value || "")
      .trim()
      .toLowerCase()
      .replace(/[^a-z0-9]/g, "");
  }

  function ensureAlignedTemplateBlockCapacity(worksheet, segmentCount, anchors) {
    const requiredBlockCount = Math.max(1, Number(segmentCount) || 0);
    while (anchors.stepStartColumns.length < requiredBlockCount) {
      const sourceStart = anchors.stepStartColumns[anchors.stepStartColumns.length - 1] || 1;
      const targetStart = sourceStart + anchors.blockWidth;
      for (let offset = 0; offset < anchors.blockWidth; offset += 1) {
        copyExcelJsColumnStyle(worksheet.getColumn(sourceStart + offset), worksheet.getColumn(targetStart + offset));
      }
      for (let rowIndex = 1; rowIndex <= worksheet.rowCount; rowIndex += 1) {
        for (let offset = 0; offset < anchors.blockWidth; offset += 1) {
          const sourceCell = worksheet.getCell(rowIndex, sourceStart + offset);
          const targetCell = worksheet.getCell(rowIndex, targetStart + offset);
          copyExcelJsCellStyle(sourceCell, targetCell);
          if (rowIndex <= Number(anchors.headerRow || 0)) {
            targetCell.value = cloneExcelPayload(sourceCell.value);
          }
        }
      }
      anchors.stepStartColumns.push(targetStart);
    }
  }

  function ensureAlignedTemplateRowCapacity(worksheet, requiredRowCount) {
    const nextRequiredRowCount = Math.max(1, Number(requiredRowCount) || 1);
    while (worksheet.rowCount < nextRequiredRowCount) {
      const targetRowIndex = worksheet.rowCount + 1;
      const styleRowIndex = Math.max(1, targetRowIndex - 1);
      const styleSourceRow = worksheet.getRow(styleRowIndex);
      const targetRow = worksheet.getRow(targetRowIndex);
      targetRow.height = styleSourceRow.height;
      for (let columnIndex = 1; columnIndex <= worksheet.columnCount; columnIndex += 1) {
        copyExcelJsCellStyle(
          worksheet.getCell(styleRowIndex, columnIndex),
          worksheet.getCell(targetRowIndex, columnIndex)
        );
      }
    }
  }

  function getAlignedTemplateAnchors(worksheet) {
    const stepTitleRow = PATH_EXPORT_DEFAULT_STEP_TITLE_ROW;
    const headerRow = PATH_EXPORT_DEFAULT_HEADER_ROW;
    const firstStepStart = 1;
    const blockWidth = PATH_EXPORT_BLOCK_WIDTH;
    const stepStartColumns = [firstStepStart];

    const stepValueRows = {};
    for (let rowIndex = 1; rowIndex < headerRow; rowIndex += 1) {
      const token = normalizeTemplateToken(worksheet.getCell(rowIndex, firstStepStart).value);
      if (!token || stepValueRows[token]) continue;
      stepValueRows[token] = rowIndex;
    }

    const requiredStepValueRows = [
      ["Transformation", "transformation"],
      ["Source Sys.", "sourcesys"],
      ["Source Type", "sourcetype"],
      ["Source Table", "sourcetable"],
      ["Target Type", "targettype"],
      ["Target Table", "targettable"],
      ["Start Routine", "startroutine"],
      ["End Routine", "endroutine"],
      ["Expert Routine", "expertroutine"],
      ["Global 1", "global1"],
      ["Global 2", "global2"]
    ];
    requiredStepValueRows.forEach(([label, token]) => {
      if (!stepValueRows[token]) {
        throw new Error(`模板缺少必需锚点行: ${label}`);
      }
    });

    const baseHeaderMap = {};
    for (let col = firstStepStart; col <= firstStepStart + blockWidth - 1; col += 1) {
      const token = normalizeTemplateToken(worksheet.getCell(headerRow, col).value);
      if (!token) continue;
      if (!Array.isArray(baseHeaderMap[token])) {
        baseHeaderMap[token] = [];
      }
      baseHeaderMap[token].push(col);
    }
    const requireHeader = (label, token, occurrence = 0) => {
      const cols = Array.isArray(baseHeaderMap[token]) ? baseHeaderMap[token] : [];
      if (!cols[occurrence]) {
        throw new Error(`模板缺少必需数据列: ${label}`);
      }
    };
    requireHeader("Source Type", "sourcetype", 0);
    requireHeader("Source Field", "sourcefield", 0);
    requireHeader("Source KEY", "key", 0);
    requireHeader("Source Text", "text", 0);
    requireHeader("Rule", "rule", 0);
    requireHeader("Rule Content", "rulecontent", 0);
    requireHeader("Aggr.", "aggr", 0);
    requireHeader("Target Type", "targettype", 0);
    requireHeader("Target Field", "targetfield", 0);
    requireHeader("Target KEY", "key", 1);
    requireHeader("Target Text", "text", 1);

    const routineRowMap = {
      "Start Routine": stepValueRows.startroutine,
      "End Routine": stepValueRows.endroutine,
      "Expert Routine": stepValueRows.expertroutine,
      "Global 1": stepValueRows.global1,
      "Global 2": stepValueRows.global2
    };

    return {
      stepTitleRow,
      transformationRow: stepValueRows.transformation,
      routineRows: routineRowMap,
      headerRow,
      dataStartRow: PATH_EXPORT_DEFAULT_HEADER_ROW + 1,
      stepStartColumns,
      firstStepStart,
      blockWidth,
      stepValueRows
    };
  }

  function resetAlignedTemplateSheet(worksheet, segmentCount, alignedRowCount) {
    const anchors = getAlignedTemplateAnchors(worksheet);
    ensureAlignedTemplateBlockCapacity(worksheet, segmentCount, anchors);
    ensureAlignedTemplateRowCapacity(worksheet, anchors.dataStartRow + Math.max(1, alignedRowCount) - 1);

    const managedBlockCount = Math.max(anchors.stepStartColumns.length, Math.max(1, Number(segmentCount) || 1));
    const managedStepEnd = anchors.firstStepStart + (managedBlockCount * anchors.blockWidth) - 1;
    for (let rowIndex = anchors.dataStartRow; rowIndex <= worksheet.rowCount; rowIndex += 1) {
      for (let columnIndex = 1; columnIndex <= managedStepEnd; columnIndex += 1) {
        worksheet.getCell(rowIndex, columnIndex).value = null;
      }
    }

    const dynamicValueRows = Object.values(anchors.stepValueRows || {}).filter((rowIndex) => Number(rowIndex) > 0);
    for (let blockIndex = 0; blockIndex < managedBlockCount; blockIndex += 1) {
      const columnStart = anchors.firstStepStart + (blockIndex * anchors.blockWidth);
      worksheet.getCell(anchors.stepTitleRow, columnStart).value = null;
      dynamicValueRows.forEach((rowIndex) => {
        worksheet.getCell(rowIndex, columnStart + 1).value = null;
      });
    }

    return anchors;
  }

  function populateAlignedTemplateSheet(worksheet, segments, alignedRows) {
    const routineLabels = ["Start Routine", "End Routine", "Expert Routine", "Global 1", "Global 2"];
    const stepHeaderAliases = {
      source_type: ["sourcetype"],
      source_field: ["sourcefield"],
      source_key: ["key"],
      source_text: ["text"],
      rule: ["rule"],
      rule_content: ["rulecontent"],
      aggr: ["aggr"],
      target_type: ["targettype"],
      target_field: ["targetfield"],
      target_key: ["key"],
      target_text: ["text"]
    };

    const findHeaderColumn = (headerMap, aliases, occurrence = 0, fallbackColumn = 0) => {
      const list = Array.isArray(aliases) ? aliases : [aliases];
      for (const alias of list) {
        const token = normalizeTemplateToken(alias);
        const columns = Array.isArray(headerMap[token]) ? headerMap[token] : [];
        const hit = Number(columns[occurrence] || 0);
        if (hit > 0) return hit;
      }
      return fallbackColumn > 0 ? fallbackColumn : 0;
    };

    const findRequiredHeaderColumn = (headerMap, aliases, label, occurrence = 0) => {
      const hit = findHeaderColumn(headerMap, aliases, occurrence, 0);
      if (!hit) {
        throw new Error(`模板缺少必需数据列: ${label}`);
      }
      return hit;
    };

    const buildStepHeaderMap = (rowIndex, startColumn, endColumn) => {
      const map = {};
      for (let col = startColumn; col <= endColumn; col += 1) {
        const token = normalizeTemplateToken(worksheet.getCell(rowIndex, col).value);
        if (!token) continue;
        if (!Array.isArray(map[token])) {
          map[token] = [];
        }
        map[token].push(col);
      }
      return map;
    };

    const anchors = resetAlignedTemplateSheet(worksheet, segments.length, alignedRows.length);

    segments.forEach((segment, segmentIndex) => {
      const columnStart = anchors.firstStepStart + (segmentIndex * anchors.blockWidth);
      const columnEnd = columnStart + anchors.blockWidth - 1;
      const targetTextVisible = isStepTargetFieldTextVisible(segment, segmentIndex);
      const headerMap = buildStepHeaderMap(anchors.headerRow, columnStart, columnEnd);
      const sourceTypeCol = findRequiredHeaderColumn(headerMap, stepHeaderAliases.source_type, "Source Type", 0);
      const sourceFieldCol = findRequiredHeaderColumn(headerMap, stepHeaderAliases.source_field, "Source Field", 0);
      const sourceKeyCol = findRequiredHeaderColumn(headerMap, stepHeaderAliases.source_key, "Source KEY", 0);
      const sourceTextCol = findRequiredHeaderColumn(headerMap, stepHeaderAliases.source_text, "Source Text", 0);
      const ruleCol = findRequiredHeaderColumn(headerMap, stepHeaderAliases.rule, "Rule", 0);
      const ruleContentCol = findRequiredHeaderColumn(headerMap, stepHeaderAliases.rule_content, "Rule Content", 0);
      const aggrCol = findRequiredHeaderColumn(headerMap, stepHeaderAliases.aggr, "Aggr.", 0);
      const targetTypeCol = findRequiredHeaderColumn(headerMap, stepHeaderAliases.target_type, "Target Type", 0);
      const targetFieldCol = findRequiredHeaderColumn(headerMap, stepHeaderAliases.target_field, "Target Field", 0);
      const targetKeyCol = findRequiredHeaderColumn(headerMap, stepHeaderAliases.target_key, "Target KEY", 1);
      const targetTextCol = findRequiredHeaderColumn(headerMap, stepHeaderAliases.target_text, "Target Text", 1);
      const stepHeaderValues = buildExportStepHeaderValues(segment, segmentIndex, alignedRows);

      worksheet.getCell(anchors.stepTitleRow, columnStart).value = stepHeaderValues.stepTitle;
      const stepValueWriteMap = {
        transformation: stepHeaderValues.transformation,
        sourcesys: stepHeaderValues.sourcesys,
        sourcetype: stepHeaderValues.sourcetype,
        sourcetable: stepHeaderValues.sourcetable,
        targettype: stepHeaderValues.targettype,
        targettable: stepHeaderValues.targettable,
        startroutine: stepHeaderValues.startroutine,
        endroutine: stepHeaderValues.endroutine,
        expertroutine: stepHeaderValues.expertroutine,
        global1: stepHeaderValues.global1,
        global2: stepHeaderValues.global2
      };
      Object.entries(stepValueWriteMap).forEach(([token, value]) => {
        const rowIndex = Number(anchors.stepValueRows[token] || 0);
        if (rowIndex > 0) {
          worksheet.getCell(rowIndex, columnStart + 1).value = value;
        }
      });
      for (let offset = 0; offset < routineLabels.length; offset += 1) {
        const rowIndex = anchors.routineRows[routineLabels[offset]];
        worksheet.getCell(rowIndex, columnStart + 1).value = stepHeaderValues.routineValues[offset] || "None";
      }

      alignedRows.forEach((row, rowIndex) => {
        const sourceRowIndex = anchors.dataStartRow + rowIndex;
        const cell = row?.[segmentIndex] || null;
        const normalizedRule = String(cell?.rule || "").trim().toUpperCase();
        const hasSourceField = Boolean(String(cell?.source_field || "").trim());
        const hasTargetField = Boolean(String(cell?.target_field || "").trim());
        const hasNoRule = !normalizedRule && (hasSourceField || hasTargetField);
        const shouldHighlightRule = normalizedRule === "CONSTANT" || normalizedRule === "ROUTINE" || normalizedRule === "FORMULA";

        worksheet.getCell(sourceRowIndex, sourceTypeCol).value = normalizeAlignedExportTypeLabel(cell?.source_fieldtype);
        worksheet.getCell(sourceRowIndex, sourceFieldCol).value = String(cell?.source_field || "").trim() || null;
        worksheet.getCell(sourceRowIndex, sourceKeyCol).value = String(cell?.source_key || "").trim() || null;
        worksheet.getCell(sourceRowIndex, sourceTextCol).value = String(cell?.source_text || "").trim() || null;
        const ruleCell = worksheet.getCell(sourceRowIndex, ruleCol);
        ruleCell.value = hasNoRule ? "NO RULE" : (String(cell?.rule || "").trim() || null);
        const ruleContentCell = worksheet.getCell(sourceRowIndex, ruleContentCol);
        ruleContentCell.value = getAlignedRuleContentValue(cell);
        if (hasNoRule) {
          const grayFill = {
            type: "pattern",
            pattern: "solid",
            fgColor: { argb: "FFD9D9D9" },
            bgColor: { argb: "FFD9D9D9" }
          };
          if (hasSourceField) {
            [
              worksheet.getCell(sourceRowIndex, sourceTypeCol),
              worksheet.getCell(sourceRowIndex, sourceFieldCol),
              worksheet.getCell(sourceRowIndex, sourceKeyCol),
              worksheet.getCell(sourceRowIndex, sourceTextCol)
            ].forEach((targetCell) => {
              setExcelCellFill(targetCell, grayFill);
            });
          }
          if (hasTargetField) {
            [
              worksheet.getCell(sourceRowIndex, targetTypeCol),
              worksheet.getCell(sourceRowIndex, targetFieldCol),
              worksheet.getCell(sourceRowIndex, targetKeyCol),
              worksheet.getCell(sourceRowIndex, targetTextCol)
            ].forEach((targetCell) => {
              setExcelCellFill(targetCell, grayFill);
            });
          }
        }
        if (shouldHighlightRule) {
          const yellowFill = {
            type: "pattern",
            pattern: "solid",
            fgColor: { argb: "FFFFFF00" },
            bgColor: { argb: "FFFFFF00" }
          };
          setExcelCellFill(ruleCell, yellowFill);
          setExcelCellFill(ruleContentCell, yellowFill);
        }
        worksheet.getCell(sourceRowIndex, aggrCol).value = String(cell?.aggr || "").trim() || null;
        worksheet.getCell(sourceRowIndex, targetTypeCol).value = normalizeAlignedExportTypeLabel(cell?.target_fieldtype);
        worksheet.getCell(sourceRowIndex, targetFieldCol).value = String(cell?.target_field || "").trim() || null;
        worksheet.getCell(sourceRowIndex, targetKeyCol).value = String(cell?.target_key || "").trim() || null;
        worksheet.getCell(sourceRowIndex, targetTextCol).value = String(cell?.target_text || "").trim() || null;
      });

      const targetGroupEndCol = targetTextVisible ? targetTextCol : targetFieldCol;
      const targetGroups = collectAlignedExportGroupsForSegment(alignedRows, segmentIndex);
      const redBorderStyle = {
        style: "medium",
        color: { argb: "FFFF6B6B" }
      };
      targetGroups.forEach((group) => {
        const startRow = anchors.dataStartRow + Number(group.start || 0);
        const endRow = anchors.dataStartRow + Number(group.end || 0);
        applyExcelJsOuterBorder(worksheet, startRow, endRow, ruleCol, targetGroupEndCol, redBorderStyle);
      });

    });
  }

  function appendRowsToWorksheet(worksheet, rows) {
    (Array.isArray(rows) ? rows : []).forEach((row) => {
      worksheet.addRow(Array.isArray(row) ? row : []);
    });
  }

  async function buildStyledPathExportWorkbook(mappingResult, appliedPath, alignedRows) {
    await ensureExcelJsLoaded();

    const response = await fetch(PATH_EXPORT_TEMPLATE_URL, { credentials: "same-origin" });
    if (!response.ok) {
      throw new Error(`模板文件读取失败（status ${response.status}）。`);
    }

    const templateBuffer = await response.arrayBuffer();
    const workbook = new window.ExcelJS.Workbook();
    await workbook.xlsx.load(templateBuffer);

    const segments = Array.isArray(mappingResult?.segments) ? mappingResult.segments : [];
    const alignedSheet = workbook.getWorksheet(PATH_EXPORT_TEMPLATE_SHEET) || workbook.worksheets[0];
    if (!alignedSheet) {
      throw new Error("模板中未找到 Aligned Mapping 工作表。");
    }

    populateAlignedTemplateSheet(alignedSheet, segments, alignedRows);

    const { detailSheetRows, stepLogicSheetRows, summaryRows } = buildPathExportWorkbookData(mappingResult, appliedPath, alignedRows);
    const extraSheets = [
      ["Summary", summaryRows],
      ["Segment Detail", detailSheetRows],
      ["Step Logic", stepLogicSheetRows]
    ];

    extraSheets.forEach(([sheetName, rows]) => {
      const existing = workbook.getWorksheet(sheetName);
      if (existing) {
        workbook.removeWorksheet(existing.id);
      }
      const sheet = workbook.addWorksheet(sheetName);
      appendRowsToWorksheet(sheet, rows);
    });

    return workbook;
  }

  function buildPathExportWorkbookData(mappingResult, appliedPath, alignedRowsOverride = null) {
    const segments = Array.isArray(mappingResult?.segments) ? mappingResult.segments : [];
    const alignedRows = Array.isArray(alignedRowsOverride) ? alignedRowsOverride : getVisibleAlignedSegmentRows(segments);
    const toExcelTextCell = (value) => {
      const text = String(value || "");
      return text ? text : null;
    };
    const toLogicKindCell = (entries) => {
      const list = Array.isArray(entries) ? entries : [];
      if (!list.length) return null;
      const labels = [...new Set(list.map((entry) => formatLogicKindLabel(entry?.kind)).filter(Boolean))];
      return labels.length ? labels.join(", ") : null;
    };
    const toLogicLanguageCell = (entries) => {
      const list = Array.isArray(entries) ? entries : [];
      if (!list.length) return null;
      const labels = [...new Set(list.map((entry) => String(entry?.language || "").trim()).filter(Boolean))];
      return labels.length ? labels.join(", ") : null;
    };

    const alignedSheetRows = [];
    alignedSheetRows.push(segments.flatMap((segment) => [
      `Step ${segment.index || ""}: ${segment.source || "--"} -> ${segment.target || "--"}`,
      null,
      null,
      null,
      null,
      null
    ]));
    alignedSheetRows.push(segments.flatMap(() => ["Source Field", "Rule", "Aggr.", "Target Type", "Target Field", "KEY"]));
    alignedRows.forEach((row) => {
      alignedSheetRows.push(
        segments.flatMap((_, segmentIndex) => {
          const cell = row[segmentIndex];
          return [
            toExcelTextCell(cell?.source_field),
            toExcelTextCell(cell?.rule),
            toExcelTextCell(cell?.aggr),
            toExcelTextCell(normalizeAlignedExportTypeLabel(cell?.target_fieldtype)),
            toExcelTextCell(cell?.target_field),
            toExcelTextCell(cell?.target_key)
          ];
        })
      );
    });

    const detailSheetRows = [["Step", "Source", "Target", "TRANID", "Source Field", "Rule", "Aggr.", "Target Field", "KEY", "Rule Content Count", "Rule Content Kind", "Rule Content Language", "Rule Content Raw", "Rule Content Display"]];
    segments.forEach((segment) => {
      const tranText = Array.isArray(segment.tran_ids) ? segment.tran_ids.join(", ") : "";
      const rows = Array.isArray(segment.rows) ? segment.rows : [];
      if (!rows.length) {
        detailSheetRows.push([
          Number(segment.index || 0),
          toExcelTextCell(segment.source),
          toExcelTextCell(segment.target),
          toExcelTextCell(tranText),
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null
        ]);
        return;
      }
      rows.forEach((row) => {
        const logicEntries = Array.isArray(row?.logic_entries) ? row.logic_entries : [];
        detailSheetRows.push([
          Number(segment.index || 0),
          toExcelTextCell(segment.source),
          toExcelTextCell(segment.target),
          toExcelTextCell(row?.tran_id || tranText),
          toExcelTextCell(row?.source_field),
          toExcelTextCell(row?.rule),
          toExcelTextCell(row?.aggr),
          toExcelTextCell(row?.target_field),
          toExcelTextCell(row?.target_key),
          logicEntries.length || null,
          toExcelTextCell(toLogicKindCell(logicEntries)),
          toExcelTextCell(toLogicLanguageCell(logicEntries)),
          toExcelTextCell(formatLogicEntriesForExcel(logicEntries, "raw")),
          toExcelTextCell(formatLogicEntriesForExcel(logicEntries, "display"))
        ]);
      });
    });

    const stepLogicSheetRows = [["Step", "Source", "Target", "TRANID", "Logic Kind", "Title", "Language", "Raw Content", "Display Content"]];
    segments.forEach((segment) => {
      const stepLogicGroups = Array.isArray(segment.step_logic) ? segment.step_logic : [];
      stepLogicGroups.forEach((group) => {
        const entries = Array.isArray(group?.entries) ? group.entries : [];
        entries.forEach((entry) => {
          stepLogicSheetRows.push([
            Number(segment.index || 0),
            toExcelTextCell(segment.source),
            toExcelTextCell(segment.target),
            toExcelTextCell(group?.tran_id || entry?.tran_id),
            toExcelTextCell(formatLogicKindLabel(entry?.kind)),
            toExcelTextCell(getLogicEntryTitle(entry)),
            toExcelTextCell(entry?.language),
            toExcelTextCell(getLogicEntryContentRaw(entry)),
            toExcelTextCell(getLogicEntryContentDisplay(entry))
          ]);
        });
      });
    });

    const totalRuleLogicRows = detailSheetRows.slice(1).reduce((count, row) => count + (Number(row[9] || 0) > 0 ? 1 : 0), 0);
    const totalStepLogicEntries = Math.max(stepLogicSheetRows.length - 1, 0);

    const summaryRows = [
      ["Path", appliedPath?.index || ""],
      ["Segments", appliedPath?.segment_count || segments.length],
      ["View", "AG Grid"],
      ["Visible Rows", alignedRows.length],
      ["Rule Logic Rows", totalRuleLogicRows],
      ["Step Logic Entries", totalStepLogicEntries],
      ["Filter", activeMappingQuickFilter || "--"]
    ];
    return { alignedSheetRows, detailSheetRows, stepLogicSheetRows, summaryRows };
  }

  async function exportAppliedPathMapping() {
    const appliedPath = getAppliedPathCandidate();
    if (!appliedPath || !activePathMappingResult) {
      showToast("请先确定一条路径并等待字段映射加载完成。", "error");
      return;
    }

    await ensurePathMappingFeatures({ logic: true, text: true }, "正在补充导出所需数据...");

    try {
      await ensureExcelJsLoaded();
    } catch {
      showToast("Excel 导出依赖加载失败，请刷新后重试。", "error");
      return;
    }

    const alignedRows = getCurrentAlignedRowsForActions();
    const workbook = await buildStyledPathExportWorkbook(activePathMappingResult, appliedPath, alignedRows);

    const summary = activePathSearchResult || {};
    const sourceName = String(summary.source_name || "source").replace(/[^A-Za-z0-9_-]+/g, "_");
    const targetName = String(summary.target_name || "target").replace(/[^A-Za-z0-9_-]+/g, "_");
    const fileName = `path_mapping_${sourceName}_to_${targetName}_path${appliedPath.index || 1}.xlsx`;
    const buffer = await workbook.xlsx.writeBuffer();
    const blob = new Blob([
      buffer
    ], { type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" });

    const hasSavePicker = typeof window.showSaveFilePicker === "function" && !isSavePickerDisabled();
    let shouldFallbackToAnchorDownload = !hasSavePicker;
    if (hasSavePicker) {
      try {
        const fileHandle = await window.showSaveFilePicker({
          suggestedName: fileName,
          types: [
            {
              description: "Excel Workbook",
              accept: {
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": [".xlsx"]
              }
            }
          ]
        });
        const writable = await fileHandle.createWritable();
        await writable.write(blob);
        await writable.close();
        showToast(`已保存导出文件：${fileName}`);
        return;
      } catch (error) {
        const errorName = String(error?.name || "").trim();
        if (errorName === "AbortError") {
          return;
        }
        if (errorName === "NotAllowedError") {
          // Disable save picker for this environment after a platform-level rejection.
          setSavePickerDisabled(true);
          shouldFallbackToAnchorDownload = true;
        } else {
          throw new Error(`系统保存失败（${errorName || "UnknownError"}）：${String(error?.message || "当前环境不支持该保存流程")}`);
        }
      }
    }

    if (shouldFallbackToAnchorDownload) {
      const url = window.URL.createObjectURL(blob);
      const anchor = document.createElement("a");
      anchor.href = url;
      anchor.download = fileName;
      document.body.appendChild(anchor);
      anchor.click();
      document.body.removeChild(anchor);
      window.setTimeout(() => window.URL.revokeObjectURL(url), 1000);
    }
  }

  async function renderAppliedPathResult(path) {
    if (!mappingPanelsGrid) return;
    const segments = Array.isArray(path?.segments) ? path.segments : [];
    if (!segments.length) {
      renderEmptyPathResult();
      return;
    }

    pathHideNonKeyByStep = {};
    activeAgGridColumnState = [];
    pathFieldTextVisibleState = buildFieldTextVisibleState(segments);
    activeMappingQuickFilter = "";

    mappingPanelsGrid.innerHTML = `
      <article class="mapping-segment-card soft-panel mapping-empty-card">
        <div class="mapping-segment-meta">
          <div>Field Mapping</div>
          <div>正在加载当前路径的字段映射...</div>
        </div>
      </article>
    `;

    const mappingResult = await fetchPathMapping(buildPathMappingRequestSegments(segments), {
      includeLogic: false,
      includeText: isAnyFieldTextToggleVisible()
    });
    activePathMappingResult = mappingResult;
    activePathMappingFeatures = getPathMappingFeaturesFromPayload(mappingResult);
    renderAlignedPathMapping(mappingResult);
  }

  async function runPathSearch() {
    if (PATH_MAPPING_REBUILD_MODE) {
      activePathSearchResult = null;
      selectedPathCandidateId = "";
      appliedPathCandidateId = "";
      renderPathGraphEmptyState("Path Mapping Rebuild", getFieldMappingRebuildMessage("path"));
      renderPathSelectionSummaryState();
      renderEmptyPathResult(getFieldMappingRebuildMessage("path"));
      showToast(getFieldMappingRebuildMessage("path"), "success");
      return;
    }

    const source = normalizePathToken(startModelInput?.value || "");
    const sourcesys = normalizePathToken(startSourceSystemInput?.value || "");
    const target = normalizePathToken(endModelInput?.value || "");
    const tranId = normalizePathToken(tranIdInput?.value || "");

    if (startModelInput) startModelInput.value = source;
    if (startSourceSystemInput) startSourceSystemInput.value = sourcesys;
    if (endModelInput) endModelInput.value = target;
    if (tranIdInput) tranIdInput.value = tranId;

    const hasSourceTargetInput = Boolean(source || sourcesys || target);
    const hasCompleteSourceTarget = Boolean(source && target);
    const hasTranId = Boolean(tranId);

    if (hasTranId && hasSourceTargetInput) {
      showToast("Enter either Source and Target, or Transformation ID.", "error");
      return;
    }

    if (!hasTranId && !hasCompleteSourceTarget) {
      showToast("Enter both Source and Target, or Transformation ID.", "error");
      return;
    }

    setPathSelectionCollapsed(false);

    if (hasCompleteSourceTarget) {
      pushPathInputHistory("source", source);
      pushPathInputHistory("sourceSystem", sourcesys);
      pushPathInputHistory("target", target);
    }
    if (hasTranId) {
      pushPathInputHistory("tranId", tranId);
    }

    saveHomeState();
    selectedPathCandidateId = "";
    appliedPathCandidateId = "";
    activePathSearchResult = null;
    activePathMappingResult = null;
    activePathMappingFeatures = { logic: false, text: false };
    pathHideNonKeyByStep = {};
    pathFieldTextVisibleState = buildFieldTextVisibleState([]);
    activeMappingQuickFilter = "";
    renderEmptyPathResult("请点击一条候选路径，直接加载下方字段映射结果。");
    renderPathGraphEmptyState("Searching...", "正在计算候选路径，请稍候。");
    renderPathSelectionSummaryState();
    setPathSearchBusy(true);

    try {
      const loadingText = hasTranId ? "正在按转换 ID 查询路径..." : "正在查询候选路径...";
      const result = await withAppLoading(loadingText, async () => fetchPathSelection(source, sourcesys, target, tranId));
      activePathSearchResult = result;
      const items = Array.isArray(result?.candidate_paths) ? result.candidate_paths : [];
      renderPathCandidates();
      renderPathSelectionSummaryState();
      if (items.length) {
        showToast(`找到 ${formatCount(items.length)} 条候选路径，请点击路径卡片直接查看结果。`);
      } else {
        showToast("当前条件下未找到可选路径。", "error");
      }
    } catch (error) {
      const rawMsg = String(error?.message || "").trim();
      activePathSearchResult = null;
      selectedPathCandidateId = "";
      appliedPathCandidateId = "";
      renderPathGraphEmptyState("Path Search Failed", rawMsg || "路径查询失败，请确认后端服务和导入数据。");
      renderPathSelectionSummaryState();
      renderEmptyPathResult("路径查询失败，尚未生成结果。请调整条件后重试。");
      showToast(`路径查询失败。${rawMsg ? ` 详情: ${rawMsg}` : ""}`, "error");
    } finally {
      setPathSearchBusy(false);
    }
  }

  function setupWorkspaceTabs() {
    if (!appTabButtons.length || !appTabPanels.length) return;
    if (modeQuery) {
      modeQuery.addEventListener("click", () => switchWorkspaceTab("home"));
    }
    if (modeDataQuery) {
      modeDataQuery.addEventListener("click", () => switchWorkspaceTab("dataquery"));
    }
    if (modeUpload) {
      modeUpload.addEventListener("click", () => switchWorkspaceTab("import"));
    }
    switchWorkspaceTab("home");
  }

  function setupPathBuilderUi() {
    setPathSelectionCollapsed(false);
    setAutoCollapsePathSelection(true);
    if (PATH_MAPPING_REBUILD_MODE) {
      renderPathGraphEmptyState("Path Mapping Rebuild", getFieldMappingRebuildMessage("path"));
    } else {
      renderPathGraphEmptyState("Path Graph Preview", "输入 Source、Source system、Target，或直接输入 Transformation ID 后开始生成候选路径。");
    }
    renderEmptyPathResult();
    renderPathSelectionSummaryState();

    if (togglePathSelectionBtn) {
      togglePathSelectionBtn.addEventListener("click", () => {
        setPathSelectionCollapsed(!isPathSelectionCollapsed);
      });
    }
    if (autoCollapsePathBtn) {
      autoCollapsePathBtn.addEventListener("click", () => {
        setAutoCollapsePathSelection(!autoCollapsePathSelection);
        saveHomeState();
      });
    }

    [startModelInput, startSourceSystemInput, endModelInput, tranIdInput].forEach((input) => {
      if (!input) return;
      input.addEventListener("change", () => {
        input.value = normalizePathToken(input.value);
        if (input === startModelInput) {
          pushPathInputHistory("source", input.value);
        } else if (input === startSourceSystemInput) {
          pushPathInputHistory("sourceSystem", input.value);
        } else if (input === endModelInput) {
          pushPathInputHistory("target", input.value);
        } else if (input === tranIdInput) {
          pushPathInputHistory("tranId", input.value);
        }
        saveHomeState();
      });
    });
    if (pathSearchBtn) {
      pathSearchBtn.addEventListener("click", () => {
        void runPathSearch();
      });
    }
    if (clearPathFieldsBtn) {
      clearPathFieldsBtn.addEventListener("click", () => {
        if (startModelInput) startModelInput.value = "";
        if (startSourceSystemInput) startSourceSystemInput.value = "";
        if (endModelInput) endModelInput.value = "";
        saveHomeState();
      });
    }
    if (clearTranIdBtn) {
      clearTranIdBtn.addEventListener("click", () => {
        if (tranIdInput) tranIdInput.value = "";
        saveHomeState();
      });
    }
    if (PATH_MAPPING_REBUILD_MODE) {
      return;
    }
    if (pathGraphCanvas) {
      pathGraphCanvas.addEventListener("click", async (event) => {
        const trigger = event.target.closest("[data-path-id]");
        if (!trigger) return;
        selectedPathCandidateId = String(trigger.dataset.pathId || "").trim();
        renderPathCandidates();
        const selectedPath = getSelectedPathCandidate();
        if (!selectedPath) return;
        appliedPathCandidateId = selectedPath.id;
        try {
          await withAppLoading(`正在加载 Path ${selectedPath.index || ""} 的字段映射...`, async () => renderAppliedPathResult(selectedPath));
          renderPathSelectionSummaryState();
          if (autoCollapsePathSelection) {
            setPathSelectionCollapsed(true);
          }
          showToast(`已加载 Path ${selectedPath.index}，共 ${formatCount(selectedPath.segment_count || 0)} 段。`);
        } catch (error) {
          const rawMsg = String(error?.message || "").trim();
          renderEmptyPathResult("当前路径字段映射加载失败。");
          showToast(`字段映射加载失败。${rawMsg ? ` 详情: ${rawMsg}` : ""}`, "error");
        }
      });
    }
    if (mappingPanelsGrid) {
      mappingPanelsGrid.addEventListener("click", (event) => {
        const stepLogicTrigger = event.target.closest("[data-step-logic-open]");
        if (stepLogicTrigger) {
          event.preventDefault();
          event.stopPropagation();
          void openSegmentLogicViewer(stepLogicTrigger.getAttribute("data-step-logic-open") || "");
          return;
        }

        const ruleLogicTrigger = event.target.closest("[data-rule-logic-open]");
        if (ruleLogicTrigger) {
          event.preventDefault();
          event.stopPropagation();
          void openRuleLogicViewer(
            Number(ruleLogicTrigger.getAttribute("data-segment-index") || 0),
            ruleLogicTrigger.getAttribute("data-tran-id") || "",
            Number(ruleLogicTrigger.getAttribute("data-rule-id") || 0),
            Number(ruleLogicTrigger.getAttribute("data-step-id") || 0)
          );
          return;
        }

        const copyBtn = event.target.closest(".mapping-copy-btn");
        if (!copyBtn) return;
        event.preventDefault();
        event.stopPropagation();
        const copyText = String(copyBtn.getAttribute("data-copy-text") || "").trim();
        const copyLabel = String(copyBtn.getAttribute("data-copy-label") || "内容").trim() || "内容";
        if (!copyText) return;
        void (async () => {
          try {
            await writeTextToClipboard(copyText);
            showToast(`已复制${copyLabel}：${copyText}`);
          } catch (error) {
            const rawMsg = String(error?.message || "").trim();
            showToast(`复制失败。${rawMsg ? ` 详情: ${rawMsg}` : ""}`, "error");
          }
        })();
      });
      mappingPanelsGrid.addEventListener("contextmenu", (event) => {
        const copyTarget = event.target.closest(".mapping-context-copy-value");
        if (!copyTarget) return;
        event.preventDefault();
        event.stopPropagation();
        const copyText = String(copyTarget.getAttribute("data-copy-text") || "").trim();
        const copyLabel = String(copyTarget.getAttribute("data-copy-label") || "内容").trim() || "内容";
        if (!copyText) return;
        void (async () => {
          try {
            await writeTextToClipboard(copyText);
            showToast(`已复制${copyLabel}：${copyText}`);
          } catch (error) {
            const rawMsg = String(error?.message || "").trim();
            showToast(`复制失败。${rawMsg ? ` 详情: ${rawMsg}` : ""}`, "error");
          }
        })();
      });
      mappingPanelsGrid.addEventListener("change", async (event) => {
        const textToggle = event.target.closest("[data-field-text-toggle]");
        if (textToggle && activePathMappingResult) {
          const toggleScope = String(textToggle.getAttribute("data-field-text-toggle") || "").trim();
          setFieldTextToggleVisible(toggleScope, Boolean(event.target.checked));
          if (Boolean(event.target.checked) && !activePathMappingFeatures.text) {
            try {
              await ensurePathMappingFeatures({ text: true }, "正在加载字段文本...");
            } catch (error) {
              const rawMsg = String(error?.message || "").trim();
              showToast(`字段文本加载失败。${rawMsg ? ` 详情: ${rawMsg}` : ""}`, "error");
              setFieldTextToggleVisible(toggleScope, false);
            }
          }
          // Text columns are dynamically inserted/removed. Skip restoring stale column order once.
          skipAgGridStateRestoreOnce = true;
          renderAlignedPathMapping(activePathMappingResult);
          return;
        }

        const toggle = event.target.closest("[data-step-toggle]");
        if (!toggle || !activePathMappingResult) return;
        const step = String(toggle.getAttribute("data-step-toggle") || "").trim();
        if (!step) return;
        pathHideNonKeyByStep[step] = !event.target.checked;
        renderAlignedPathMapping(activePathMappingResult);
      });
    }
    pathResultActionButtons.forEach((button) => {
      button.addEventListener("click", async () => {
        const appliedPath = getAppliedPathCandidate();
        if (!appliedPath) {
          showToast("请先点击一条路径，再使用结果工具栏。", "error");
          return;
        }
        if (button.id === "exportPathResultBtn") {
          if (exportPathWorkbookLock) {
            return;
          }
          exportPathWorkbookLock = true;
          try {
            await withAppLoading("正在生成并导出 Excel...", async () => {
              await exportAppliedPathMapping();
            });
          } catch (error) {
            const rawMsg = String(error?.message || error || "导出失败").trim();
            showToast(`导出失败：${rawMsg || "请检查模板配置。"}`, "error");
          } finally {
            exportPathWorkbookLock = false;
          }
          return;
        }

        if (button.id === "copySourceKeyFieldsBtn") {
          void copyLastStepSourceKeyFields();
          return;
        }

        showToast("当前数据版本还未接入这个视图维度。", "error");
      });
    });

    if (toggleMappingResultPanelBtn) {
      toggleMappingResultPanelBtn.addEventListener("click", () => {
        toggleMappingResultPanelMaximized();
      });
      syncMappingResultPanelButton();
    }

    if (toggleDataQueryResultPanelBtn) {
      toggleDataQueryResultPanelBtn.addEventListener("click", () => {
        toggleDataQueryResultPanelMaximized();
      });
      syncDataQueryResultPanelButton();
    }

    document.addEventListener("keydown", (event) => {
      if (event.key === "Escape" && toastIsBlocking) {
        event.preventDefault();
        event.stopPropagation();
        hideToast();
        return;
      }
      if (event.key !== "Escape") return;
      if (mappingResultPanel?.classList.contains("is-maximized")) {
        setMappingResultPanelMaximized(false);
        return;
      }
      if (dataQueryResultPanel?.classList.contains("is-maximized")) {
        setDataQueryResultPanelMaximized(false);
      }
    });
  }

  function normalizeFieldName(name) {
    return String(name || "")
      .trim()
      .toLowerCase()
      .replace(/[^a-z0-9]/g, "");
  }

  async function fetchImportStatus() {
    const resp = await apiFetch(`${importStatusApiBase}/import-status`);
    if (!resp.ok) {
      throw new Error(`status ${resp.status}`);
    }
    return resp.json();
  }

  async function fetchImportSchema(tableName) {
    const resp = await apiFetch(`${importStatusApiBase}/import-schema?table_name=${encodeURIComponent(tableName)}`);
    if (!resp.ok) {
      throw new Error(`status ${resp.status}`);
    }
    return resp.json();
  }

  async function rebuildRstranMappingRuleTable() {
    const resp = await apiFetch(`${importStatusApiBase}/import/rebuild-rstran-mapping-rule`, {
      method: "POST",
      timeoutMs: 300000
    });
    if (!resp.ok) {
      const text = await resp.text();
      const msg = parseErrorText(text, `status ${resp.status}`);
      throw new Error(msg);
    }
    return resp.json();
  }

  async function rebuildRstranMappingRuleFullTable() {
    const resp = await apiFetch(`${importStatusApiBase}/import/rebuild-rstran-mapping-rule-full`, {
      method: "POST",
      timeoutMs: 300000
    });
    if (!resp.ok) {
      const text = await resp.text();
      const msg = parseErrorText(text, `status ${resp.status}`);
      throw new Error(msg);
    }
    return resp.json();
  }

  async function ensureImportSchema(tableName) {
    const normalized = String(tableName || "").trim().toLowerCase();
    if (!normalized) return [];
    if (Array.isArray(importSchemas[normalized]) && importSchemas[normalized].length) {
      return importSchemas[normalized];
    }
    if (importSchemaPromises[normalized]) {
      return importSchemaPromises[normalized];
    }

    importSchemaPromises[normalized] = fetchImportSchema(normalized)
      .then((payload) => {
        const columns = Array.isArray(payload?.columns) ? payload.columns : [];
        importSchemas[normalized] = columns.map((column) => String(column?.name || "").trim()).filter(Boolean);
        importSchemaMeta[normalized] = columns;
        return importSchemas[normalized];
      })
      .finally(() => {
        delete importSchemaPromises[normalized];
      });

    return importSchemaPromises[normalized];
  }

  async function markImportUpdated(tableName) {
    const resp = await apiFetch(`${importStatusApiBase}/import-status/upsert`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ table_name: tableName })
    });

    if (!resp.ok) {
      throw new Error(`status ${resp.status}`);
    }

    return resp.json();
  }

  async function executeSingleImportRequest({ tableName, mapping, sheetName, file, duplicateMode = "fail", headerRowNum = getHeaderRowNumber() }) {
    const formData = new FormData();
    formData.append("table_name", tableName);
    formData.append("mapping_json", JSON.stringify(mapping));
    formData.append("sheet_name", sheetName || "");
    formData.append("header_row_num", String(Math.max(1, Number(headerRowNum) || 1)));
    formData.append("duplicate_mode", duplicateMode);
    formData.append("file", file);

    const importTimeoutMs = String(tableName || "").trim().toLowerCase() === "dd03l" ? 1800000 : 300000;

    const resp = await apiFetch(`${importStatusApiBase}/import/execute`, {
      method: "POST",
      body: formData,
      timeoutMs: importTimeoutMs
    });

    if (!resp.ok) {
      const text = await resp.text();
      const msg = parseErrorText(text, `status ${resp.status}`);
      throw new Error(msg);
    }

    return resp.json();
  }

  async function executeChunkedCsvImport({ tableName, mapping, file, duplicateMode = "fail", headerRowNum = getHeaderRowNumber() }) {
    const chunks = await buildChunkedCsvFiles(file, headerRowNum, LARGE_IMPORT_CSV_CHUNK_ROWS);
    if (!chunks.length) {
      throw new Error("文件未读取到可导入数据行。请确认有表头并至少包含1行数据。");
    }

    const summary = {
      table_name: tableName,
      affected_rows: 0,
      inserted_rows: 0,
      updated_rows: 0,
      db_count: 0
    };

    for (let index = 0; index < chunks.length; index += 1) {
      const percent = Math.min(96, 8 + ((index / chunks.length) * 88));
      setImportProgressValue(percent, `正在上传第 ${index + 1}/${chunks.length} 批数据...`);
      try {
        const result = await executeSingleImportRequest({
          tableName,
          mapping,
          sheetName: "",
          file: chunks[index],
          duplicateMode,
          headerRowNum: 1
        });
        summary.affected_rows += Number(result?.affected_rows ?? 0);
        summary.inserted_rows += Number(result?.inserted_rows ?? 0);
        summary.updated_rows += Number(result?.updated_rows ?? 0);
        summary.db_count = Number(result?.db_count ?? summary.db_count ?? 0);
      } catch (error) {
        const message = String(error?.message || "").trim() || "未知错误";
        throw new Error(`第 ${index + 1}/${chunks.length} 批导入失败。${summary.affected_rows > 0 ? `此前已处理 ${formatCount(summary.affected_rows)} 条。` : ""}${message}`);
      }
    }

    setImportProgressValue(98, `全部 ${chunks.length} 批已上传，正在完成收尾...`);
    return summary;
  }

  async function executeChunkedCsvImport({ tableName, mapping, file, duplicateMode = "fail", headerRowNum = getHeaderRowNumber() }) {
    const chunks = await buildChunkedCsvFiles(file, headerRowNum, LARGE_IMPORT_CSV_CHUNK_ROWS);
    if (!chunks.length) {
      throw new Error("文件未读取到可导入数据行。请确认有表头并至少包含1行数据。");
    }

    const summary = {
      table_name: tableName,
      affected_rows: 0,
      inserted_rows: 0,
      updated_rows: 0,
      db_count: 0
    };

    for (let index = 0; index < chunks.length; index += 1) {
      const percent = Math.min(96, 8 + ((index / chunks.length) * 88));
      setImportProgressValue(percent, `正在上传第 ${index + 1}/${chunks.length} 批数据...`);
      try {
        const result = await executeSingleImportRequest({
          tableName,
          mapping,
          sheetName: "",
          file: chunks[index],
          duplicateMode,
          headerRowNum: 1
        });
        summary.affected_rows += Number(result?.affected_rows ?? 0);
        summary.inserted_rows += Number(result?.inserted_rows ?? 0);
        summary.updated_rows += Number(result?.updated_rows ?? 0);
        summary.db_count = Number(result?.db_count ?? summary.db_count ?? 0);
      } catch (error) {
        const message = String(error?.message || "").trim() || "未知错误";
        throw new Error(`第 ${index + 1}/${chunks.length} 批导入失败。${summary.affected_rows > 0 ? `此前已处理 ${formatCount(summary.affected_rows)} 条。` : ""}${message}`);
      }
    }

    setImportProgressValue(98, `全部 ${chunks.length} 批已上传，正在完成收尾...`);
    return summary;
  }

  async function executeImport({ tableName, mapping, sheetName, file, duplicateMode = "fail", headerRowNum = getHeaderRowNumber() }) {
    if (shouldUseChunkedCsvImport(tableName, file)) {
      return executeChunkedCsvImport({ tableName, mapping, file, duplicateMode, headerRowNum });
    }
    return executeSingleImportRequest({ tableName, mapping, sheetName, file, duplicateMode, headerRowNum });
  }

  async function refreshImportCardTimes() {
    let payload = {};
    let loaded = false;
    try {
      payload = await fetchImportStatus();
      loaded = true;
    } catch {
      payload = {};
    }
    applyImportStatusPayload(payload);
    importStatusLoadedOnce = loaded;
    return payload;
  }

  async function ensureImportCardTimesLoaded(force = false) {
    if (!force && importStatusLoadedOnce) {
      return latestImportStatusPayload;
    }
    if (importStatusLoadPromise) {
      return importStatusLoadPromise;
    }
    importStatusLoadPromise = refreshImportCardTimes().finally(() => {
      importStatusLoadPromise = null;
    });
    return importStatusLoadPromise;
  }

  function buildMapOptions(excelFields, selected) {
    const opts = [`<option value="">未映射</option>`]
      .concat(
        excelFields.map((field) => {
          const isSelected = selected === field ? "selected" : "";
          return `<option value="${escAttr(field)}" ${isSelected}>${esc(field)}</option>`;
        })
      )
      .join("");
    return opts;
  }

  function readMappingFromUI() {
    const mapping = {};
    importMapBody.querySelectorAll("tr").forEach((row) => {
      const dbField = row.dataset.dbField || "";
      const fixedToggle = row.querySelector(".js-fixed-toggle");
      const fixedSelect = row.querySelector(".js-fixed-select");
      const fixedInput = row.querySelector(".js-fixed-input");
      const excelSelect = row.querySelector(".js-excel-select");

      if (!dbField) return;

      if (fixedToggle && fixedToggle.checked) {
        const fixedVal = fixedInput
          ? String(fixedInput.value || "").trim()
          : String(fixedSelect?.value || "").trim();
        if (fixedVal) {
          mapping[dbField] = `__FIXED__:${fixedVal}`;
        }
        return;
      }

      if (excelSelect && excelSelect.value) {
        mapping[dbField] = excelSelect.value;
      }
    });

    const logicFields = logicManagedFields[activeImportTable] || {};
    Object.entries(logicFields).forEach(([dbField, marker]) => {
      mapping[dbField] = marker;
    });

    return mapping;
  }

  function getVisibleDbFields(tableName) {
    const allFields = importSchemas[tableName] || [];
    const hidden = new Set(Object.keys(logicManagedFields[tableName] || {}));
    return allFields.filter((f) => !hidden.has(f));
  }

  function renderMappingRows(dbFields, excelHeaders, presetMap = {}) {
    importMapBody.innerHTML = dbFields
      .map((dbField) => {
        const presetRaw = String(presetMap[dbField] || "");
        const isSourceSysRow = activeImportTable === "bw_object_name" && dbField === "SOURCESYS";
        const isBwObjectTypeRow = activeImportTable === "bw_object_name" && dbField === "BW_OBJECT_TYPE";
        const isFixedRow = isSourceSysRow || isBwObjectTypeRow;
        const isFixed = isFixedRow && presetRaw.startsWith("__FIXED__:");
        const fixedVal = isFixed ? presetRaw.replace("__FIXED__:", "") : "";
        const selected = isFixed ? "" : presetRaw;
        const options = buildMapOptions(excelHeaders, selected);
        const fixedOptions = bwObjectFixedSourceOptions
          .map((item) => `<option value="${escAttr(item)}" ${fixedVal === item ? "selected" : ""}>${esc(item)}</option>`)
          .join("");
        const rowClass = isFixed ? Boolean(fixedVal) : Boolean(selected);
        const fixedControl = isBwObjectTypeRow
          ? `<select class="js-fixed-select">${fixedOptions}</select>`
          : isSourceSysRow
            ? `<input type="text" class="js-fixed-input" value="${escAttr(fixedVal)}" placeholder="输入固定值" />`
            : "";

        return `
          <tr data-db-field="${esc(dbField)}" class="${rowClass ? "mapped-row" : ""}">
            <td>${esc(dbField)}</td>
            <td>
              ${isFixedRow
                ? `<label class="fixed-inline"><input type="checkbox" class="js-fixed-toggle" ${isFixed ? "checked" : ""} /><span>固定值</span></label>`
                : ""}
            </td>
            <td>
              ${isFixedRow
                ? `<div class="fixed-select-wrap ${isFixed ? "" : "hidden"}">${fixedControl}</div>`
                : ""}
              <div class="excel-select-wrap ${isFixed ? "hidden" : ""}"><select class="js-excel-select">${options}</select></div>
            </td>
          </tr>
        `;
      })
      .join("");

    importMapBody.querySelectorAll("tr").forEach((row) => {
      const fixedToggle = row.querySelector(".js-fixed-toggle");
      const fixedSelect = row.querySelector(".js-fixed-select");
      const fixedInput = row.querySelector(".js-fixed-input");
      const excelSelect = row.querySelector(".js-excel-select");
      const fixedWrap = row.querySelector(".fixed-select-wrap");
      const excelWrap = row.querySelector(".excel-select-wrap");

      const refreshRow = () => {
        const fixedOn = Boolean(fixedToggle?.checked);
        if (fixedWrap) fixedWrap.classList.toggle("hidden", !fixedOn);
        if (excelWrap) excelWrap.classList.toggle("hidden", fixedOn);
        const fixedText = String(fixedInput?.value || "").trim();
        const hasValue = fixedOn
          ? Boolean(fixedText || fixedSelect?.value)
          : Boolean(excelSelect?.value);
        row.classList.toggle("mapped-row", hasValue);
        renderMappingProgressMeta();
      };

      if (fixedToggle) {
        fixedToggle.addEventListener("change", refreshRow);
      }
      if (fixedSelect) {
        fixedSelect.addEventListener("change", refreshRow);
      }
      if (fixedInput) {
        fixedInput.addEventListener("input", refreshRow);
      }
      if (excelSelect) {
        excelSelect.addEventListener("change", refreshRow);
      }

      refreshRow();
    });

    renderMappingProgressMeta();
  }

  function renderMappingProgressMeta() {
    if (!activeImportTable) return;

    const { total, mapped, unmapped, isComplete } = getCurrentImportMappingStats();
    if (!total) return;

    const statusText = isComplete
      ? `映射：${mapped}/${total} 字段已满足`
      : `映射：${unmapped}个字段未映射`;
    const detectText = `识别信息：标题行=第${activeHeaderRowNumber}行｜数据行数=${formatCount(activeImportDataRowCount)}`;
    const logicText = logicRuleDesc[activeImportTable] || "";

    importMeta.innerHTML = `
      <div class="import-meta-logic">${esc(detectText)}</div>
      <div class="import-meta-status ${isComplete ? "complete" : "incomplete"}">${esc(statusText)}</div>
      ${logicText ? `<div class="import-meta-logic">${esc(logicText)}</div>` : ""}
    `;
  }

  function suggestMapping(dbFields, excelHeaders) {
    const excelByNorm = new Map(excelHeaders.map((f) => [normalizeFieldName(f), f]));
    const mapped = {};

    dbFields.forEach((dbField) => {
      const direct = excelByNorm.get(normalizeFieldName(dbField));
      if (direct) {
        mapped[dbField] = direct;
      }
    });

    return mapped;
  }

  function getHeaderRowNumber() {
    const parsed = Number.parseInt(String(importHeaderRowSelect?.value || "1"), 10);
    if (!Number.isFinite(parsed) || parsed < 1) return 1;
    return Math.min(parsed, 10);
  }

  function extractHeadersAndRowCount(rows, headerRowNumber = 1) {
    const list = Array.isArray(rows) ? rows : [];
    const headerIndex = Math.max(0, Math.min(list.length - 1, headerRowNumber - 1));
    const headerRow = Array.isArray(list[headerIndex]) ? list[headerIndex] : [];
    const headers = headerRow.map((x) => String(x || "").trim()).filter(Boolean);

    const rowCount = list.slice(headerIndex + 1).reduce((count, row) => {
      if (!Array.isArray(row)) return count;
      const hasValue = row.some((cell) => String(cell || "").trim());
      return count + (hasValue ? 1 : 0);
    }, 0);

    return { headers, rowCount };
  }

  function rowsToCsv(rows) {
    return (Array.isArray(rows) ? rows : [])
      .map((row) => {
        const cols = Array.isArray(row) ? row : [];
        return cols
          .map((cell) => {
            const text = String(cell == null ? "" : cell);
            if (/[",\n\r]/.test(text)) {
              return `"${text.replace(/"/g, '""')}"`;
            }
            return text;
          })
          .join(",");
      })
      .join("\n");
  }

  function splitCsvTextIntoRecords(text) {
    const source = String(text || "");
    const records = [];
    let current = "";
    let inQuotes = false;

    for (let index = 0; index < source.length; index += 1) {
      const char = source[index];
      const nextChar = source[index + 1];

      if (char === '"') {
        current += char;
        if (inQuotes && nextChar === '"') {
          current += nextChar;
          index += 1;
        } else {
          inQuotes = !inQuotes;
        }
        continue;
      }

      if (!inQuotes && (char === "\n" || char === "\r")) {
        records.push(current);
        current = "";
        if (char === "\r" && nextChar === "\n") {
          index += 1;
        }
        continue;
      }

      current += char;
    }

    if (current || source.endsWith("\n") || source.endsWith("\r")) {
      records.push(current);
    }

    return records;
  }

  function shouldUseChunkedCsvImport(tableName, file) {
    const fileName = String(file?.name || "").trim().toLowerCase();
    return fileName.endsWith(".csv");
  }

  async function buildChunkedCsvFiles(file, headerRowNumber, chunkRowCount = LARGE_IMPORT_CSV_CHUNK_ROWS) {
    const text = await file.text();
    const records = splitCsvTextIntoRecords(text);
    const headerIndex = Math.max(0, Number(headerRowNumber || 1) - 1);
    if (!records.length || headerIndex >= records.length) {
      throw new Error(`标题行数第${headerRowNumber}行超出文件有效范围`);
    }

    const headerRecord = records[headerIndex];
    const dataRecords = records.slice(headerIndex + 1).filter((record) => String(record || "").trim());
    if (!dataRecords.length) {
      return [];
    }

    const baseName = String(file.name || "import.csv").replace(/\.csv$/i, "") || "import";
    const chunks = [];
    for (let start = 0; start < dataRecords.length; start += chunkRowCount) {
      const chunkRows = dataRecords.slice(start, start + chunkRowCount);
      const chunkText = `\ufeff${[headerRecord].concat(chunkRows).join("\r\n")}`;
      chunks.push(new File([chunkText], `${baseName}.part-${chunks.length + 1}.csv`, { type: file.type || "text/csv;charset=utf-8" }));
    }
    return chunks;
  }

  async function buildImportPayloadByHeaderRow(file, sheetName, headerRowNumber) {
    const headerRow = Math.max(1, Number(headerRowNumber) || 1);
    return { file, sheetName: sheetName || "", headerRowNum: headerRow, transformed: false };
  }

  async function readWorkbookSheetNames(file) {
    if (!(await ensureXlsxLoadedOrNotify())) {
      return [];
    }

    const buffer = await file.arrayBuffer();
    const workbook = window.XLSX.read(buffer, {
      type: "array",
      bookSheets: true,
      bookProps: true
    });
    return Array.isArray(workbook?.SheetNames) ? workbook.SheetNames.filter(Boolean) : [];
  }

  async function readExcelSheetPreview(file, sheetName = "", headerRowNumber = 1) {
    if (!(await ensureXlsxLoadedOrNotify())) {
      return { headers: [], rowCount: 0, detectedHeaderRow: 1, sheetNames: [] };
    }

    const previewRowLimit = Math.max(12, Number(headerRowNumber) + 8);
    const buffer = await file.arrayBuffer();
    const workbook = window.XLSX.read(buffer, {
      type: "array",
      sheetRows: previewRowLimit
    });
    const sheetNames = Array.isArray(workbook?.SheetNames) ? workbook.SheetNames.filter(Boolean) : [];
    const targetSheet = String(sheetName || sheetNames[0] || "").trim();
    if (!targetSheet) {
      return { headers: [], rowCount: 0, detectedHeaderRow: 1, sheetNames };
    }

    const sheet = workbook.Sheets[targetSheet];
    if (!sheet) {
      return { headers: [], rowCount: 0, detectedHeaderRow: 0, sheetNames };
    }

    const rows = window.XLSX.utils.sheet_to_json(sheet, { header: 1, raw: false, defval: "" });
    const detectedHeaderRow = autoDetectHeaderRow(rows, activeImportTable);
    const parsed = extractHeadersAndRowCount(rows, headerRowNumber);
    return { ...parsed, detectedHeaderRow, sheetNames };
  }

  function autoDetectHeaderRow(rows, tableName) {
    const list = Array.isArray(rows) ? rows : [];
    const dbFields = getVisibleDbFields(tableName);
    const maxRows = Math.min(10, list.length || 0);
    let bestRow = 1;
    let bestScore = -1;

    for (let index = 0; index < maxRows; index += 1) {
      const row = Array.isArray(list[index]) ? list[index] : [];
      const headers = row.map((cell) => String(cell || "").trim()).filter(Boolean);
      if (!headers.length) continue;
      const mappedCount = Object.keys(suggestMapping(dbFields, headers)).length;
      const score = mappedCount * 10 + Math.min(headers.length, 9);
      if (score > bestScore) {
        bestScore = score;
        bestRow = index + 1;
      }
    }

    return bestRow;
  }

  async function parseExcelHeaders(file, headerRowNumber = 1) {
    const fileName = file.name.toLowerCase();
    if (fileName.endsWith(".csv")) {
      const text = await file.text();
      const lines = text.split(/\r?\n/);
      const rows = lines.filter((line) => line.trim()).map((line) => line.split(","));
      if (!rows.length) {
        return { headers: [], rowCount: 0, detectedHeaderRow: 1 };
      }
      const detectedHeaderRow = autoDetectHeaderRow(rows, activeImportTable);
      const parsed = extractHeadersAndRowCount(rows, headerRowNumber);
      return { ...parsed, detectedHeaderRow };
    }

    if (!(await ensureXlsxLoadedOrNotify())) {
      return { headers: [], rowCount: 0, detectedHeaderRow: 1 };
    }

    const preview = await readExcelSheetPreview(file, "", headerRowNumber);
    return {
      headers: preview.headers,
      rowCount: preview.rowCount,
      detectedHeaderRow: preview.detectedHeaderRow,
      sheetNames: preview.sheetNames || []
    };
  }

  async function getHeadersFromSheet(file, sheetName, headerRowNumber = 1) {
    if (!file || !sheetName) return { headers: [], rowCount: 0, detectedHeaderRow: 1, sheetNames: [] };
    return readExcelSheetPreview(file, sheetName, headerRowNumber);
  }

  function renderMappingByHeaders(headers, rowCount = 0) {
    const dbFields = getVisibleDbFields(activeImportTable);
    const suggested = suggestMapping(dbFields, headers);

    activeExcelHeaders = headers;
    activeImportDataRowCount = Math.max(0, Number(rowCount) || 0);
    activeHeaderRowNumber = getHeaderRowNumber();
    if (!headers.length) {
      importMeta.innerHTML = "";
      importMapBody.innerHTML = "";
      showToast("未读取到表头，请确认第一行为字段名。");
      return;
    }

    renderMappingRows(dbFields, headers, suggested);
  }

  async function openImportModal(tableName) {
    activeImportTable = tableName;
    setWorkbenchCardSelected(getImportCardElement(tableName));
    await ensureImportSchema(tableName);
    activeExcelHeaders = [];
    activeImportDataRowCount = 0;
    activeHeaderRowNumber = 1;
    importTaskLock = false;
    resetImportProgress();
    const matchedCard = getImportCardElement(tableName);
    const displayTitle = matchedCard?.dataset.title || tableName;
    importModalTitle.textContent = `字段映射 - ${displayTitle}`;
    if (confirmImportBtn) {
      confirmImportBtn.textContent = tableName === "bw_object_name" ? "更新" : "开始导入";
    }
    if (clearImportTableBtn) {
      clearImportTableBtn.classList.toggle("hidden", !clearableImportTables.includes(tableName));
      clearImportTableBtn.textContent = `删除当前表数据${tableName ? ` (${tableName})` : ""}`;
    }
    importMeta.innerHTML = "";
    importFileInput.value = "";
    if (importHeaderRowSelect) {
      importHeaderRowSelect.value = "1";
      importHeaderRowSelect.disabled = false;
    }
    if (importSheetSelect) {
      importSheetSelect.innerHTML = '<option value="">Sheet页</option>';
      importSheetSelect.disabled = true;
    }
    importMapBody.innerHTML = "";
    activeImportSheetNames = [];
    activeImportFileName = "";
    showBlockingModal(importModal);
  }

  function setupImportMapping() {
    if (!importCards.length || !importModal) return;

    if (IMPORT_MAPPING_REBUILD_MODE) {
      importCards.forEach((card) => {
        card.addEventListener("click", () => {
          showToast(getFieldMappingRebuildMessage("import"), "success");
        });
      });
      if (rebuildRstranMappingRuleBtn) {
        rebuildRstranMappingRuleBtn.addEventListener("click", () => {
          showToast(getFieldMappingRebuildMessage("import"), "success");
        });
      }
      return;
    }

    if (rebuildRstranMappingRuleBtn) {
      rebuildRstranMappingRuleBtn.addEventListener("click", async () => {
        if (mappingRuleRebuildLock || importTaskLock) return;

        const precheckPassed = await checkRebuildDependencies("mappingRule");
        if (!precheckPassed) return;

        const confirmed = window.confirm("确认重建字段规则表吗？这会先清空 rstran_mapping_rule 全部数据，再重新填充整张表。");
        if (!confirmed) return;

        try {
          setWorkbenchCardSelected(rebuildRstranMappingRuleBtn);
          mappingRuleRebuildLock = true;
          rebuildRstranMappingRuleBtn.disabled = true;
          if (rebuildRstranMappingRuleBtnTitle) {
            rebuildRstranMappingRuleBtnTitle.textContent = "重建中...";
          }

          const result = await withWorkbenchCardLoading(rebuildRstranMappingRuleBtn, "正在重建字段规则表...", async () => withAppLoading("正在重建字段规则表...", async () => rebuildRstranMappingRuleTable()));
          const insertedRows = Number(result?.inserted_rows ?? 0);
          const tranCount = Number(result?.tran_count ?? 0);
          const totalRows = Number(result?.db_count ?? 0);
          showToast(
            `字段规则表重建成功。已先清空旧数据，再重新写入：${formatCount(insertedRows)} 条；涉及转换：${formatCount(tranCount)} 个；当前表总条数：${formatCount(totalRows)}。后续若重新导入 RSTRANRULE 或 RSTRANFIELD，请再次执行重建。`,
            "success",
            {
              title: "重建成功",
              blocking: true,
              actions: true,
              requireClose: true
            }
          );
        } catch (error) {
          const rawMsg = String(error?.message || "").trim();
          showToast(`字段规则表重建失败。${rawMsg ? ` 失败原因：${rawMsg}` : ""}`, "error", {
            title: "重建失败",
            blocking: true,
            actions: true,
            requireClose: true
          });
        } finally {
          mappingRuleRebuildLock = false;
          rebuildRstranMappingRuleBtn.disabled = false;
          if (rebuildRstranMappingRuleBtnTitle) {
            rebuildRstranMappingRuleBtnTitle.textContent = "重建字段规则表";
          }
        }
      });
    }

    if (rebuildRstranMappingRuleFullBtn) {
      rebuildRstranMappingRuleFullBtn.addEventListener("click", async () => {
        if (mappingRuleRebuildLock || importTaskLock) return;

        const precheckPassed = await checkRebuildDependencies("mappingRuleFull");
        if (!precheckPassed) return;

        const confirmed = window.confirm("确认重建完整字段表吗？这会先刷新 rstran_mapping_rule，再清空并重建 rstran_mapping_rule_full。当前版本会补齐已支持的元数据来源：RSDS 依赖 rsdssegfd；ADSO 依赖 dd03l，并固定按 TABNAME=/BIC/A+技术名+1位数字、FIELDNAME 按 /BIC/ 去前缀或补 0 解析。");
        if (!confirmed) return;

        try {
          setWorkbenchCardSelected(rebuildRstranMappingRuleFullBtn);
          mappingRuleRebuildLock = true;
          rebuildRstranMappingRuleFullBtn.disabled = true;
          if (rebuildRstranMappingRuleFullBtnTitle) {
            rebuildRstranMappingRuleFullBtnTitle.textContent = "重建中...";
          }

          const result = await withWorkbenchCardLoading(rebuildRstranMappingRuleFullBtn, "正在重建完整字段表...", async () => withAppLoading("正在重建完整字段表...", async () => rebuildRstranMappingRuleFullTable()));
          const insertedRows = Number(result?.inserted_rows ?? 0);
          const tranCount = Number(result?.tran_count ?? 0);
          const mappedRows = Number(result?.mapped_rows ?? 0);
          const sourceCompletedRows = Number(result?.source_completed_rows ?? 0);
          const targetCompletedRows = Number(result?.target_completed_rows ?? 0);
          const totalRows = Number(result?.db_count ?? 0);
          showToast(
            `完整字段表重建成功。写入：${formatCount(insertedRows)} 条；涉及转换：${formatCount(tranCount)} 个；基础映射：${formatCount(mappedRows)} 条；补齐 source：${formatCount(sourceCompletedRows)} 条；补齐 target：${formatCount(targetCompletedRows)} 条；当前表总条数：${formatCount(totalRows)}。`,
            "success",
            {
              title: "重建成功",
              blocking: true,
              actions: true,
              requireClose: true
            }
          );
        } catch (error) {
          const rawMsg = String(error?.message || "").trim();
          showToast(`完整字段表重建失败。${rawMsg ? ` 失败原因：${rawMsg}` : ""}`, "error", {
            title: "重建失败",
            blocking: true,
            actions: true,
            requireClose: true
          });
        } finally {
          mappingRuleRebuildLock = false;
          rebuildRstranMappingRuleFullBtn.disabled = false;
          if (rebuildRstranMappingRuleFullBtnTitle) {
            rebuildRstranMappingRuleFullBtnTitle.textContent = "重建完整字段表";
          }
        }
      });
    }

    importCards.forEach((card) => {
      card.addEventListener("click", async () => {
        const tableName = String(card.dataset.table || "").trim().toLowerCase();
        const ready = card.dataset.importReady !== "false";
        setWorkbenchCardSelected(card);
        if (!ready || !importSchemas[tableName]) {
          if (!ready) {
            showToast(`${card.dataset.title || tableName} 导入映射下一步接入。`, "success");
            return;
          }
        }
        try {
          await withWorkbenchCardLoading(card, "正在准备字段映射...", async () => openImportModal(tableName));
        } catch (error) {
          const rawMsg = String(error?.message || "").trim();
          showToast(`读取 ${card.dataset.title || tableName} 表结构失败。${rawMsg ? ` 详情: ${rawMsg}` : ""}`, "error");
        }
      });
    });

    closeImportModal.addEventListener("click", () => hideBlockingModal(importModal));

    if (maxImportModal && importModalShell) {
      maxImportModal.addEventListener("click", () => {
        const isMaximized = importModalShell.classList.contains("maximized");
        if (!isMaximized) {
          if (importModalShell.style.width) {
            importModalShell.dataset.restoreWidth = importModalShell.style.width;
          }
          if (importModalShell.style.height) {
            importModalShell.dataset.restoreHeight = importModalShell.style.height;
          }
          importModalShell.classList.add("maximized");
          importModalShell.style.width = "";
          importModalShell.style.height = "";
          maxImportModal.setAttribute("title", "还原");
        } else {
          importModalShell.classList.remove("maximized");
          if (importModalShell.dataset.restoreWidth) {
            importModalShell.style.width = importModalShell.dataset.restoreWidth;
          }
          if (importModalShell.dataset.restoreHeight) {
            importModalShell.style.height = importModalShell.dataset.restoreHeight;
          }
          maxImportModal.setAttribute("title", "最大化");
        }
      });
    }

    if (closeLogicViewerModal) {
      closeLogicViewerModal.addEventListener("click", closeLogicViewer);
    }

    if (maxLogicViewerModal && logicViewerModalShell) {
      maxLogicViewerModal.addEventListener("click", () => {
        const isMaximized = logicViewerModalShell.classList.contains("maximized");
        if (!isMaximized) {
          if (logicViewerModalShell.style.width) {
            logicViewerModalShell.dataset.restoreWidth = logicViewerModalShell.style.width;
          }
          if (logicViewerModalShell.style.height) {
            logicViewerModalShell.dataset.restoreHeight = logicViewerModalShell.style.height;
          }
          logicViewerModalShell.classList.add("maximized");
          logicViewerModalShell.style.width = "";
          logicViewerModalShell.style.height = "";
          maxLogicViewerModal.setAttribute("title", "还原");
        } else {
          logicViewerModalShell.classList.remove("maximized");
          if (logicViewerModalShell.dataset.restoreWidth) {
            logicViewerModalShell.style.width = logicViewerModalShell.dataset.restoreWidth;
          }
          if (logicViewerModalShell.dataset.restoreHeight) {
            logicViewerModalShell.style.height = logicViewerModalShell.dataset.restoreHeight;
          }
          maxLogicViewerModal.setAttribute("title", "最大化");
        }
        resizeLogicViewerEditor();
      });
    }

    if (logicViewerNav) {
      logicViewerNav.addEventListener("click", (event) => {
        const trigger = event.target.closest("[data-logic-viewer-index]");
        if (!trigger) return;
        activeLogicViewerIndex = Number(trigger.getAttribute("data-logic-viewer-index") || 0);
        renderLogicViewer();
      });
    }

    if (copyLogicViewerBtn) {
      copyLogicViewerBtn.addEventListener("click", async () => {
        const activeEntry = activeLogicViewerEntries[activeLogicViewerIndex] || null;
        const rawContent = getLogicEntryContentRaw(activeEntry);
        if (!rawContent) {
          showToast("当前内容为空，无法复制。", "error");
          return;
        }
        try {
          await writeTextToClipboard(rawContent);
          showToast(`已复制${formatLogicKindLabel(activeEntry?.kind)}原值。`);
        } catch (error) {
          const rawMsg = String(error?.message || "").trim();
          showToast(`复制失败。${rawMsg ? ` 详情: ${rawMsg}` : ""}`, "error");
        }
      });
    }

    if (logicViewerModal) {
      logicViewerModal.addEventListener("click", (event) => {
        if (event.target === logicViewerModal) {
          event.preventDefault();
          event.stopPropagation();
        }
      });
    }

    importModal.addEventListener("click", (event) => {
      if (event.target === importModal) {
        event.preventDefault();
        event.stopPropagation();
      }
    });

    importFileInput.addEventListener("change", async () => {
      const file = importFileInput.files && importFileInput.files[0];
      if (!file || !activeImportTable) return;

      activeImportFileName = file.name;
      const fileNameLower = file.name.toLowerCase();

      await withModalLoading(importModal, "正在识别表头和字段映射...", async () => {
        if (fileNameLower.endsWith(".csv")) {
          activeImportSheetNames = [];
          if (importSheetSelect) {
            importSheetSelect.innerHTML = '<option value="">CSV无Sheet</option>';
            importSheetSelect.disabled = true;
          }
          const parsed = await parseExcelHeaders(file, getHeaderRowNumber());
          const nextHeaderRow = String(parsed.detectedHeaderRow || 1);
          if (importHeaderRowSelect) {
            importHeaderRowSelect.value = nextHeaderRow;
          }
          const refreshed = await parseExcelHeaders(file, Number(nextHeaderRow));
          renderMappingByHeaders(refreshed.headers, refreshed.rowCount);
          return;
        }

        if (!(await ensureXlsxLoadedOrNotify())) {
          return;
        }

        const initialPreview = await readExcelSheetPreview(file, "", getHeaderRowNumber());
        const sheets = initialPreview.sheetNames || [];
        activeImportSheetNames = sheets;

        if (importSheetSelect) {
          importSheetSelect.innerHTML = sheets
            .map((name, idx) => `<option value="${escAttr(name)}" ${idx === 0 ? "selected" : ""}>${esc(name)}</option>`)
            .join("");
          importSheetSelect.disabled = sheets.length === 0;
        }

        const firstSheet = sheets[0] || "";
        const parsed = firstSheet ? initialPreview : { headers: [], rowCount: 0, detectedHeaderRow: 1 };
        const nextHeaderRow = String(parsed.detectedHeaderRow || 1);
        if (importHeaderRowSelect) {
          importHeaderRowSelect.value = nextHeaderRow;
        }
        const refreshed = firstSheet ? await getHeadersFromSheet(file, firstSheet, Number(nextHeaderRow)) : { headers: [], rowCount: 0 };
        renderMappingByHeaders(refreshed.headers, refreshed.rowCount);
      });
    });

    if (importSheetSelect) {
      importSheetSelect.addEventListener("change", async () => {
        const file = importFileInput?.files && importFileInput.files[0];
        if (!file) return;
        await withModalLoading(importModal, "正在切换 Sheet 并重建字段映射...", async () => {
          const sheetName = importSheetSelect.value;
          const parsed = await getHeadersFromSheet(file, sheetName, getHeaderRowNumber());
          const nextHeaderRow = String(parsed.detectedHeaderRow || 1);
          if (importHeaderRowSelect) {
            importHeaderRowSelect.value = nextHeaderRow;
          }
          const refreshed = await getHeadersFromSheet(file, sheetName, Number(nextHeaderRow));
          renderMappingByHeaders(refreshed.headers, refreshed.rowCount);
        });
      });
    }

    if (importHeaderRowSelect) {
      importHeaderRowSelect.addEventListener("change", async () => {
        importHeaderRowSelect.value = String(getHeaderRowNumber());
        const file = importFileInput?.files && importFileInput.files[0];
        if (!file || !activeImportTable) return;

        await withModalLoading(importModal, `正在按标题行第${getHeaderRowNumber()}行识别字段...`, async () => {
          const fileNameLower = file.name.toLowerCase();
          if (fileNameLower.endsWith(".csv")) {
            const parsed = await parseExcelHeaders(file, getHeaderRowNumber());
            renderMappingByHeaders(parsed.headers, parsed.rowCount);
            return;
          }

          if (!importSheetSelect) return;
          const sheetName = importSheetSelect.value;
          const parsed = await getHeadersFromSheet(file, sheetName, getHeaderRowNumber());
          renderMappingByHeaders(parsed.headers, parsed.rowCount);
        });
      });
    }

    autoMapBtn.addEventListener("click", () => {
      if (!activeImportTable || !activeExcelHeaders.length) return;
      const dbFields = getVisibleDbFields(activeImportTable);
      const suggested = suggestMapping(dbFields, activeExcelHeaders);
      renderMappingRows(dbFields, activeExcelHeaders, suggested);
    });

    if (clearImportTableBtn) {
      clearImportTableBtn.addEventListener("click", async () => {
        if (importTaskLock) return;
        if (!clearableImportTables.includes(activeImportTable)) return;
        const ok = window.confirm(`确认删除导入表（${activeImportTable}）中的全部数据吗？此操作不可撤销。`);
        if (!ok) return;

        const form = new FormData();
        form.append("table_name", activeImportTable);

        try {
          importTaskLock = true;
          setImportBusyState(true, "正在删除全部数据...");
          const resp = await apiFetch(`${importStatusApiBase}/import/clear-table`, {
            method: "POST",
            body: form
          });
          await refreshImportCardTimes();
          const count = Number(resp?.db_count ?? 0);
          showToast(`删除完成：${activeImportTable} 当前数据条目 ${formatCount(count)}`);
          completeImportBusyState();
        } catch (err) {
          const rawMsg = String(err?.message || "").trim();
          showToast(`删除失败，请稍后重试。${rawMsg ? ` 详情: ${rawMsg}` : ""}`, "error");
          setImportBusyState(false);
        } finally {
          importTaskLock = false;
        }
      });
    }

    confirmImportBtn.addEventListener("click", async () => {
      if (importTaskLock) return;
      if (!activeImportTable) return;
      const selectedFile = importFileInput.files && importFileInput.files[0];
      if (!selectedFile) {
        showToast("本次导入失败。导入条目：0。失败原因：请先选择要导入的 Excel/CSV 文件。", "error");
        return;
      }

      const mapping = readMappingFromUI();
      const mappingStats = getCurrentImportMappingStats();

      if (!mappingStats.isComplete) {
        const warningAction = await showToastAndWait(
          `警告：当前仍有 ${formatCount(mappingStats.unmapped)} 个字段未映射。请选择继续导入或取消导入；未映射字段将按空值导入。`,
          "warning",
          {
            title: "导入警告",
            primaryLabel: "继续导入",
            primaryValue: "continue",
            secondaryLabel: "取消导入",
            secondaryValue: "cancel",
            showClose: false,
            dismissValue: "cancel"
          }
        );
        if (warningAction !== "continue") {
          return;
        }
      }

      try {
        importTaskLock = true;
        setImportBusyState(true, "正在导入数据...");
        const sheetName = importSheetSelect && !importSheetSelect.disabled ? importSheetSelect.value : "";
        const headerRowNumber = getHeaderRowNumber();
        const payload = await buildImportPayloadByHeaderRow(selectedFile, sheetName, headerRowNumber);
        if (payload.transformed && importProgressText) {
          importProgressText.textContent = `已按标题行第${headerRowNumber}行重构文件，正在导入...`;
        }
        const result = await withAppLoading(`正在导入 ${String(activeImportTable || "").toUpperCase()} 数据...`, async () => executeImport({
          tableName: activeImportTable,
          mapping,
          sheetName: payload.sheetName,
          file: payload.file,
          headerRowNum: payload.headerRowNum
        }));
        await refreshImportCardTimes();
        const successText = formatImportSuccessToast(result, mappingStats);
        showToast(successText, "success", {
          title: "导入成功",
          blocking: true,
          actions: true,
          requireClose: true
        });
        completeImportBusyState();
      } catch (err) {
        const rawMsg = String(err?.message || "").trim();
        const reason = formatImportFailureReason(rawMsg);
        showToast(formatImportFailureToast(activeImportTable, reason), "error", {
          title: "导入失败",
          blocking: true,
          actions: true,
          requireClose: true
        });
        setImportBusyState(false);
      } finally {
        importTaskLock = false;
      }
    });
  }

  function setupSelectionHighlight() {
    document.addEventListener("click", (event) => {
      const passiveControl = event.target.closest(".glass-radio, .liquid-toggle");
      if (passiveControl) {
        document.querySelectorAll(".selected-outline").forEach((el) => el.classList.remove("selected-outline"));
        return;
      }

      const target = event.target.closest(".glass-btn, .search-wrap");
      if (!target || target.classList.contains("text-btn")) return;
      document.querySelectorAll(".selected-outline").forEach((el) => el.classList.remove("selected-outline"));
      target.classList.add("selected-outline");
    });
  }


  function startBackgroundAnimation() {
    const canvas = document.getElementById("bgCanvas");
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    let cssWidth = 0;
    let cssHeight = 0;
    let pixelRatio = 1;

    function drawFrame() {
      const width = cssWidth;
      const height = cssHeight;
      if (!width || !height) return;

      ctx.setTransform(pixelRatio, 0, 0, pixelRatio, 0, 0);
      ctx.clearRect(0, 0, width, height);

      const backdrop = ctx.createLinearGradient(0, 0, 0, height);
      backdrop.addColorStop(0, "#08111f");
      backdrop.addColorStop(0.48, "#102038");
      backdrop.addColorStop(1, "#17314d");
      ctx.fillStyle = backdrop;
      ctx.fillRect(0, 0, width, height);

      const primaryGlow = ctx.createRadialGradient(
        width * 0.24,
        height * 0.18,
        0,
        width * 0.22,
        height * 0.2,
        width * 0.42
      );
      primaryGlow.addColorStop(0, "rgba(107, 154, 255, 0.34)");
      primaryGlow.addColorStop(0.4, "rgba(74, 120, 232, 0.22)");
      primaryGlow.addColorStop(1, "rgba(74, 120, 232, 0)");
      ctx.fillStyle = primaryGlow;
      ctx.fillRect(0, 0, width, height);

      const secondaryGlow = ctx.createRadialGradient(
        width * 0.78,
        height * 0.72,
        0,
        width * 0.78,
        height * 0.72,
        width * 0.48
      );
      secondaryGlow.addColorStop(0, "rgba(76, 233, 196, 0.26)");
      secondaryGlow.addColorStop(0.45, "rgba(76, 233, 196, 0.16)");
      secondaryGlow.addColorStop(1, "rgba(76, 233, 196, 0)");
      ctx.fillStyle = secondaryGlow;
      ctx.fillRect(0, 0, width, height);

      ctx.save();
      ctx.lineWidth = 1;
      const cellWidth = Math.max(96, Math.round(width / 10));
      const cellHeight = Math.max(82, Math.round(height / 7));
      const cols = Math.ceil(width / cellWidth) + 3;
      const rows = Math.ceil(height / cellHeight) + 3;
      const baseX = -cellWidth;
      const baseY = -cellHeight;
      const pointAt = (col, row) => {
        const seed = col * 12.9898 + row * 78.233;
        const driftX = Math.sin(seed) * cellWidth * 0.18 + Math.cos(seed * 0.73) * cellWidth * 0.08;
        const driftY = Math.cos(seed * 0.92) * cellHeight * 0.2 + Math.sin(seed * 1.08) * cellHeight * 0.09;
        return {
          x: baseX + col * cellWidth + driftX,
          y: baseY + row * cellHeight + driftY
        };
      };

      for (let row = 0; row < rows - 1; row += 1) {
        for (let col = 0; col < cols - 1; col += 1) {
          const p1 = pointAt(col, row);
          const p2 = pointAt(col + 1, row);
          const p3 = pointAt(col + 1, row + 1);
          const p4 = pointAt(col, row + 1);
          const pulse = 0.45 + 0.25 * Math.sin(col * 0.8 + row * 0.55);
          const fillAlpha = 0.015 + Math.max(0, pulse) * 0.018;

          ctx.beginPath();
          ctx.moveTo(p1.x, p1.y);
          ctx.lineTo(p2.x, p2.y);
          ctx.lineTo(p3.x, p3.y);
          ctx.lineTo(p4.x, p4.y);
          ctx.closePath();
          ctx.fillStyle = `rgba(150, 205, 255, ${fillAlpha.toFixed(4)})`;
          ctx.fill();
          ctx.strokeStyle = `rgba(169, 206, 255, ${(0.045 + Math.max(0, pulse) * 0.03).toFixed(4)})`;
          ctx.stroke();

          if ((row + col) % 3 === 0) {
            ctx.beginPath();
            ctx.moveTo(p1.x, p1.y);
            ctx.lineTo(p3.x, p3.y);
            ctx.strokeStyle = `rgba(108, 226, 197, ${(0.018 + Math.max(0, pulse) * 0.02).toFixed(4)})`;
            ctx.stroke();
          }
        }
      }
      ctx.restore();

      ctx.fillStyle = "rgba(255, 255, 255, 0.035)";
      for (let i = 0; i < 24; i += 1) {
        const seed = i * 17.371;
        const x = (width * ((i * 0.137) % 1) + Math.sin(seed) * 28 + width) % width;
        const y = (height * ((i * 0.193) % 1) + Math.cos(seed * 1.2) * 24 + height) % height;
        const radius = 1.2 + (i % 3) * 0.7;
        ctx.beginPath();
        ctx.arc(x, y, radius, 0, Math.PI * 2);
        ctx.fill();
      }
    }

    function resize() {
      cssWidth = Math.max(1, window.innerWidth);
      cssHeight = Math.max(1, window.innerHeight);
      pixelRatio = Math.max(1, Math.min(window.devicePixelRatio || 1, 2));
      canvas.width = Math.round(cssWidth * pixelRatio);
      canvas.height = Math.round(cssHeight * pixelRatio);
      drawFrame();
    }

    window.addEventListener("resize", resize);
    resize();
  }

  function init() {
    loadPathInputHistory();
    applyFieldMappingRebuildModeUi();
    setupDataQueryUi();
    setupWorkspaceTabs();
    setupPathBuilderUi();
    setupAuthUi();
    refreshUserMenu();
    restoreHomeState();
    setupDialogDragAndResize(importModal, importModalShell);
    setupDialogDragAndResize(changePasswordModal, changePasswordModalShell);
    setupDialogDragAndResize(userAdminModal, userAdminModalShell);
    setupDialogDragAndResize(createUserModal, createUserModalShell);
    setupDialogDragAndResize(adminResetUserModal, adminResetUserModalShell);
    setupDialogDragAndResize(logicViewerModal, logicViewerModalShell);
    setupBlockingModalGuards();
    setupSelectionHighlight();
    setupImportMapping();
    void bootstrapAuth();
    startBackgroundAnimation();
  }

  init();
})();
