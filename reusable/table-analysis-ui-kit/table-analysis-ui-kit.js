(function (global) {
  "use strict";

  function escapeHtml(value) {
    return String(value ?? "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");
  }

  function compareText(a, b) {
    return String(a ?? "").trim().toLowerCase().localeCompare(String(b ?? "").trim().toLowerCase(), undefined, {
      sensitivity: "base"
    });
  }

  function cycleSortDirection(direction) {
    if (direction === "asc") return "desc";
    if (direction === "desc") return "";
    return "asc";
  }

  function ensureArray(value) {
    return Array.isArray(value) ? value : [];
  }

  function createResizableSortableTable(table, tbody, options) {
    const getColumnIds = options.getColumnIds;
    const getSortValue = options.getSortValue;
    const minWidths = ensureArray(options.minWidths);
    const selectable = options.selectable !== false;
    const onSortChange = options.onSortChange || function () {};
    const widthStore = new Map();
    const sortStore = new Map();

    function getHeaderRow() {
      return table.tHead ? table.tHead.rows[table.tHead.rows.length - 1] : null;
    }

    function applyWidth(index, widthPx) {
      const col = table.querySelector(`colgroup col:nth-child(${index + 1})`);
      if (col) {
        col.style.width = `${widthPx}px`;
      }
      Array.from(table.querySelectorAll("tr")).forEach((tr) => {
        const cell = tr.children[index];
        if (cell) {
          cell.style.width = `${widthPx}px`;
          cell.style.minWidth = `${widthPx}px`;
          cell.style.maxWidth = `${widthPx}px`;
        }
      });
    }

    function applyStoredWidths() {
      getColumnIds().forEach((columnId, index) => {
        if (widthStore.has(columnId)) {
          applyWidth(index, widthStore.get(columnId));
        }
      });
    }

    function getActiveSorts() {
      return getColumnIds()
        .map((columnId, index) => ({ columnId, index, direction: sortStore.get(columnId) || "" }))
        .filter((item) => item.direction);
    }

    function updateIndicators() {
      const headerRow = getHeaderRow();
      if (!headerRow) return;
      const activeSorts = getActiveSorts();
      const orderMap = new Map(activeSorts.map((item, index) => [item.columnId, index + 1]));

      Array.from(headerRow.cells).forEach((th, index) => {
        const columnId = getColumnIds()[index];
        if (!columnId) return;

        th.classList.add("ta-kit-sortable");
        let label = th.querySelector(".ta-kit-header-label");
        const resizer = th.querySelector(".ta-kit-col-resizer");
        if (!label) {
          const text = Array.from(th.childNodes)
            .filter((node) => node !== resizer)
            .map((node) => node.textContent || "")
            .join("")
            .trim();
          Array.from(th.childNodes).forEach((node) => {
            if (node !== resizer) {
              th.removeChild(node);
            }
          });
          label = document.createElement("span");
          label.className = "ta-kit-header-label";
          label.textContent = text;
          th.insertBefore(label, resizer || null);
        }

        let indicator = th.querySelector(".ta-kit-sort-indicator");
        if (!indicator) {
          indicator = document.createElement("span");
          indicator.className = "ta-kit-sort-indicator";
          th.appendChild(indicator);
        }

        const direction = sortStore.get(columnId) || "";
        if (!direction) {
          indicator.innerHTML = "";
          return;
        }
        indicator.innerHTML = `<span class="ta-kit-sort-arrow ${direction}" aria-hidden="true"></span><span class="ta-kit-sort-order">${orderMap.get(columnId) || ""}</span>`;
      });
    }

    function ensureResizers() {
      const headerRow = getHeaderRow();
      if (!headerRow) return;
      const cells = Array.from(headerRow.cells);
      cells.forEach((th, index) => {
        const existing = th.querySelector(".ta-kit-col-resizer");
        if (index >= cells.length - 1) {
          if (existing) existing.remove();
          return;
        }
        if (!existing) {
          const handle = document.createElement("span");
          handle.className = "ta-kit-col-resizer";
          handle.dataset.col = String(index);
          handle.setAttribute("aria-hidden", "true");
          th.appendChild(handle);
        } else {
          existing.dataset.col = String(index);
        }
      });
    }

    function sortDomRows() {
      const activeSorts = getActiveSorts();
      const rows = Array.from(tbody.querySelectorAll("tr"));
      rows.sort((rowA, rowB) => {
        for (const sort of activeSorts) {
          const result = compareText(getSortValue(rowA, sort.columnId), getSortValue(rowB, sort.columnId));
          if (result !== 0) {
            return sort.direction === "asc" ? result : -result;
          }
        }
        return Number(rowA.dataset.originalOrder || 0) - Number(rowB.dataset.originalOrder || 0);
      });
      rows.forEach((row) => tbody.appendChild(row));
    }

    if (selectable) {
      table.classList.add("selectable");
      tbody.addEventListener("pointerdown", (event) => {
        const row = event.target.closest("tr");
        if (!row || row.classList.contains("result-empty")) return;
        tbody.querySelectorAll(".is-row-selected").forEach((item) => item.classList.remove("is-row-selected"));
        row.classList.add("is-row-selected");
      });
    }

    table.addEventListener("click", (event) => {
      if (event.target.closest(".ta-kit-col-resizer")) return;
      const th = event.target.closest("th.ta-kit-sortable");
      const headerRow = getHeaderRow();
      if (!th || !headerRow || th.parentElement !== headerRow) return;
      const index = Array.from(headerRow.cells).indexOf(th);
      const columnId = getColumnIds()[index];
      if (!columnId) return;
      const nextDirection = cycleSortDirection(sortStore.get(columnId) || "");
      if (!nextDirection) {
        sortStore.delete(columnId);
      } else {
        sortStore.set(columnId, nextDirection);
      }
      updateIndicators();
      onSortChange();
      sortDomRows();
    });

    table.addEventListener("pointerdown", (event) => {
      const handle = event.target.closest(".ta-kit-col-resizer");
      if (!handle) return;
      event.preventDefault();
      const index = Number(handle.dataset.col);
      const nextIndex = index + 1;
      const headerRow = getHeaderRow();
      const currentCell = headerRow && headerRow.children[index];
      const nextCell = headerRow && headerRow.children[nextIndex];
      if (!currentCell || !nextCell) return;
      const currentRect = currentCell.getBoundingClientRect();
      const nextRect = nextCell.getBoundingClientRect();
      const startX = event.clientX;

      function onMove(moveEvent) {
        const delta = moveEvent.clientX - startX;
        let currentWidth = currentRect.width + delta;
        let nextWidth = nextRect.width - delta;
        const minCurrent = minWidths[index] || 90;
        const minNext = minWidths[nextIndex] || 90;

        if (currentWidth < minCurrent) {
          currentWidth = minCurrent;
          nextWidth = currentRect.width + nextRect.width - currentWidth;
        }
        if (nextWidth < minNext) {
          nextWidth = minNext;
          currentWidth = currentRect.width + nextRect.width - nextWidth;
        }

        applyWidth(index, currentWidth);
        applyWidth(nextIndex, nextWidth);
        widthStore.set(getColumnIds()[index], currentWidth);
        widthStore.set(getColumnIds()[nextIndex], nextWidth);
      }

      function onUp() {
        window.removeEventListener("pointermove", onMove);
        window.removeEventListener("pointerup", onUp);
      }

      window.addEventListener("pointermove", onMove);
      window.addEventListener("pointerup", onUp);
    });

    ensureResizers();
    updateIndicators();
    applyStoredWidths();

    return {
      clearSort() {
        sortStore.clear();
        updateIndicators();
        onSortChange();
        sortDomRows();
      },
      refresh() {
        ensureResizers();
        updateIndicators();
        applyStoredWidths();
      }
    };
  }

  function ReusableDisplayTable(mountElement, options) {
    if (!mountElement) {
      throw new Error("ReusableDisplayTable requires a mount element.");
    }
    const config = options || {};
    const columns = ensureArray(config.columns);
    let baseRows = ensureArray(config.rows).slice();
    let currentRows = baseRows.slice();
    let showEn = true;
    let showDe = true;
    let fullLengthMode = false;
    const sortStore = new Map();
    const widthStore = new Map();

    mountElement.innerHTML = `
      <section class="ta-kit ta-kit-panel ta-kit-table-shell">
        <div class="ta-kit-head">
          <div class="ta-kit-title"></div>
          <div class="ta-kit-actions">
            <label class="ta-kit-toggle" data-role="toggle-en">
              <input type="checkbox" checked />
              <span>Display EN</span>
            </label>
            <label class="ta-kit-toggle" data-role="toggle-de">
              <input type="checkbox" checked />
              <span>Display DE</span>
            </label>
            <button class="ta-kit-btn ta-kit-btn-with-state" data-role="full-length" type="button">
              <span>Full Length Display</span>
              <span class="ta-kit-btn-state">OFF</span>
            </button>
            <button class="ta-kit-btn" data-role="clear-sort" type="button">Clear Sort</button>
          </div>
        </div>
        <div class="ta-kit-table-wrap">
          <table class="ta-kit-table selectable">
            <colgroup></colgroup>
            <thead></thead>
            <tbody></tbody>
          </table>
        </div>
      </section>
    `;

    const root = mountElement.querySelector(".ta-kit");
    const titleNode = root.querySelector(".ta-kit-title");
    const toggleEnLabel = root.querySelector('[data-role="toggle-en"]');
    const toggleDeLabel = root.querySelector('[data-role="toggle-de"]');
    const toggleEn = toggleEnLabel.querySelector("input");
    const toggleDe = toggleDeLabel.querySelector("input");
    const fullLengthBtn = root.querySelector('[data-role="full-length"]');
    const fullLengthState = fullLengthBtn.querySelector(".ta-kit-btn-state");
    const clearSortBtn = root.querySelector('[data-role="clear-sort"]');
    const table = root.querySelector("table");
    const colgroup = table.querySelector("colgroup");
    const thead = table.querySelector("thead");
    const tbody = table.querySelector("tbody");

    function getVisibleColumns() {
      return columns.filter((column) => {
        if (column.localeGroup === "en" && !showEn) return false;
        if (column.localeGroup === "de" && !showDe) return false;
        return true;
      });
    }

    function getColumnIds() {
      return getVisibleColumns().map((column) => column.key);
    }

    function renderHeader() {
      const visibleColumns = getVisibleColumns();
      colgroup.innerHTML = visibleColumns.map(() => "<col />").join("");
      const row = document.createElement("tr");
      visibleColumns.forEach((column) => {
        const th = document.createElement("th");
        th.innerHTML = `<span class="ta-kit-header-label">${escapeHtml(column.label)}</span><span class="ta-kit-sort-indicator"></span>`;
        row.appendChild(th);
      });
      thead.innerHTML = "";
      thead.appendChild(row);
    }

    function getActiveSorts() {
      return getColumnIds()
        .map((columnId, index) => ({ columnId, index, direction: sortStore.get(columnId) || "" }))
        .filter((item) => item.direction);
    }

    function updateHeaderIndicators() {
      const activeSorts = getActiveSorts();
      const orderMap = new Map(activeSorts.map((item, index) => [item.columnId, index + 1]));
      const headerRow = thead.rows[thead.rows.length - 1];
      if (!headerRow) return;

      Array.from(headerRow.cells).forEach((th, index) => {
        const columnId = getColumnIds()[index];
        if (!columnId) return;
        th.classList.add("ta-kit-sortable");
        const indicator = th.querySelector(".ta-kit-sort-indicator");
        const direction = sortStore.get(columnId) || "";
        if (!indicator) return;
        indicator.innerHTML = direction
          ? `<span class="ta-kit-sort-arrow ${direction}" aria-hidden="true"></span><span class="ta-kit-sort-order">${orderMap.get(columnId) || ""}</span>`
          : "";
      });
    }

    function applyColumnWidth(index, widthPx) {
      const col = table.querySelector(`colgroup col:nth-child(${index + 1})`);
      if (col) col.style.width = `${widthPx}px`;
      Array.from(table.querySelectorAll("tr")).forEach((tr) => {
        const cell = tr.children[index];
        if (cell) {
          cell.style.width = `${widthPx}px`;
          cell.style.minWidth = `${widthPx}px`;
          cell.style.maxWidth = `${widthPx}px`;
        }
      });
    }

    function applyStoredWidths() {
      getColumnIds().forEach((columnId, index) => {
        if (widthStore.has(columnId)) {
          applyColumnWidth(index, widthStore.get(columnId));
        }
      });
    }

    function ensureResizers() {
      const headerRow = thead.rows[thead.rows.length - 1];
      if (!headerRow) return;
      const cells = Array.from(headerRow.cells);
      cells.forEach((th, index) => {
        const existing = th.querySelector(".ta-kit-col-resizer");
        if (index >= cells.length - 1) {
          if (existing) existing.remove();
          return;
        }
        if (!existing) {
          const handle = document.createElement("span");
          handle.className = "ta-kit-col-resizer";
          handle.dataset.col = String(index);
          handle.setAttribute("aria-hidden", "true");
          th.appendChild(handle);
        }
      });
    }

    function applySorting() {
      const activeSorts = getActiveSorts();
      const wrapped = baseRows.map((row, index) => ({ row, index }));
      wrapped.sort((left, right) => {
        for (const sort of activeSorts) {
          const result = compareText(left.row[sort.columnId], right.row[sort.columnId]);
          if (result !== 0) {
            return sort.direction === "asc" ? result : -result;
          }
        }
        return left.index - right.index;
      });
      currentRows = wrapped.map((item) => item.row);
    }

    function renderBody() {
      const visibleColumns = getVisibleColumns();
      tbody.innerHTML = "";
      if (!currentRows.length) {
        const tr = document.createElement("tr");
        tr.innerHTML = `<td class="result-empty" colspan="${visibleColumns.length || 1}">No results found.</td>`;
        tbody.appendChild(tr);
        return;
      }

      currentRows.forEach((row, index) => {
        const tr = document.createElement("tr");
        tr.dataset.originalOrder = String(index);
        tr.innerHTML = visibleColumns.map((column) => `<td>${escapeHtml(row[column.key] ?? "")}</td>`).join("");
        tbody.appendChild(tr);
      });
    }

    function updateToolbar() {
      titleNode.textContent = `${config.title || "Result List"} (${currentRows.length})`;
      toggleEn.checked = showEn;
      toggleDe.checked = showDe;
      toggleEnLabel.classList.toggle("is-active", showEn);
      toggleDeLabel.classList.toggle("is-active", showDe);
      fullLengthBtn.classList.toggle("is-active", fullLengthMode);
      fullLengthState.textContent = fullLengthMode ? "ON" : "OFF";

      const hasEn = columns.some((column) => column.localeGroup === "en");
      const hasDe = columns.some((column) => column.localeGroup === "de");
      toggleEnLabel.style.display = hasEn ? "inline-flex" : "none";
      toggleDeLabel.style.display = hasDe ? "inline-flex" : "none";
    }

    function rerender() {
      applySorting();
      updateToolbar();
      renderHeader();
      updateHeaderIndicators();
      renderBody();
      ensureResizers();
      applyStoredWidths();
      table.classList.toggle("full-length-mode", fullLengthMode);
    }

    toggleEn.addEventListener("change", () => {
      showEn = toggleEn.checked;
      rerender();
    });

    toggleDe.addEventListener("change", () => {
      showDe = toggleDe.checked;
      rerender();
    });

    fullLengthBtn.addEventListener("click", () => {
      fullLengthMode = !fullLengthMode;
      updateToolbar();
      table.classList.toggle("full-length-mode", fullLengthMode);
    });

    clearSortBtn.addEventListener("click", () => {
      sortStore.clear();
      rerender();
    });

    table.addEventListener("click", (event) => {
      if (event.target.closest(".ta-kit-col-resizer")) return;
      const th = event.target.closest("th.ta-kit-sortable");
      const headerRow = thead.rows[thead.rows.length - 1];
      if (!th || !headerRow || th.parentElement !== headerRow) return;
      const index = Array.from(headerRow.cells).indexOf(th);
      const columnId = getColumnIds()[index];
      if (!columnId) return;
      const nextDirection = cycleSortDirection(sortStore.get(columnId) || "");
      if (!nextDirection) {
        sortStore.delete(columnId);
      } else {
        sortStore.set(columnId, nextDirection);
      }
      rerender();
    });

    table.addEventListener("pointerdown", (event) => {
      const handle = event.target.closest(".ta-kit-col-resizer");
      if (!handle) return;
      event.preventDefault();
      const headerRow = thead.rows[thead.rows.length - 1];
      const index = Number(handle.dataset.col);
      const nextIndex = index + 1;
      const currentCell = headerRow && headerRow.children[index];
      const nextCell = headerRow && headerRow.children[nextIndex];
      if (!currentCell || !nextCell) return;
      const startX = event.clientX;
      const startWidth = currentCell.getBoundingClientRect().width;
      const nextStartWidth = nextCell.getBoundingClientRect().width;

      function onMove(moveEvent) {
        const delta = moveEvent.clientX - startX;
        const visibleColumns = getVisibleColumns();
        const currentKey = visibleColumns[index] && visibleColumns[index].key;
        const nextKey = visibleColumns[nextIndex] && visibleColumns[nextIndex].key;
        if (!currentKey || !nextKey) return;
        let currentWidth = startWidth + delta;
        let nextWidth = nextStartWidth - delta;
        if (currentWidth < 90) {
          currentWidth = 90;
          nextWidth = startWidth + nextStartWidth - currentWidth;
        }
        if (nextWidth < 90) {
          nextWidth = 90;
          currentWidth = startWidth + nextStartWidth - nextWidth;
        }
        applyColumnWidth(index, currentWidth);
        applyColumnWidth(nextIndex, nextWidth);
        widthStore.set(currentKey, currentWidth);
        widthStore.set(nextKey, nextWidth);
      }

      function onUp() {
        window.removeEventListener("pointermove", onMove);
        window.removeEventListener("pointerup", onUp);
      }

      window.addEventListener("pointermove", onMove);
      window.addEventListener("pointerup", onUp);
    });

    tbody.addEventListener("pointerdown", (event) => {
      const row = event.target.closest("tr");
      if (!row || row.classList.contains("result-empty")) return;
      tbody.querySelectorAll(".is-row-selected").forEach((item) => item.classList.remove("is-row-selected"));
      row.classList.add("is-row-selected");
    });

    rerender();

    return {
      setRows(nextRows) {
        baseRows = ensureArray(nextRows).slice();
        rerender();
      },
      getState() {
        return {
          showEn,
          showDe,
          fullLengthMode,
          rowCount: currentRows.length
        };
      },
      rerender
    };
  }

  function ReusableValueSelectionModal(options) {
    const config = options || {};
    const overlay = document.createElement("section");
    overlay.className = "ta-kit-modal ta-kit-hidden";
    overlay.innerHTML = `
      <div class="ta-kit-modal-shell ta-kit-panel" data-resize-min-width="860" data-resize-min-height="480">
        <div class="ta-kit-modal-head">
          <h2>Object Value Selection Screen</h2>
          <div class="ta-kit-window-controls">
            <button class="ta-kit-btn tiny" data-role="maximize" type="button" title="Maximize or restore">□</button>
            <button class="ta-kit-btn tiny" data-role="close" type="button" title="Close">X</button>
          </div>
        </div>
        <div class="ta-kit-value-wrap">
          <table class="ta-kit-value-table">
            <colgroup>
              <col class="col-exclude" />
              <col class="col-operator" />
              <col />
              <col />
            </colgroup>
            <thead>
              <tr>
                <th>Exclude</th>
                <th>Operator</th>
                <th>From</th>
                <th>To</th>
              </tr>
            </thead>
            <tbody></tbody>
          </table>
          <div class="ta-kit-value-tools">
            <button class="ta-kit-btn compact" data-role="add-rows" type="button">Add 2 Rows</button>
            <button class="ta-kit-btn compact" data-role="clear-sort" type="button">Clear Sort</button>
          </div>
        </div>
        <div class="ta-kit-modal-actions">
          <button class="ta-kit-btn" data-role="clear-selection" type="button">Clear Selection</button>
          <button class="ta-kit-btn" data-role="copy-clipboard" type="button">Copy from clipboard</button>
          <button class="ta-kit-btn strong" data-role="confirm" type="button">Confirm</button>
        </div>
        <div class="ta-kit-resize-handle" aria-hidden="true"></div>
      </div>
    `;
    document.body.appendChild(overlay);

    const shell = overlay.querySelector(".ta-kit-modal-shell");
    const titleNode = overlay.querySelector("h2");
    const tbody = overlay.querySelector("tbody");
    const table = overlay.querySelector("table");
    const maximizeBtn = overlay.querySelector('[data-role="maximize"]');
    const closeBtn = overlay.querySelector('[data-role="close"]');
    const addRowsBtn = overlay.querySelector('[data-role="add-rows"]');
    const clearSortBtn = overlay.querySelector('[data-role="clear-sort"]');
    const clearSelectionBtn = overlay.querySelector('[data-role="clear-selection"]');
    const copyClipboardBtn = overlay.querySelector('[data-role="copy-clipboard"]');
    const confirmBtn = overlay.querySelector('[data-role="confirm"]');
    const resizeHandle = overlay.querySelector(".ta-kit-resize-handle");

    let rules = [];
    let activeField = "";
    let onConfirm = config.onConfirm || function () {};
    let maximized = false;
    let dragState = null;
    let resizeState = null;

    function computeOperator(fromValue, toValue, excluded) {
      const hasFrom = fromValue.length > 0;
      const hasTo = toValue.length > 0;
      if (hasFrom && hasTo) return excluded ? "] [" : "[ ]";
      if (hasFrom && !hasTo) return excluded ? "!=" : "=";
      return "";
    }

    function validateRow(tr) {
      const excluded = tr.querySelector(".exclude-flag").checked;
      const fromValue = tr.querySelector(".from-input").value.trim();
      const toValue = tr.querySelector(".to-input").value.trim();
      if (!fromValue && toValue) {
        return "Cannot input To without From.";
      }
      if (excluded && !fromValue && !toValue) {
        return "Cannot mark Exclude on an empty row.";
      }
      return "";
    }

    function repaintRow(tr) {
      const excluded = tr.querySelector(".exclude-flag").checked;
      const fromValue = tr.querySelector(".from-input").value.trim();
      const toValue = tr.querySelector(".to-input").value.trim();
      const operator = tr.querySelector(".operator-text");
      const error = validateRow(tr);
      const hasContent = fromValue.length > 0 || toValue.length > 0;
      operator.textContent = error ? "" : computeOperator(fromValue, toValue, excluded);
      tr.classList.toggle("has-content", hasContent && !excluded && !error);
      tr.classList.toggle("excluded", excluded && !error && fromValue.length > 0);
    }

    function buildRow(index) {
      const tr = document.createElement("tr");
      tr.className = "ta-kit-value-row";
      tr.dataset.originalOrder = String(index);
      tr.innerHTML = [
        '<td class="readonly-cell"><input type="checkbox" class="exclude-flag" /></td>',
        '<td class="readonly-cell operator-text"></td>',
        '<td><input class="from-input" type="text" maxlength="40" /></td>',
        '<td><div class="ta-kit-to-cell-wrap"><input class="to-input" type="text" maxlength="40" /><button class="ta-kit-row-delete" type="button" title="Delete row" aria-label="Delete row">x</button></div></td>'
      ].join("");
      tr.addEventListener("input", function () { repaintRow(tr); });
      tr.addEventListener("change", function () { repaintRow(tr); });
      repaintRow(tr);
      return tr;
    }

    function setRules(nextRules) {
      rules = ensureArray(nextRules).slice();
      tbody.innerHTML = "";
      const rowCount = Math.max(3, rules.length);
      for (let i = 0; i < rowCount; i += 1) {
        const tr = buildRow(i);
        tbody.appendChild(tr);
      }
      rules.forEach((rule, index) => {
        const tr = tbody.children[index];
        if (!tr) return;
        tr.querySelector(".exclude-flag").checked = Boolean(rule.exclude);
        tr.querySelector(".from-input").value = rule.from || "";
        tr.querySelector(".to-input").value = rule.to || "";
        repaintRow(tr);
      });
      interactive.refresh();
    }

    function appendRows(count) {
      const start = tbody.children.length;
      for (let i = 0; i < count; i += 1) {
        tbody.appendChild(buildRow(start + i));
      }
      interactive.refresh();
    }

    function collectRules() {
      const nextRules = [];
      for (const tr of tbody.querySelectorAll("tr")) {
        const error = validateRow(tr);
        if (error) {
          throw new Error(error);
        }
        const exclude = tr.querySelector(".exclude-flag").checked;
        const from = tr.querySelector(".from-input").value.trim();
        const to = tr.querySelector(".to-input").value.trim();
        if (!from && !to) continue;
        nextRules.push({ exclude, from, to });
      }
      return nextRules;
    }

    const interactive = createResizableSortableTable(table, tbody, {
      getColumnIds() {
        return ["exclude", "operator", "from", "to"];
      },
      getSortValue(row, columnId) {
        if (columnId === "exclude") return row.querySelector(".exclude-flag").checked ? "x" : "";
        if (columnId === "operator") return row.querySelector(".operator-text").textContent || "";
        if (columnId === "from") return row.querySelector(".from-input").value || "";
        if (columnId === "to") return row.querySelector(".to-input").value || "";
        return "";
      },
      minWidths: [68, 78, 140, 160],
      selectable: true
    });

    function close() {
      overlay.classList.add("ta-kit-hidden");
      maximized = false;
      dragState = null;
      resizeState = null;
      shell.classList.remove("maximized", "dragging", "resizing");
      maximizeBtn.textContent = "□";
      activeField = "";
    }

    function open(openOptions) {
      const data = openOptions || {};
      activeField = data.fieldName || "";
      titleNode.textContent = data.title || "Object Value Selection Screen";
      if (typeof data.onConfirm === "function") {
        onConfirm = data.onConfirm;
      }
      shell.style.removeProperty("--ta-kit-modal-dx");
      shell.style.removeProperty("--ta-kit-modal-dy");
      shell.style.removeProperty("width");
      shell.style.removeProperty("height");
      setRules(data.rules || []);
      overlay.classList.remove("ta-kit-hidden");
    }

    closeBtn.addEventListener("click", close);
    addRowsBtn.addEventListener("click", function () { appendRows(2); });
    clearSortBtn.addEventListener("click", function () { interactive.clearSort(); });
    clearSelectionBtn.addEventListener("click", function () { setRules([]); });
    copyClipboardBtn.addEventListener("click", async function () {
      try {
        const text = await navigator.clipboard.readText();
        const lines = text.split(/\r?\n/).map((line) => line.trim()).filter(Boolean);
        setRules(lines.map((line) => {
          const parts = line.split(/[\t,;]/).map((part) => part.trim());
          const flag = parts[0] || "";
          return {
            exclude: /^x$/i.test(flag),
            from: parts[1] || "",
            to: parts[2] || ""
          };
        }));
      } catch (error) {
        if (typeof config.onError === "function") {
          config.onError(error);
        }
      }
    });
    confirmBtn.addEventListener("click", function () {
      try {
        const nextRules = collectRules();
        onConfirm(nextRules, activeField);
        close();
      } catch (error) {
        if (typeof config.onError === "function") {
          config.onError(error);
        }
      }
    });

    overlay.addEventListener("click", function (event) {
      if (event.target === overlay) {
        event.stopPropagation();
      }
    });

    maximizeBtn.addEventListener("click", function () {
      maximized = !maximized;
      shell.classList.toggle("maximized", maximized);
      maximizeBtn.textContent = maximized ? "❐" : "□";
      if (maximized) {
        shell.style.removeProperty("--ta-kit-modal-dx");
        shell.style.removeProperty("--ta-kit-modal-dy");
      }
    });

    const head = overlay.querySelector(".ta-kit-modal-head");
    head.addEventListener("pointerdown", function (event) {
      if (maximized) return;
      if (event.target.closest("button")) return;
      dragState = {
        startX: event.clientX,
        startY: event.clientY,
        originDx: Number.parseFloat(shell.style.getPropertyValue("--ta-kit-modal-dx")) || 0,
        originDy: Number.parseFloat(shell.style.getPropertyValue("--ta-kit-modal-dy")) || 0
      };
      shell.classList.add("dragging");
    });

    resizeHandle.addEventListener("pointerdown", function (event) {
      if (maximized) return;
      event.preventDefault();
      const rect = shell.getBoundingClientRect();
      resizeState = {
        startX: event.clientX,
        startY: event.clientY,
        startWidth: rect.width,
        startHeight: rect.height,
        minWidth: Number(shell.dataset.resizeMinWidth) || 420,
        minHeight: Number(shell.dataset.resizeMinHeight) || 220
      };
      shell.classList.add("resizing");
    });

    window.addEventListener("pointermove", function (event) {
      if (dragState) {
        const dx = dragState.originDx + (event.clientX - dragState.startX);
        const dy = dragState.originDy + (event.clientY - dragState.startY);
        shell.style.setProperty("--ta-kit-modal-dx", `${dx}px`);
        shell.style.setProperty("--ta-kit-modal-dy", `${dy}px`);
      }
      if (resizeState) {
        const nextWidth = Math.max(resizeState.minWidth, Math.min(window.innerWidth * 0.98, resizeState.startWidth + (event.clientX - resizeState.startX)));
        const nextHeight = Math.max(resizeState.minHeight, Math.min(window.innerHeight * 0.94, resizeState.startHeight + (event.clientY - resizeState.startY)));
        shell.style.width = `${nextWidth}px`;
        shell.style.height = `${nextHeight}px`;
      }
    });

    window.addEventListener("pointerup", function () {
      if (dragState) {
        dragState = null;
        shell.classList.remove("dragging");
      }
      if (resizeState) {
        resizeState = null;
        shell.classList.remove("resizing");
      }
    });

    tbody.addEventListener("click", function (event) {
      const deleteButton = event.target.closest(".ta-kit-row-delete");
      if (!deleteButton) return;
      const row = deleteButton.closest("tr");
      if (row) {
        row.remove();
      }
      if (!tbody.children.length) {
        tbody.appendChild(buildRow(0));
      }
      interactive.refresh();
    });

    setRules([]);

    return {
      open,
      close,
      destroy() {
        overlay.remove();
      },
      getRules() {
        return collectRules();
      },
      setRules
    };
  }

  global.TableAnalysisUIKit = {
    createDisplayTable: ReusableDisplayTable,
    createValueSelectionModal: ReusableValueSelectionModal
  };
})(window);