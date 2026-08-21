(() => {
  const cookieValue = (name) => {
    const prefix = `${name}=`;
    for (const part of document.cookie.split(";")) {
      const value = part.trim();
      if (value.startsWith(prefix)) {
        return decodeURIComponent(value.slice(prefix.length));
      }
    }
    return "";
  };

  for (const form of document.querySelectorAll("form")) {
    if ((form.getAttribute("method") || "get").toLowerCase() !== "post") continue;
    form.addEventListener("submit", (event) => {
      const submitter = event.submitter;
      const confirmMessage = submitter?.getAttribute("data-confirm-message");
      if (confirmMessage && !window.confirm(confirmMessage)) {
        event.preventDefault();
        return;
      }
      const token = cookieValue("phr_app_csrf");
      if (token && !form.querySelector("input[name='_csrf_token']")) {
        const input = document.createElement("input");
        input.type = "hidden";
        input.name = "_csrf_token";
        input.value = token;
        form.appendChild(input);
      }

      form.querySelectorAll("input[data-submit-shadow='true']").forEach((node) => node.remove());
      if (!submitter || !submitter.name) return;
      const shadow = document.createElement("input");
      shadow.type = "hidden";
      shadow.name = submitter.name;
      shadow.value = submitter.value;
      shadow.dataset.submitShadow = "true";
      form.appendChild(shadow);
    });
  }

  const normalize = (value) => String(value || "").toLocaleLowerCase().replace(/\s+/g, "");
  const escapeHtml = (value) => String(value || "").replace(/[&<>"']/g, (char) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#39;",
  }[char]));
  const filters = new Map();
  const processingOverlay = document.querySelector("[data-processing-overlay]");
  const processingTitle = document.querySelector("[data-processing-overlay-title]");
  const processingMessage = document.querySelector("[data-processing-overlay-message]");
  let navigationStarted = false;

  const showProcessingOverlay = (title, message) => {
    if (processingTitle) processingTitle.textContent = title || "処理しています";
    if (processingMessage) processingMessage.textContent = message || "しばらくお待ちください。";
    if (processingOverlay) processingOverlay.hidden = false;
  };

  const isPlainLeftClick = (event) => (
    event.button === 0
    && !event.metaKey
    && !event.ctrlKey
    && !event.shiftKey
    && !event.altKey
  );

  for (const link of document.querySelectorAll("a.home-menu-card[href]")) {
    link.addEventListener("click", (event) => {
      if (!isPlainLeftClick(event) || link.target === "_blank") {
        return;
      }
      if (navigationStarted || link.classList.contains("is-navigating")) {
        event.preventDefault();
        return;
      }
      navigationStarted = true;
      link.classList.add("is-navigating");
      link.setAttribute("aria-busy", "true");
      showProcessingOverlay("画面を開いています", "移動先の画面を読み込んでいます。");
      const badge = link.querySelector(".status-pill");
      if (badge) {
        badge.textContent = "移動中";
        badge.classList.remove("status-ready", "status-pending", "status-muted", "status-danger");
        badge.classList.add("status-pending");
      }
    });
  }

  for (const button of document.querySelectorAll("[data-scroll-target]")) {
    button.addEventListener("click", () => {
      const selector = button.getAttribute("data-scroll-target");
      const target = selector ? document.querySelector(selector) : null;
      if (!target) return;
      target.scrollIntoView({ behavior: "smooth", block: "start" });
      target.classList.add("is-scroll-target-panel");
      window.setTimeout(() => target.classList.remove("is-scroll-target-panel"), 1600);
    });
  }

  for (const button of document.querySelectorAll("[data-copy-target]")) {
    button.addEventListener("click", async (event) => {
      event.preventDefault();
      event.stopPropagation();
      const selector = button.getAttribute("data-copy-target");
      const target = selector ? document.querySelector(selector) : null;
      const text = target?.textContent || "";
      if (!text) return;
      const originalLabel = button.textContent;
      try {
        await navigator.clipboard.writeText(text);
        button.textContent = "コピーしました";
      } catch {
        button.textContent = "コピー失敗";
      }
      window.setTimeout(() => {
        button.textContent = originalLabel;
      }, 1600);
    });
  }

  for (const input of document.querySelectorAll("[data-live-filter-input]")) {
    const tableSelector = input.getAttribute("data-live-filter-input");
    const table = tableSelector ? document.querySelector(tableSelector) : null;
    if (!table) continue;

    const rows = Array.from(table.querySelectorAll("tbody tr[data-filter-text]"));
    const tableFilters = {
      keywordInput: input,
      toggleGroups: new Map(),
      toggleModes: new Map(),
      toggleMatchModes: new Map(),
    };
    filters.set(tableSelector, tableFilters);
    const emptyMessage = table.dataset.emptyMessage || "一致する行はありません。";
    const emptyRow = document.createElement("tr");
    emptyRow.hidden = true;
    const emptyCell = document.createElement("td");
    emptyCell.colSpan = table.querySelectorAll("thead th").length || 1;
    emptyCell.textContent = emptyMessage;
    emptyRow.appendChild(emptyCell);
    table.querySelector("tbody").appendChild(emptyRow);

    const applyFilter = () => {
      const keyword = normalize(input.value);
      let visibleCount = 0;
      for (const row of rows) {
        const keywordMatched = !keyword || normalize(row.dataset.filterText).includes(keyword);
        let togglesMatched = true;
        for (const [field, values] of tableFilters.toggleGroups.entries()) {
          if (!values.size) continue;
          if (values.has("__ALL__")) continue;
          const matchMode = tableFilters.toggleMatchModes.get(field) || "exactAny";
          const rowValue = String(row.dataset[field] || "");
          if (matchMode === "allTokens") {
            const tokens = new Set(rowValue.split(/\s+/).filter(Boolean));
            for (const value of values) {
              if (!tokens.has(value)) {
                togglesMatched = false;
                break;
              }
            }
            if (!togglesMatched) break;
            continue;
          }
          if (!values.has(rowValue)) {
            togglesMatched = false;
            break;
          }
        }
        const matched = keywordMatched && togglesMatched;
        row.hidden = !matched;
        if (row.nextElementSibling?.classList.contains("nested-member-row")) {
          row.nextElementSibling.hidden = !matched;
        }
        if (matched) visibleCount += 1;
      }
      emptyRow.hidden = visibleCount !== 0;
    };

    tableFilters.applyFilter = applyFilter;
    input.addEventListener("input", applyFilter);
    applyFilter();
  }

  for (const button of document.querySelectorAll("[data-live-filter-toggle]")) {
    const tableSelector = button.getAttribute("data-live-filter-toggle");
    const field = button.dataset.filterField;
    const value = button.dataset.filterValue;
    const tableFilters = filters.get(tableSelector);
    if (!tableFilters || !field || value == null) continue;
    const mode = button.dataset.filterMode || "multi";
    const matchMode = button.dataset.filterMatch || "exactAny";
    tableFilters.toggleModes.set(field, mode);
    tableFilters.toggleMatchModes.set(field, matchMode);

    button.setAttribute("aria-pressed", "false");
    button.addEventListener("click", () => {
      const group = tableFilters.toggleGroups.get(field) || new Set();
      if (mode === "single") {
        for (const other of document.querySelectorAll(`[data-live-filter-toggle="${tableSelector}"][data-filter-field="${field}"]`)) {
          other.classList.remove("is-active");
          other.setAttribute("aria-pressed", "false");
        }
        group.clear();
        group.add(value);
        button.classList.add("is-active");
        button.setAttribute("aria-pressed", "true");
      } else {
        if (group.has(value)) {
          group.delete(value);
          button.classList.remove("is-active");
          button.setAttribute("aria-pressed", "false");
        } else {
          group.add(value);
          button.classList.add("is-active");
          button.setAttribute("aria-pressed", "true");
        }
      }
      tableFilters.toggleGroups.set(field, group);
      tableFilters.applyFilter();
    });
    if (button.classList.contains("is-active")) {
      button.setAttribute("aria-pressed", "true");
      const group = tableFilters.toggleGroups.get(field) || new Set();
      if (mode === "single") group.clear();
      group.add(value);
      tableFilters.toggleGroups.set(field, group);
      tableFilters.applyFilter();
    }
  }

  for (const button of document.querySelectorAll("[data-server-filter-toggle]")) {
    button.setAttribute("aria-pressed", button.classList.contains("is-active") ? "true" : "false");
    button.addEventListener("click", () => {
      const field = button.getAttribute("data-server-filter-toggle");
      const value = button.dataset.filterValue || "";
      const form = button.closest("form");
      if (!field || !form) return;
      const input = form.querySelector(`[data-server-filter-value="${field}"]`);
      if (!input) return;
      const isActive = input.value !== value;
      input.value = isActive ? value : "";
      for (const peer of form.querySelectorAll(`[data-server-filter-toggle="${field}"]`)) {
        const peerActive = isActive && peer.dataset.filterValue === value;
        peer.classList.toggle("is-active", peerActive);
        peer.setAttribute("aria-pressed", peerActive ? "true" : "false");
      }
    });
  }

  const parseSortValue = (cell, type) => {
    const raw = String(cell?.dataset.sortValue ?? cell?.textContent ?? "").trim();
    if (type === "number" || type === "percent") {
      if (!raw || raw === "-") return Number.NEGATIVE_INFINITY;
      const value = Number(raw.replace("%", "").replace(/,/g, ""));
      return Number.isFinite(value) ? value : Number.NEGATIVE_INFINITY;
    }
    return normalize(raw);
  };

  for (const table of document.querySelectorAll("[data-sortable-table]")) {
    const tbody = table.querySelector("tbody");
    if (!tbody) continue;
    const headers = Array.from(table.querySelectorAll("th[data-sort-index]"));
    const sortableRows = () => Array.from(tbody.querySelectorAll("tr[data-filter-text]"));
    for (const header of headers) {
      const button = header.querySelector("button");
      if (!button) continue;
      button.addEventListener("click", () => {
        const index = Number(header.dataset.sortIndex || "0");
        const type = header.dataset.sortType || "text";
        const currentDirection = header.dataset.sortDirection === "asc" ? "asc" : "desc";
        const nextDirection = currentDirection === "asc" ? "desc" : "asc";
        for (const other of headers) {
          other.dataset.sortDirection = "";
          other.removeAttribute("aria-sort");
        }
        header.dataset.sortDirection = nextDirection;
        header.setAttribute("aria-sort", nextDirection === "asc" ? "ascending" : "descending");
        const rows = sortableRows().map((row, originalIndex) => ({ row, originalIndex }));
        rows.sort((left, right) => {
          const leftValue = parseSortValue(left.row.children[index], type);
          const rightValue = parseSortValue(right.row.children[index], type);
          let compared = 0;
          if (typeof leftValue === "number" && typeof rightValue === "number") {
            compared = leftValue - rightValue;
          } else {
            compared = String(leftValue).localeCompare(String(rightValue), "ja");
          }
          if (compared === 0) compared = left.originalIndex - right.originalIndex;
          return nextDirection === "asc" ? compared : -compared;
        });
        for (const item of rows) {
          tbody.appendChild(item.row);
        }
      });
    }
  }

  const normalizeDateInputValue = (raw) => {
    const text = String(raw || "").trim();
    if (!text) return "";
    const digits = text.replace(/[^\d]/g, "");
    if (digits.length !== 8) return text;
    const year = Number(digits.slice(0, 4));
    const month = Number(digits.slice(4, 6));
    const day = Number(digits.slice(6, 8));
    const date = new Date(year, month - 1, day);
    if (
      date.getFullYear() !== year
      || date.getMonth() !== month - 1
      || date.getDate() !== day
    ) {
      return text;
    }
    return `${digits.slice(0, 4)}-${digits.slice(4, 6)}-${digits.slice(6, 8)}`;
  };

  for (const input of document.querySelectorAll("input[data-date-normalize='true']")) {
    const toTextInput = () => {
      input.type = "text";
      input.value = normalizeDateInputValue(input.value);
    };
    input.value = normalizeDateInputValue(input.value);
    input.inputMode = "numeric";
    input.autocomplete = "off";
    input.addEventListener("focus", () => {
      input.value = normalizeDateInputValue(input.value);
      input.type = "date";
      if (typeof input.showPicker === "function") {
        try {
          input.showPicker();
        } catch {
          // Some browsers only allow showPicker from a direct user gesture.
        }
      }
    });
    input.addEventListener("blur", toTextInput);
    input.addEventListener("change", toTextInput);
  }

  const closeHelpPopovers = (exceptId = "") => {
    for (const popover of document.querySelectorAll(".help-popover")) {
      if (popover.classList.contains("hover-help-popover")) continue;
      if (exceptId && popover.id === exceptId) continue;
      popover.hidden = true;
      const toggle = document.querySelector(`[data-help-toggle="${popover.id}"]`);
      if (toggle) toggle.setAttribute("aria-expanded", "false");
    }
  };

  for (const button of document.querySelectorAll("[data-help-toggle]")) {
    button.addEventListener("click", (event) => {
      event.stopPropagation();
      const targetId = button.getAttribute("data-help-toggle") || "";
      const popover = document.getElementById(targetId);
      if (!popover) return;
      const willOpen = popover.hidden;
      closeHelpPopovers(targetId);
      popover.hidden = !willOpen;
      button.setAttribute("aria-expanded", willOpen ? "true" : "false");
    });
  }

  document.addEventListener("click", (event) => {
    const target = event.target;
    if (target instanceof Element && (target.closest("[data-help-toggle]") || target.closest(".help-popover"))) return;
    closeHelpPopovers();
  });

  for (const input of document.querySelectorAll(".binary-switch input")) {
    const update = () => {
      const label = input.closest(".binary-switch");
      if (!label) return;
      label.classList.toggle("switch-on", input.checked);
      label.classList.toggle("switch-off", !input.checked);
    };
    input.addEventListener("change", update);
    update();
  }

  const floatingCategoryNav = document.querySelector("[data-floating-category-nav]");
  const floatingCategoryToggle = document.querySelector("[data-floating-category-toggle]");
  if (floatingCategoryNav && floatingCategoryToggle) {
    const setCollapsed = (collapsed) => {
      floatingCategoryNav.classList.toggle("is-collapsed", collapsed);
      floatingCategoryToggle.setAttribute("aria-expanded", collapsed ? "false" : "true");
      floatingCategoryToggle.setAttribute("aria-label", collapsed ? "カテゴリメニューを開く" : "カテゴリメニューを閉じる");
    };
    setCollapsed(false);
    floatingCategoryToggle.addEventListener("click", () => {
      const collapsed = !floatingCategoryNav.classList.contains("is-collapsed");
      setCollapsed(collapsed);
    });
  }

  const setAllStepsButtons = Array.from(document.querySelectorAll("[data-set-all-steps]"));
  const processingStepCheckboxes = Array.from(document.querySelectorAll("[data-processing-step-checkbox]"));
  const selectedRunButton = document.querySelector("[data-run-selected-steps]");
  if (processingStepCheckboxes.length) {
    const updateProcessingStepControls = () => {
      const checkedCount = processingStepCheckboxes.filter((input) => input.checked).length;
      const allChecked = checkedCount === processingStepCheckboxes.length;
      const noneChecked = checkedCount === 0;
      for (const button of setAllStepsButtons) {
        const mode = button.getAttribute("data-set-all-steps");
        button.hidden = (mode === "on" && allChecked) || (mode === "off" && noneChecked);
      }
      if (selectedRunButton) {
        selectedRunButton.disabled = checkedCount === 0;
      }
      for (const input of processingStepCheckboxes) {
        const label = input.closest(".processing-step-toggle");
        if (!label) continue;
        label.classList.toggle("is-on", input.checked);
        label.classList.toggle("is-off", !input.checked);
        const actionLabel = label.querySelector("[data-processing-step-toggle-label]");
        if (actionLabel) actionLabel.textContent = input.checked ? "外す" : "選択";
      }
    };
    for (const button of setAllStepsButtons) {
      button.addEventListener("click", () => {
        const checked = button.getAttribute("data-set-all-steps") === "on";
        for (const input of processingStepCheckboxes) {
          input.checked = checked;
          input.dispatchEvent(new Event("change"));
        }
        updateProcessingStepControls();
      });
    }
    for (const input of processingStepCheckboxes) {
      input.addEventListener("change", updateProcessingStepControls);
    }
    updateProcessingStepControls();
  }

  const modeFromSourceKinds = (selected) => {
    const hasXml = selected.has("XML");
    const hasCsv = selected.has("CSV");
    const hasPaper = selected.has("PAPER");
    if (!hasXml && !hasCsv && !hasPaper) return "UNKNOWN";
    if (hasXml && !hasCsv && !hasPaper) return "XML_ONLY";
    if (!hasXml && hasCsv && !hasPaper) return "CSV_ONLY";
    if (hasXml && hasCsv && !hasPaper) return "XML_CSV_MERGE";
    if (!hasXml && !hasCsv && hasPaper) return "PAPER_ONLY";
    if (hasXml && !hasCsv && hasPaper) return "XML_PAPER_MERGE";
    if (!hasXml && hasCsv && hasPaper) return "CSV_PAPER_MERGE";
    return "XML_CSV_PAPER_MERGE";
  };

  const sourceKindsFromMode = (mode) => {
    const values = {
      UNKNOWN: [],
      XML_ONLY: ["XML"],
      CSV_ONLY: ["CSV"],
      XML_CSV_MERGE: ["XML", "CSV"],
      PAPER_ONLY: ["PAPER"],
      XML_PAPER_MERGE: ["XML", "PAPER"],
      CSV_PAPER_MERGE: ["CSV", "PAPER"],
      XML_CSV_PAPER_MERGE: ["XML", "CSV", "PAPER"],
    };
    return new Set(values[mode] || []);
  };

  const sourceModeLabel = (mode) => {
    const labels = {
      UNKNOWN: "未設定",
      XML_ONLY: "XMLのみ",
      CSV_ONLY: "CSVのみ",
      XML_CSV_MERGE: "XML+CSV",
      PAPER_ONLY: "紙のみ",
      XML_PAPER_MERGE: "XML+紙",
      CSV_PAPER_MERGE: "CSV+紙",
      XML_CSV_PAPER_MERGE: "XML+CSV+紙",
    };
    return labels[mode] || "未設定";
  };

  for (const picker of document.querySelectorAll("[data-source-kind-picker]")) {
    const hidden = picker.querySelector("[data-source-mode-value]");
    const label = picker.querySelector("[data-source-kind-label]");
    const options = Array.from(picker.querySelectorAll("[data-source-kind-option]"));

    const update = () => {
      const selected = new Set(options.filter((option) => option.checked).map((option) => option.value));
      const mode = modeFromSourceKinds(selected);
      if (hidden) hidden.value = mode;
      if (label) label.textContent = sourceModeLabel(mode);
    };

    const initial = sourceKindsFromMode((hidden && hidden.value) || picker.dataset.currentMode || "UNKNOWN");
    for (const option of options) {
      option.checked = initial.has(option.value);
      option.addEventListener("change", update);
    }
    update();
  }

  const closeModal = (modal) => {
    if (!modal) return;
    modal.hidden = true;
    document.body.classList.remove("has-open-modal");
  };

  for (const button of document.querySelectorAll("[data-modal-open]")) {
    button.addEventListener("click", () => {
      const modal = document.getElementById(button.getAttribute("data-modal-open") || "");
      if (!modal) return;
      modal.hidden = false;
      document.body.classList.add("has-open-modal");
      const firstInput = modal.querySelector("[data-modal-focus]:not([disabled])")
        || modal.querySelector("input:not([disabled]), select, textarea, button");
      if (firstInput) firstInput.focus();
    });
  }

  for (const button of document.querySelectorAll("[data-modal-close]")) {
    button.addEventListener("click", () => closeModal(button.closest(".edit-modal")));
  }

  const facilityCodesInput = document.getElementById("facility-codes-input");
  const facilityCodesSummary = document.getElementById("facility-codes-summary");
  const facilityCodeValues = () => {
    if (!facilityCodesInput) return [];
    return facilityCodesInput.value
      .split(/[\s,，、]+/)
      .map((value) => value.trim())
      .filter(Boolean);
  };
  const updateFacilityCodesSummary = () => {
    if (!facilityCodesInput || !facilityCodesSummary) return;
    const values = facilityCodeValues();
    facilityCodesSummary.textContent = values.length ? `${values.length}施設を指定中` : "未指定: 全施設";
  };
  const updateFacilityAddButtons = () => {
    const values = new Set(facilityCodeValues());
    for (const button of document.querySelectorAll("[data-export-facility-add]")) {
      const code = String(button.getAttribute("data-facility-code") || "").trim();
      const added = code && values.has(code);
      button.textContent = added ? "追加済み" : "追加";
      button.disabled = Boolean(added);
    }
  };
  const updateFacilityPickerState = () => {
    updateFacilityCodesSummary();
    updateFacilityAddButtons();
  };
  if (facilityCodesInput) {
    facilityCodesInput.addEventListener("input", updateFacilityPickerState);
    updateFacilityPickerState();
  }
  for (const button of document.querySelectorAll("[data-export-facility-add]")) {
    button.addEventListener("click", () => {
      if (!facilityCodesInput) return;
      const code = String(button.getAttribute("data-facility-code") || "").trim();
      if (!code) return;
      const values = facilityCodeValues();
      if (!values.includes(code)) {
        values.push(code);
        facilityCodesInput.value = values.join("\n");
      }
      updateFacilityPickerState();
    });
  }

  const ledgerFacilityInput = document.getElementById("ledger-facility-filter-input");
  const ledgerFacilitySummary = document.getElementById("ledger-facility-codes-summary");
  const ledgerFacilityForm = ledgerFacilityInput ? ledgerFacilityInput.closest("form") : null;
  const ledgerFacilityValues = () => {
    if (!ledgerFacilityInput) return [];
    return ledgerFacilityInput.value
      .split(/[\s,，、]+/)
      .map((value) => value.trim())
      .filter(Boolean);
  };
  const updateLedgerFacilityPickerState = () => {
    const values = new Set(ledgerFacilityValues());
    if (ledgerFacilitySummary) {
      ledgerFacilitySummary.textContent = values.size ? `${values.size}施設を指定中` : "未指定: 全施設";
    }
    for (const button of document.querySelectorAll("[data-ledger-facility-select]")) {
      const code = String(button.getAttribute("data-facility-code") || "").trim();
      const added = code && values.has(code);
      button.textContent = added ? "解除" : "追加";
      button.classList.toggle("is-active", Boolean(added));
      button.disabled = false;
    }
    for (const row of document.querySelectorAll("[data-ledger-facility-row]")) {
      const code = String(row.getAttribute("data-facility-code") || "").trim();
      row.classList.toggle("is-selected", Boolean(code && values.has(code)));
    }
  };
  const selectLedgerFacility = (element) => {
    if (!ledgerFacilityInput || !(element instanceof Element)) return;
    const code = String(element.getAttribute("data-facility-code") || "").trim();
    if (!code) return;
    const values = ledgerFacilityValues();
    if (values.includes(code)) {
      ledgerFacilityInput.value = values.filter((value) => value !== code).join(", ");
    } else {
      values.push(code);
      ledgerFacilityInput.value = values.join(", ");
    }
    ledgerFacilityInput.dispatchEvent(new Event("input", { bubbles: true }));
    updateLedgerFacilityPickerState();
  };
  if (ledgerFacilityInput) {
    ledgerFacilityInput.addEventListener("input", updateLedgerFacilityPickerState);
    updateLedgerFacilityPickerState();
  }
  for (const button of document.querySelectorAll("[data-ledger-facility-select]")) {
    button.addEventListener("click", (event) => {
      event.stopPropagation();
      selectLedgerFacility(button);
    });
  }
  for (const row of document.querySelectorAll("[data-ledger-facility-row]")) {
    row.addEventListener("click", () => selectLedgerFacility(row));
  }
  for (const button of document.querySelectorAll("[data-ledger-facility-clear]")) {
    button.addEventListener("click", () => {
      if (!ledgerFacilityInput) return;
      ledgerFacilityInput.value = "";
      ledgerFacilityInput.dispatchEvent(new Event("input", { bubbles: true }));
      updateLedgerFacilityPickerState();
      closeModal(button.closest(".edit-modal"));
      if (ledgerFacilityForm) {
        if (typeof ledgerFacilityForm.requestSubmit === "function") {
          ledgerFacilityForm.requestSubmit();
        } else {
          ledgerFacilityForm.submit();
        }
      }
    });
  }

  const caseFacilityInput = document.getElementById("case-facility-filter-input");
  const caseFacilitySummary = document.getElementById("case-facility-codes-summary");
  const caseFacilityForm = caseFacilityInput ? caseFacilityInput.closest("form") : null;
  const caseFacilityValues = () => {
    if (!caseFacilityInput) return [];
    return caseFacilityInput.value
      .split(/[\s,，、]+/)
      .map((value) => value.trim())
      .filter(Boolean);
  };
  const updateCaseFacilityPickerState = () => {
    const values = new Set(caseFacilityValues());
    if (caseFacilitySummary) {
      caseFacilitySummary.textContent = values.size ? `${values.size}施設を指定中` : "未指定: 全施設";
    }
    for (const button of document.querySelectorAll("[data-case-facility-select]")) {
      const code = String(button.getAttribute("data-facility-code") || "").trim();
      const added = code && values.has(code);
      button.textContent = added ? "解除" : "追加";
      button.classList.toggle("is-active", Boolean(added));
      button.disabled = false;
    }
    for (const row of document.querySelectorAll("[data-case-facility-row]")) {
      const code = String(row.getAttribute("data-facility-code") || "").trim();
      row.classList.toggle("is-selected", Boolean(code && values.has(code)));
    }
  };
  const selectCaseFacility = (element) => {
    if (!caseFacilityInput || !(element instanceof Element)) return;
    const code = String(element.getAttribute("data-facility-code") || "").trim();
    if (!code) return;
    const values = caseFacilityValues();
    if (values.includes(code)) {
      caseFacilityInput.value = values.filter((value) => value !== code).join(", ");
    } else {
      values.push(code);
      caseFacilityInput.value = values.join(", ");
    }
    caseFacilityInput.dispatchEvent(new Event("input", { bubbles: true }));
    updateCaseFacilityPickerState();
  };
  if (caseFacilityInput) {
    caseFacilityInput.addEventListener("input", updateCaseFacilityPickerState);
    updateCaseFacilityPickerState();
  }
  for (const button of document.querySelectorAll("[data-case-facility-select]")) {
    button.addEventListener("click", (event) => {
      event.stopPropagation();
      selectCaseFacility(button);
    });
  }
  for (const row of document.querySelectorAll("[data-case-facility-row]")) {
    row.addEventListener("click", () => selectCaseFacility(row));
  }
  for (const button of document.querySelectorAll("[data-case-facility-clear]")) {
    button.addEventListener("click", () => {
      if (!caseFacilityInput) return;
      caseFacilityInput.value = "";
      caseFacilityInput.dispatchEvent(new Event("input", { bubbles: true }));
      updateCaseFacilityPickerState();
      closeModal(button.closest(".edit-modal"));
      if (caseFacilityForm) {
        if (typeof caseFacilityForm.requestSubmit === "function") {
          caseFacilityForm.requestSubmit();
        } else {
          caseFacilityForm.submit();
        }
      }
    });
  }

  const closeMonthPickers = (except = null) => {
    for (const popover of document.querySelectorAll("[data-month-picker-popover]")) {
      if (except && popover === except) continue;
      popover.hidden = true;
    }
  };
  for (const picker of document.querySelectorAll("[data-month-picker]")) {
    const input = picker.querySelector("[data-month-picker-input]");
    const popover = picker.querySelector("[data-month-picker-popover]");
    const options = Array.from(picker.querySelectorAll("[data-month-picker-option]"));
    const clearButton = picker.querySelector("[data-month-picker-clear]");
    if (!input || !popover) continue;

    const selectedMonths = () => options.filter((option) => option.checked).map((option) => option.value);
    const updateInput = () => {
      input.value = selectedMonths().join(", ");
      input.dispatchEvent(new Event("input", { bubbles: true }));
    };
    const openPopover = () => {
      closeMonthPickers(popover);
      popover.hidden = false;
    };

    input.addEventListener("focus", openPopover);
    input.addEventListener("click", openPopover);
    popover.addEventListener("mousedown", (event) => event.stopPropagation());
    popover.addEventListener("click", (event) => event.stopPropagation());
    for (const option of options) {
      option.addEventListener("change", updateInput);
    }
    if (clearButton) {
      clearButton.addEventListener("click", () => {
        for (const option of options) option.checked = false;
        updateInput();
      });
    }
  }

  document.addEventListener("keydown", (event) => {
    if (event.key !== "Escape") return;
    closeMonthPickers();
    closeHelpPopovers();
    closeModal(document.querySelector(".edit-modal:not([hidden])"));
  });

  document.addEventListener("mousedown", (event) => {
    const target = event.target;
    if (target instanceof Element && target.closest("[data-month-picker]")) return;
    closeMonthPickers();
  });

  for (const zone of document.querySelectorAll("[data-file-drop-zone]")) {
    const input = zone.querySelector("[data-file-drop-input]");
    const nameNode = zone.querySelector("[data-file-drop-name]");
    if (!input) continue;

    const setFileName = () => {
      const file = input.files && input.files[0];
      if (nameNode) {
        nameNode.textContent = file ? file.name : "またはクリックしてファイルを選択";
      }
      zone.classList.toggle("has-file", Boolean(file));
    };

    input.addEventListener("change", setFileName);

    for (const eventName of ["dragenter", "dragover"]) {
      zone.addEventListener(eventName, (event) => {
        event.preventDefault();
        zone.classList.add("is-dragover");
      });
    }

    for (const eventName of ["dragleave", "drop"]) {
      zone.addEventListener(eventName, (event) => {
        event.preventDefault();
        zone.classList.remove("is-dragover");
      });
    }

    zone.addEventListener("drop", (event) => {
      const files = event.dataTransfer && event.dataTransfer.files;
      if (!files || !files.length) return;
      const transfer = new DataTransfer();
      transfer.items.add(files[0]);
      input.files = transfer.files;
      setFileName();
    });
  }

  for (const form of document.querySelectorAll("[data-processing-form]")) {
    form.addEventListener("submit", () => {
      const button = form.querySelector("button[type='submit']");
      const message = form.getAttribute("data-processing-message") || "処理しています";
      showProcessingOverlay(message, "ファイル数や内容によって時間がかかることがあります。");
      if (button) {
        button.disabled = true;
        button.textContent = "処理中";
      }
      form.classList.add("is-processing");
    });
  }

  for (const filter of document.querySelectorAll("[data-xml-finding-controls]")) {
    const targetSelector = filter.getAttribute("data-xml-finding-controls");
    const target = targetSelector ? document.querySelector(targetSelector) : null;
    if (!target) continue;

    const buttons = Array.from(filter.querySelectorAll("[data-xml-filter-field][data-xml-filter-value]"));
    const accordionActionButtons = Array.from(filter.querySelectorAll("[data-xml-accordion-action]"));
    const displayNameInput = filter.querySelector("[data-xml-display-name-filter]");
    const groups = Array.from(target.querySelectorAll("[data-xml-finding-group]"));

    const setGroupExpanded = (group, expanded) => {
      const toggle = group.querySelector("[data-xml-accordion-toggle]");
      const items = group.querySelector(".xml-error-items");
      if (!toggle || !items) return;
      toggle.setAttribute("aria-expanded", expanded ? "true" : "false");
      items.hidden = !expanded;
    };

    const applyFindingFilter = () => {
      const activeByField = new Map();
      for (const button of buttons) {
        const field = button.dataset.xmlFilterField;
        const value = button.dataset.xmlFilterValue;
        if (!field || value == null || !button.classList.contains("is-active")) continue;
        const values = activeByField.get(field) || new Set();
        values.add(value);
        activeByField.set(field, values);
      }
      const displayNameKeyword = normalize(displayNameInput ? displayNameInput.value : "");

      for (const group of groups) {
        let visibleCount = 0;
        for (const item of group.querySelectorAll("[data-xml-severity][data-xml-fixability]")) {
          let matched = true;
          for (const [field, values] of activeByField.entries()) {
            if (!values.has(item.getAttribute(`data-xml-${field}`))) {
              matched = false;
              break;
            }
          }
          if (matched && displayNameKeyword) {
            matched = normalize(item.getAttribute("data-xml-display-name")).includes(displayNameKeyword);
          }
          item.hidden = !matched;
          if (matched) visibleCount += 1;
        }
        group.hidden = visibleCount === 0;
      }
    };

    for (const group of groups) {
      const toggle = group.querySelector("[data-xml-accordion-toggle]");
      if (!toggle) continue;
      setGroupExpanded(group, false);
      toggle.addEventListener("click", () => {
        setGroupExpanded(group, toggle.getAttribute("aria-expanded") !== "true");
      });
    }

    for (const button of buttons) {
      button.addEventListener("click", () => {
        button.classList.toggle("is-active");
        button.setAttribute("aria-pressed", button.classList.contains("is-active") ? "true" : "false");
        applyFindingFilter();
      });
    }

    for (const button of accordionActionButtons) {
      button.addEventListener("click", () => {
        const shouldExpand = button.dataset.xmlAccordionAction === "expand";
        for (const group of groups) {
          if (!group.hidden) {
            setGroupExpanded(group, shouldExpand);
          }
        }
      });
    }

    if (displayNameInput) {
      displayNameInput.addEventListener("input", applyFindingFilter);
    }

    applyFindingFilter();
  }

  const personSelectionList = document.querySelector("[data-person-selection-list]");
  if (personSelectionList) {
    const emptyRow = personSelectionList.querySelector("[data-person-selection-empty]");
    const choiceSelector = "[data-person-selection-choice]";

    const setEmptyVisible = () => {
      if (!emptyRow) return;
      emptyRow.hidden = Boolean(personSelectionList.querySelector("[data-person-selection-item]"));
    };

    const addChoice = (choice) => {
      const subscriberId = choice.dataset.subscriberId || "";
      if (!subscriberId || personSelectionList.querySelector(`[data-person-selection-item="${subscriberId}"]`)) {
        return;
      }
      const row = document.createElement("tr");
      row.setAttribute("data-person-selection-item", subscriberId);
      const eventId = document.querySelector("select[name='event_id']")?.value || "2";
      const caseListUrl = `/exam-export-cases?event_id=${encodeURIComponent(eventId)}&subscriber_id=${encodeURIComponent(subscriberId)}&limit=500`;
      row.innerHTML = `
        <td><strong>${escapeHtml(subscriberId)}</strong><small>HIA ${escapeHtml(choice.dataset.hiaSubscriberId || "-")}</small></td>
        <td><strong>${escapeHtml(choice.dataset.personName || "-")}</strong><small>${escapeHtml(choice.dataset.gender || "-")}</small></td>
        <td><strong>${escapeHtml(choice.dataset.insurance || "-")}</strong><small>${escapeHtml(choice.dataset.birthdate || "-")}</small></td>
        <td><strong>${escapeHtml(choice.dataset.latestCaseId || "-")}</strong><small>${escapeHtml(choice.dataset.caseCount || "0")}件</small><a class="small-inline-link" href="${caseListUrl}">case一覧</a></td>
        <td><button type="button" class="small-button" data-person-selection-remove>外す</button></td>
      `;
      personSelectionList.appendChild(row);
      choice.disabled = true;
      choice.checked = true;
      choice.closest(".selectable-candidate")?.classList.add("is-added");
      setEmptyVisible();
    };

    document.querySelector("[data-person-selection-add-all]")?.addEventListener("click", () => {
      for (const choice of document.querySelectorAll(choiceSelector)) {
        addChoice(choice);
      }
    });

    document.querySelector("[data-person-selection-add-selected]")?.addEventListener("click", () => {
      for (const choice of document.querySelectorAll(`${choiceSelector}:checked`)) {
        addChoice(choice);
      }
    });

    personSelectionList.addEventListener("click", (event) => {
      const button = event.target.closest("[data-person-selection-remove]");
      if (!button) return;
      const row = button.closest("[data-person-selection-item]");
      const subscriberId = row?.getAttribute("data-person-selection-item");
      if (subscriberId) {
        const choice = document.querySelector(`${choiceSelector}[data-subscriber-id="${subscriberId}"]`);
        if (choice) {
          choice.disabled = false;
          choice.checked = false;
          choice.closest(".selectable-candidate")?.classList.remove("is-added");
        }
      }
      row?.remove();
      setEmptyVisible();
    });

    document.querySelector("[data-person-selection-clear]")?.addEventListener("click", () => {
      for (const row of personSelectionList.querySelectorAll("[data-person-selection-item]")) {
        row.remove();
      }
      for (const choice of document.querySelectorAll(choiceSelector)) {
        choice.disabled = false;
        choice.checked = false;
        choice.closest(".selectable-candidate")?.classList.remove("is-added");
      }
      setEmptyVisible();
    });

    document.querySelector("[data-person-selection-copy]")?.addEventListener("click", async () => {
      const subscriberIds = Array.from(personSelectionList.querySelectorAll("[data-person-selection-item]"))
        .map((row) => row.getAttribute("data-person-selection-item"))
        .filter(Boolean);
      if (!subscriberIds.length) return;
      try {
        await navigator.clipboard.writeText(subscriberIds.join("\n"));
      } catch (_error) {
        window.prompt("subscriber_id一覧", subscriberIds.join("\n"));
      }
    });

    setEmptyVisible();
  }

  const manualFacilityInput = document.querySelector("#manual-entry-facility-input");
  if (manualFacilityInput) {
    const setManualFacility = (code) => {
      manualFacilityInput.value = code || "";
      const caseFacilityInput = document.querySelector("#manual-entry-case-search-facility");
      if (caseFacilityInput) caseFacilityInput.value = code || "";
      document.querySelector("#manual-entry-facility-picker-modal [data-modal-close]")?.click();
    };

    for (const button of document.querySelectorAll("[data-manual-entry-facility-select]")) {
      button.addEventListener("click", (event) => {
        event.stopPropagation();
        setManualFacility(button.dataset.facilityCode || "");
      });
    }

    for (const row of document.querySelectorAll("[data-manual-entry-facility-row]")) {
      row.addEventListener("click", () => {
        setManualFacility(row.dataset.facilityCode || "");
      });
    }
  }

  const manualSubscriberResults = document.querySelector("[data-manual-entry-subscriber-results]");
  if (manualSubscriberResults) {
    const searchButton = document.querySelector("[data-manual-entry-subscriber-search]");
    const applyButton = document.querySelector("[data-manual-entry-subscriber-apply]");
    const caseSearchButton = document.querySelector("[data-manual-entry-case-search]");
    const casePickerResults = document.querySelector("[data-manual-entry-case-picker-results]");
    const selectedCasePanel = document.querySelector("[data-manual-entry-selected-case]");
    const selectedCaseTitle = document.querySelector("[data-manual-entry-selected-case-title]");
    const selectedCaseDetail = document.querySelector("[data-manual-entry-selected-case-detail]");
    const casePanel = document.querySelector("[data-manual-entry-case-panel]");
    const caseResults = document.querySelector("[data-manual-entry-case-results]");
    const caseCount = document.querySelector("[data-manual-entry-case-count]");
    const searchQ = document.querySelector("#manual-entry-subscriber-search-q");
    const searchKana = document.querySelector("#manual-entry-subscriber-search-kana");
    const searchSymbol = document.querySelector("#manual-entry-subscriber-search-symbol");
    const searchNumber = document.querySelector("#manual-entry-subscriber-search-number");
    const caseSearchFacility = document.querySelector("#manual-entry-case-search-facility");
    const caseSearchKana = document.querySelector("#manual-entry-case-search-kana");
    const caseSearchHia = document.querySelector("#manual-entry-case-search-hia");
    const caseSearchSymbol = document.querySelector("#manual-entry-case-search-symbol");
    const caseSearchNumber = document.querySelector("#manual-entry-case-search-number");
    let selectedSubscriber = null;

    const setValue = (selector, value) => {
      const input = document.querySelector(selector);
      if (input) input.value = value || "";
    };

    const setManualSubscriberMessage = (message) => {
      manualSubscriberResults.innerHTML = `<tr><td colspan="4">${escapeHtml(message)}</td></tr>`;
    };

    const setManualCasePickerMessage = (message) => {
      if (!casePickerResults) return;
      casePickerResults.innerHTML = `<tr><td colspan="5">${escapeHtml(message)}</td></tr>`;
    };

    const setManualSubscriberSelected = (subscriber) => {
      selectedSubscriber = subscriber;
      for (const row of manualSubscriberResults.querySelectorAll("[data-manual-entry-subscriber-row]")) {
        const isSelected = subscriber && row.dataset.subscriberId === String(subscriber.subscriber_id || "");
        row.classList.toggle("is-selected", Boolean(isSelected));
        const button = row.querySelector("[data-manual-entry-subscriber-pick]");
        if (button) {
          button.textContent = isSelected ? "選択中" : "選ぶ";
          button.classList.toggle("is-active", Boolean(isSelected));
        }
      }
      if (applyButton) {
        applyButton.disabled = !selectedSubscriber;
        applyButton.classList.toggle("disabled", !selectedSubscriber);
      }
    };

    const fillManualEntryFromPerson = (person) => {
      setValue("#manual-entry-subscriber-id-input", person.subscriber_id);
      setValue("#manual-entry-hia-subscriber-id-input", person.hia_subscriber_id);
      setValue("#manual-entry-name-full-input", person.name_full);
      setValue("#manual-entry-name-kana-input", person.name_kana);
      setValue("#manual-entry-insurance-symbol-input", person.insurance_symbol);
      setValue("#manual-entry-insurance-number-input", person.insurance_number);
      setValue("#manual-entry-insurance-branch-input", person.insurance_branch_number);
      setValue("#manual-entry-birthdate-input", person.birth || person.birthdate);
      setValue("#manual-entry-gender-input", person.gender_label);
    };

    const fillManualEntryFromCase = (item) => {
      fillManualEntryFromPerson(item);
      setValue("#manual-entry-facility-input", item.facility_code);
      setValue("#manual-entry-case-search-facility", item.facility_code);
      setValue("#manual-entry-exam-date-input", item.exam_date);
      const purpose = document.querySelector("select[name='entry_purpose']");
      if (purpose) purpose.value = "SUPPLEMENT";
      if (selectedCasePanel) selectedCasePanel.hidden = false;
      if (selectedCaseTitle) {
        selectedCaseTitle.textContent = `case ${item.exam_export_case_id || "-"} / ${item.name_kana || "-"}`;
      }
      if (selectedCaseDetail) {
        selectedCaseDetail.textContent = `${item.facility_name || "-"} / ${item.exam_date || "-"} / ${item.source_mode || "-"} / 出力 ${item.export_readiness_status || "-"}`;
      }
      renderManualCaseRows([item]);
      document.querySelector("#manual-entry-case-picker-modal [data-modal-close]")?.click();
      document.querySelector("#manual-entry-basic")?.scrollIntoView({ behavior: "smooth", block: "start" });
    };

    const setManualCaseMessage = (message, label = "未検索") => {
      if (casePanel) casePanel.hidden = false;
      if (caseResults) {
        caseResults.innerHTML = `<tr><td colspan="5">${escapeHtml(message)}</td></tr>`;
      }
      if (caseCount) {
        caseCount.textContent = label;
        caseCount.className = "status-pill status-muted";
      }
    };

    const renderManualCaseRows = (items) => {
      if (!casePanel || !caseResults) return;
      casePanel.hidden = false;
      if (caseCount) {
        caseCount.textContent = `${items.length}件`;
        caseCount.className = `status-pill ${items.length ? "status-ready" : "status-muted"}`;
      }
      if (!items.length) {
        caseResults.innerHTML = `<tr><td colspan="5">この加入者の既存caseはありません。紙のみ新規sourceとして作成する想定です。</td></tr>`;
        return;
      }
      caseResults.innerHTML = "";
      for (const item of items) {
        const sourceText = `XML ${item.xml_count || 0} / CSV ${item.csv_count || 0} / 紙 ${item.paper_count || 0}`;
        const legal = `${item.legal_check_result || "PENDING"}${item.legal_reason_summary ? ` / ${item.legal_reason_summary}` : ""}`;
        const specific = `${item.specific_check_result || "PENDING"}${item.specific_reason_summary ? ` / ${item.specific_reason_summary}` : ""}`;
        const row = document.createElement("tr");
        row.innerHTML = `
          <td>
            <strong>${escapeHtml(item.exam_export_case_id || "-")}</strong>
            <small>${escapeHtml(item.source_mode || "-")} / 値 ${escapeHtml(item.case_value_count || "0")}</small>
          </td>
          <td>
            <strong title="${escapeHtml(item.facility_name || "")}">${escapeHtml(item.facility_name || "-")}</strong>
            <small>${escapeHtml(item.exam_date || "-")} / ${escapeHtml(item.facility_code || "-")}</small>
          </td>
          <td>
            <strong>${escapeHtml(sourceText)}</strong>
            <small>構成source ${escapeHtml(item.source_count || "0")}</small>
          </td>
          <td>
            <strong>法定 ${escapeHtml(legal)}</strong>
            <small>特定 ${escapeHtml(specific)} / 出力 ${escapeHtml(item.export_readiness_status || "-")}</small>
          </td>
          <td>
            <a class="ghost-button compact-action-button" href="/exam-export-cases/${encodeURIComponent(item.exam_export_case_id)}">詳細</a>
          </td>
        `;
        caseResults.appendChild(row);
      }
    };

    const renderManualCasePickerRows = (items) => {
      if (!casePickerResults) return;
      if (!items.length) {
        setManualCasePickerMessage("一致するcaseはありません。");
        return;
      }
      casePickerResults.innerHTML = "";
      for (const item of items) {
        const sourceText = `XML ${item.xml_count || 0} / CSV ${item.csv_count || 0} / 紙 ${item.paper_count || 0}`;
        const legal = `${item.legal_check_result || "PENDING"}${item.legal_reason_summary ? ` / ${item.legal_reason_summary}` : ""}`;
        const specific = `${item.specific_check_result || "PENDING"}${item.specific_reason_summary ? ` / ${item.specific_reason_summary}` : ""}`;
        const row = document.createElement("tr");
        row.className = "clickable-table-row";
        row.innerHTML = `
          <td>
            <strong>case ${escapeHtml(item.exam_export_case_id || "-")} / ${escapeHtml(item.name_kana || "-")}</strong>
            <small>${escapeHtml(item.name_full || "-")} / HIA ${escapeHtml(item.hia_subscriber_id || "-")} / ${escapeHtml(item.birthdate || "-")} / ${escapeHtml(item.gender_label || "-")}</small>
          </td>
          <td>
            <strong title="${escapeHtml(item.facility_name || "")}">${escapeHtml(item.facility_name || "-")}</strong>
            <small>${escapeHtml(item.exam_date || "-")} / ${escapeHtml(item.facility_code || "-")} / ${escapeHtml(item.expected_source_mode_label || "-")}</small>
          </td>
          <td>
            <strong>${escapeHtml(sourceText)}</strong>
            <small>${escapeHtml(item.source_mode || "-")} / 値 ${escapeHtml(item.case_value_count || "0")}</small>
          </td>
          <td>
            <strong>法定 ${escapeHtml(legal)}</strong>
            <small>特定 ${escapeHtml(specific)} / 出力 ${escapeHtml(item.export_readiness_status || "-")}</small>
          </td>
          <td><button type="button" class="ghost-button compact-action-button" data-manual-entry-case-pick>選択</button></td>
        `;
        row.addEventListener("click", () => fillManualEntryFromCase(item));
        row.querySelector("[data-manual-entry-case-pick]")?.addEventListener("click", (event) => {
          event.stopPropagation();
          fillManualEntryFromCase(item);
        });
        casePickerResults.appendChild(row);
      }
    };

    const loadManualCasesForSubscriber = async (subscriberId) => {
      if (!subscriberId) {
        setManualCaseMessage("加入者を選択してください。", "未検索");
        return;
      }
      const eventId = document.querySelector("select[name='event_id']")?.value || "2";
      const params = new URLSearchParams({
        event_id: eventId,
        subscriber_id: String(subscriberId),
      });
      setManualCaseMessage("caseを確認中...", "検索中");
      try {
        const response = await fetch(`/api/manual-exam-entry/cases?${params.toString()}`, {
          headers: { Accept: "application/json" },
        });
        if (!response.ok) {
          throw new Error(`HTTP ${response.status}`);
        }
        const payload = await response.json();
        renderManualCaseRows(Array.isArray(payload.items) ? payload.items : []);
      } catch (_error) {
        setManualCaseMessage("case確認でエラーが発生しました。", "エラー");
      }
    };

    const renderManualSubscriberRows = (items) => {
      if (!items.length) {
        setManualSubscriberMessage("一致する加入者はいません。");
        setManualSubscriberSelected(null);
        return;
      }
      manualSubscriberResults.innerHTML = "";
      for (const item of items) {
        const row = document.createElement("tr");
        row.setAttribute("data-manual-entry-subscriber-row", "true");
        row.dataset.subscriberId = String(item.subscriber_id || "");
        const insurance = `${item.insurance_symbol || "-"}-${item.insurance_number || "-"} 枝番 ${item.insurance_branch_number || "-"}`;
        const hia = `HIA ${item.hia_subscriber_id || "-"} / subscriber ${item.subscriber_id || "-"}`;
        const dashboard = item.hia_dashboard_status
          ? `${item.hia_dashboard_status} / ${item.hia_dashboard_medical_institution || "-"}`
          : "HIAダッシュボードなし";
        row.innerHTML = `
          <td>
            <strong>${escapeHtml(item.name_kana || "-")}</strong>
            <small>${escapeHtml(item.name_full || "-")} / ${escapeHtml(item.birth || "-")} / ${escapeHtml(item.gender_label || "-")}</small>
          </td>
          <td>
            <strong>${escapeHtml(insurance)}</strong>
            <small>社員 ${escapeHtml(item.employee_code || "-")} / 資格喪失 ${escapeHtml(item.qualification_lost_date || "-")}</small>
          </td>
          <td>
            <strong>${escapeHtml(hia)}</strong>
            <small>case ${escapeHtml(item.candidate_case_count || "0")}件 / ${escapeHtml(dashboard)}</small>
          </td>
          <td><button type="button" class="ghost-button" data-manual-entry-subscriber-pick>選ぶ</button></td>
        `;
        row.addEventListener("click", () => {
          if (selectedSubscriber && String(selectedSubscriber.subscriber_id || "") === String(item.subscriber_id || "")) {
            setManualSubscriberSelected(null);
          } else {
            setManualSubscriberSelected(item);
          }
        });
        row.querySelector("[data-manual-entry-subscriber-pick]")?.addEventListener("click", (event) => {
          event.stopPropagation();
          if (selectedSubscriber && String(selectedSubscriber.subscriber_id || "") === String(item.subscriber_id || "")) {
            setManualSubscriberSelected(null);
          } else {
            setManualSubscriberSelected(item);
          }
        });
        manualSubscriberResults.appendChild(row);
      }
      setManualSubscriberSelected(null);
    };

    const searchManualSubscribers = async () => {
      const eventId = document.querySelector("select[name='event_id']")?.value || "2";
      const params = new URLSearchParams({
        event_id: eventId,
        q: searchQ ? searchQ.value.trim() : "",
        name_kana: searchKana ? searchKana.value.trim() : "",
        insurance_symbol: searchSymbol ? searchSymbol.value.trim() : "",
        insurance_number: searchNumber ? searchNumber.value.trim() : "",
      });
      if (![...params.values()].some((value) => value && value !== eventId)) {
        setManualSubscriberMessage("検索条件を入力してください。");
        setManualSubscriberSelected(null);
        return;
      }
      setManualSubscriberMessage("検索中...");
      try {
        const response = await fetch(`/api/manual-exam-entry/subscribers?${params.toString()}`, {
          headers: { Accept: "application/json" },
        });
        if (!response.ok) {
          throw new Error(`HTTP ${response.status}`);
        }
        const payload = await response.json();
        renderManualSubscriberRows(Array.isArray(payload.items) ? payload.items : []);
      } catch (_error) {
        setManualSubscriberMessage("検索でエラーが発生しました。");
        setManualSubscriberSelected(null);
      }
    };

    const searchManualCases = async () => {
      if (!casePickerResults) return;
      const eventId = document.querySelector("select[name='event_id']")?.value || "2";
      const params = new URLSearchParams({
        event_id: eventId,
        facility_q: caseSearchFacility ? caseSearchFacility.value.trim() : "",
        name_kana: caseSearchKana ? caseSearchKana.value.trim() : "",
        hia_subscriber_id: caseSearchHia ? caseSearchHia.value.trim() : "",
        insurance_symbol: caseSearchSymbol ? caseSearchSymbol.value.trim() : "",
        insurance_number: caseSearchNumber ? caseSearchNumber.value.trim() : "",
      });
      if (![...params.values()].some((value) => value && value !== eventId)) {
        setManualCasePickerMessage("検索条件を入力してください。");
        return;
      }
      setManualCasePickerMessage("検索中...");
      try {
        const response = await fetch(`/api/manual-exam-entry/case-candidates?${params.toString()}`, {
          headers: { Accept: "application/json" },
        });
        if (!response.ok) {
          throw new Error(`HTTP ${response.status}`);
        }
        const payload = await response.json();
        renderManualCasePickerRows(Array.isArray(payload.items) ? payload.items : []);
      } catch (_error) {
        setManualCasePickerMessage("case検索でエラーが発生しました。");
      }
    };

    searchButton?.addEventListener("click", searchManualSubscribers);
    caseSearchButton?.addEventListener("click", searchManualCases);
    for (const input of [searchQ, searchKana, searchSymbol, searchNumber]) {
      input?.addEventListener("keydown", (event) => {
        if (event.key === "Enter") {
          event.preventDefault();
          searchManualSubscribers();
        }
      });
    }
    for (const input of [caseSearchFacility, caseSearchKana, caseSearchHia, caseSearchSymbol, caseSearchNumber]) {
      input?.addEventListener("keydown", (event) => {
        if (event.key === "Enter") {
          event.preventDefault();
          searchManualCases();
        }
      });
    }
    applyButton?.addEventListener("click", () => {
      if (!selectedSubscriber) return;
      fillManualEntryFromPerson(selectedSubscriber);
      document.querySelector("#manual-entry-subscriber-picker-modal [data-modal-close]")?.click();
      loadManualCasesForSubscriber(selectedSubscriber.subscriber_id);
    });
  }

  const manualValueInputs = Array.from(document.querySelectorAll("[data-manual-method-group]"));
  if (manualValueInputs.length) {
    const refreshManualMethodGroup = (groupKey) => {
      if (!groupKey) return;
      const groupInputs = manualValueInputs.filter((input) => input.dataset.manualMethodGroup === groupKey);
      const activeInput = groupInputs.find((input) => input.value.trim() !== "");
      for (const input of groupInputs) {
        const row = input.closest("tr");
        const include = row ? row.querySelector("input[type='checkbox'][name^='include_']") : null;
        const hasValue = input.value.trim() !== "";
        if (include) {
          include.checked = hasValue;
          include.disabled = !hasValue;
        }
        if (activeInput && input !== activeInput) {
          input.disabled = true;
          row?.classList.add("is-method-disabled");
        } else {
          input.disabled = false;
          row?.classList.remove("is-method-disabled");
        }
      }
    };

    for (const input of manualValueInputs) {
      input.addEventListener("input", () => refreshManualMethodGroup(input.dataset.manualMethodGroup || ""));
      refreshManualMethodGroup(input.dataset.manualMethodGroup || "");
    }

    for (const select of document.querySelectorAll("[data-manual-code-select]")) {
      select.addEventListener("change", () => {
        const input = select.closest(".manual-entry-value-control")?.querySelector(".manual-entry-value-input");
        if (!input || !select.value) return;
        input.value = select.value;
        input.dispatchEvent(new Event("input", { bubbles: true }));
      });
    }
  }

  const manualEntryItemList = document.querySelector("#manual-entry-item-list");
  const manualEntryItemSearchInput = document.querySelector("#manual-entry-item-search-input");
  if (manualEntryItemList) {
    const manualEntryFloatingNav = document.querySelector(".manual-entry-floating-nav");
    const manualEntryFloatingCategoryToggle = document.querySelector("[data-manual-entry-floating-category-toggle]");
    const manualEntryFloatingItemSearchInput = document.querySelector("#manual-entry-floating-item-search-input");
    const manualEntryFloatingItemResults = document.querySelector("[data-manual-entry-floating-item-results]");
    const categories = Array.from(manualEntryItemList.querySelectorAll(".manual-entry-category"));
    const itemRows = Array.from(manualEntryItemList.querySelectorAll("tbody tr[data-filter-text]"));
    const openCategoryForRow = (row) => {
      const category = row.closest(".manual-entry-category");
      if (category) category.open = true;
    };
    const jumpToManualEntryRow = (row) => {
      if (!row) return;
      openCategoryForRow(row);
      row.scrollIntoView({ behavior: "smooth", block: "center" });
      row.classList.add("is-scroll-target");
      window.setTimeout(() => row.classList.remove("is-scroll-target"), 1600);
    };
    const jumpToManualEntryItem = () => {
      const keyword = normalize(manualEntryItemSearchInput?.value || "");
      const target = itemRows.find((row) => {
        if (keyword) {
          return normalize(row.dataset.filterText).includes(keyword);
        }
        return !row.hidden;
      });
      jumpToManualEntryRow(target);
    };
    const renderFloatingItemResults = () => {
      if (!manualEntryFloatingItemSearchInput || !manualEntryFloatingItemResults) return;
      const keyword = normalize(manualEntryFloatingItemSearchInput.value);
      if (!keyword) {
        manualEntryFloatingItemResults.hidden = true;
        manualEntryFloatingItemResults.innerHTML = "";
        return;
      }
      const matches = itemRows
        .map((row, index) => ({ row, index }))
        .filter(({ row }) => normalize(row.dataset.filterText).includes(keyword))
        .slice(0, 10);
      manualEntryFloatingItemResults.hidden = false;
      if (!matches.length) {
        manualEntryFloatingItemResults.innerHTML = `<p class="manual-entry-floating-item-empty">該当項目はありません。</p>`;
        return;
      }
      manualEntryFloatingItemResults.innerHTML = matches.map(({ row, index }) => {
        const itemName = row.dataset.itemName || "-";
        const namecode = row.dataset.namecode || "-";
        const categoryName = row.dataset.categoryName || "-";
        const identityName = row.dataset.identityItemName || "";
        const itemLabel = identityName && identityName !== itemName ? `${itemName} / ${identityName}` : itemName;
        return `
          <div class="manual-entry-floating-item-result" role="button" tabindex="0" data-manual-entry-floating-item-jump="${index}">
            <div>
              <strong>${escapeHtml(itemLabel)}</strong>
              <small>${escapeHtml(namecode)} / ${escapeHtml(categoryName)}</small>
            </div>
            <span class="manual-entry-floating-item-jump-label">項目に飛ぶ</span>
          </div>
        `;
      }).join("");
    };

    if (manualEntryFloatingNav && manualEntryFloatingCategoryToggle) {
      const setCategoryCollapsed = (collapsed) => {
        manualEntryFloatingNav.classList.toggle("is-category-collapsed", collapsed);
        manualEntryFloatingCategoryToggle.setAttribute("aria-expanded", collapsed ? "false" : "true");
        manualEntryFloatingCategoryToggle.setAttribute("aria-label", collapsed ? "カテゴリ一覧を開く" : "カテゴリ一覧を閉じる");
      };
      setCategoryCollapsed(true);
      manualEntryFloatingCategoryToggle.addEventListener("click", () => {
        setCategoryCollapsed(!manualEntryFloatingNav.classList.contains("is-category-collapsed"));
      });
      manualEntryFloatingCategoryToggle.addEventListener("keydown", (event) => {
        if (event.key !== "Enter" && event.key !== " ") return;
        event.preventDefault();
        setCategoryCollapsed(!manualEntryFloatingNav.classList.contains("is-category-collapsed"));
      });
    }

    document.querySelector("[data-manual-entry-categories-open]")?.addEventListener("click", () => {
      for (const category of categories) {
        category.open = true;
      }
    });
    document.querySelector("[data-manual-entry-categories-close]")?.addEventListener("click", () => {
      for (const category of categories) {
        category.open = false;
      }
    });
    document.querySelector("[data-manual-entry-item-jump]")?.addEventListener("click", jumpToManualEntryItem);
    manualEntryItemSearchInput?.addEventListener("keydown", (event) => {
      if (event.key === "Enter") {
        event.preventDefault();
        jumpToManualEntryItem();
      }
    });
    manualEntryFloatingItemSearchInput?.addEventListener("input", renderFloatingItemResults);
    manualEntryFloatingItemSearchInput?.addEventListener("keydown", (event) => {
      if (event.key === "Enter") {
        event.preventDefault();
        const firstCard = manualEntryFloatingItemResults?.querySelector("[data-manual-entry-floating-item-jump]");
        firstCard?.click();
      }
    });
    manualEntryFloatingItemResults?.addEventListener("click", (event) => {
      const target = event.target;
      if (!(target instanceof Element)) return;
      const card = target.closest("[data-manual-entry-floating-item-jump]");
      if (!card) return;
      const rowIndex = Number(card.getAttribute("data-manual-entry-floating-item-jump"));
      jumpToManualEntryRow(itemRows[rowIndex]);
    });
    manualEntryFloatingItemResults?.addEventListener("keydown", (event) => {
      if (event.key !== "Enter" && event.key !== " ") return;
      const target = event.target;
      if (!(target instanceof Element)) return;
      const card = target.closest("[data-manual-entry-floating-item-jump]");
      if (!card) return;
      event.preventDefault();
      const rowIndex = Number(card.getAttribute("data-manual-entry-floating-item-jump"));
      jumpToManualEntryRow(itemRows[rowIndex]);
    });
  }
})();
