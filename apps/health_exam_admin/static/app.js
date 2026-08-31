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
      const csrfInput = form.querySelector("input[name='_csrf_token']");
      if (token && csrfInput) {
        csrfInput.value = token;
      } else if (token) {
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
  const fetchJson = async (url) => {
    const response = await fetch(url, {
      headers: { Accept: "application/json" },
    });
    let payload = null;
    let bodyText = "";
    try {
      bodyText = await response.text();
      payload = bodyText ? JSON.parse(bodyText) : null;
    } catch (_error) {
      payload = null;
    }
    if (!response.ok) {
      const detail = payload?.message || payload?.detail || bodyText.slice(0, 160);
      throw new Error(detail ? `HTTP ${response.status}: ${detail}` : `HTTP ${response.status}`);
    }
    return payload || {};
  };
  const postJson = async (url, data) => {
    const token = cookieValue("phr_app_csrf");
    const response = await fetch(url, {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
        ...(token ? { "x-csrf-token": token } : {}),
      },
      body: JSON.stringify(data || {}),
    });
    let payload = null;
    let bodyText = "";
    try {
      bodyText = await response.text();
      payload = bodyText ? JSON.parse(bodyText) : null;
    } catch (_error) {
      payload = null;
    }
    if (!response.ok) {
      const detail = payload?.message || payload?.detail || bodyText.slice(0, 160);
      throw new Error(detail ? `HTTP ${response.status}: ${detail}` : `HTTP ${response.status}`);
    }
    return payload || {};
  };
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

  const copyTextToClipboard = async (text) => {
    if (!text) return false;
    if (window.isSecureContext && navigator.clipboard && typeof navigator.clipboard.writeText === "function") {
      await navigator.clipboard.writeText(text);
      return true;
    }
    const textarea = document.createElement("textarea");
    textarea.value = text;
    textarea.setAttribute("readonly", "readonly");
    textarea.style.position = "fixed";
    textarea.style.top = "-1000px";
    textarea.style.left = "-1000px";
    document.body.appendChild(textarea);
    textarea.focus();
    textarea.select();
    try {
      return document.execCommand("copy");
    } finally {
      textarea.remove();
    }
  };

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
        const copied = await copyTextToClipboard(text);
        button.textContent = copied ? "コピーしました" : "コピー失敗";
      } catch {
        button.textContent = "コピー失敗";
      }
      window.setTimeout(() => {
        button.textContent = originalLabel;
      }, 1600);
    });
  }

  for (const button of document.querySelectorAll("[data-clear-input-ids]")) {
    button.addEventListener("click", () => {
      const ids = String(button.getAttribute("data-clear-input-ids") || "")
        .split(",")
        .map((value) => value.trim())
        .filter(Boolean);
      for (const id of ids) {
        const input = document.getElementById(id);
        if (!input) continue;
        input.value = "";
        input.dispatchEvent(new Event("input", { bubbles: true }));
        input.dispatchEvent(new Event("change", { bubbles: true }));
      }
    });
  }

  for (const input of document.querySelectorAll("[data-live-filter-input]")) {
    const tableSelector = input.getAttribute("data-live-filter-input");
    const table = tableSelector ? document.querySelector(tableSelector) : null;
    if (!table) continue;

    const isTableTarget = table.matches("table");
    const tbody = isTableTarget ? table.querySelector("tbody") : null;
    const rows = Array.from(tbody ? table.querySelectorAll("tbody tr[data-filter-text]") : table.querySelectorAll("[data-filter-text]"));
    const tableFilters = {
      keywordInput: input,
      toggleGroups: new Map(),
      toggleModes: new Map(),
      toggleMatchModes: new Map(),
    };
    filters.set(tableSelector, tableFilters);
    const emptyMessage = table.dataset.emptyMessage || "一致する行はありません。";
    let emptyRow;
    if (tbody) {
      emptyRow = document.createElement("tr");
      const emptyCell = document.createElement("td");
      emptyCell.colSpan = table.querySelectorAll("thead th").length || 1;
      emptyCell.textContent = emptyMessage;
      emptyRow.appendChild(emptyCell);
      tbody.appendChild(emptyRow);
    } else {
      emptyRow = document.createElement("div");
      emptyRow.className = "empty-state";
      emptyRow.textContent = emptyMessage;
      table.appendChild(emptyRow);
    }
    emptyRow.hidden = true;

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

  const updatePersonSelectionInputPanels = () => {
    const selectedMode = document.querySelector(".person-selection-mode input[type='radio']:checked")?.value || "bulk";
    for (const panel of document.querySelectorAll("[data-person-input-panel]")) {
      panel.hidden = panel.getAttribute("data-person-input-panel") !== selectedMode;
    }
  };

  for (const radio of document.querySelectorAll(".person-selection-mode input[type='radio']")) {
    radio.addEventListener("change", () => {
      const group = radio.closest(".person-selection-mode");
      for (const card of group?.querySelectorAll(".choice-card") || []) {
        const input = card.querySelector("input[type='radio']");
        card.classList.toggle("is-selected", Boolean(input?.checked));
      }
      updatePersonSelectionInputPanels();
    });
  }
  updatePersonSelectionInputPanels();

  const updateCheckboxChoiceCard = (card) => {
    const input = card.querySelector("input[type='checkbox']");
    const badge = card.querySelector("b");
    const checked = Boolean(input?.checked);
    card.classList.toggle("is-selected", checked);
    if (badge) badge.textContent = checked ? "ON" : "OFF";
  };

  for (const card of document.querySelectorAll("[data-checkbox-choice-card]")) {
    const input = card.querySelector("input[type='checkbox']");
    updateCheckboxChoiceCard(card);
    input?.addEventListener("change", () => updateCheckboxChoiceCard(card));
  }

  const updateRadioChoiceCards = (name) => {
    if (!name) return;
    for (const input of document.querySelectorAll("input[type='radio']")) {
      if (input.name !== name) continue;
      const card = input.closest("[data-radio-choice-card]");
      if (card) card.classList.toggle("is-selected", input.checked);
    }
  };

  for (const card of document.querySelectorAll("[data-radio-choice-card]")) {
    const input = card.querySelector("input[type='radio']");
    if (!input) continue;
    updateRadioChoiceCards(input.name);
    card.addEventListener("click", () => {
      input.checked = true;
      input.dispatchEvent(new Event("change", { bubbles: true }));
      updateRadioChoiceCards(input.name);
    });
    input.addEventListener("change", () => updateRadioChoiceCards(input.name));
  }

  const caseCsvForm = document.querySelector(".case-csv-download-form");
  if (caseCsvForm) {
    const storageKey = "phr.caseExportCsvPatterns.v1";
    const fieldInputs = Array.from(caseCsvForm.querySelectorAll("input[name='fields']"));
    const patternSelect = caseCsvForm.querySelector("[data-case-csv-pattern-select]");
    const patternNameInput = caseCsvForm.querySelector("[data-case-csv-pattern-name]");
    const message = caseCsvForm.querySelector("[data-case-csv-pattern-message]");
    const defaultFields = fieldInputs.filter((input) => input.defaultChecked).map((input) => input.value);

    const setMessage = (text) => {
      if (!message) return;
      message.textContent = text || "";
    };
    const loadPatterns = () => {
      try {
        const parsed = JSON.parse(window.localStorage.getItem(storageKey) || "{}");
        return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed : {};
      } catch {
        return {};
      }
    };
    const savePatterns = (patterns) => {
      window.localStorage.setItem(storageKey, JSON.stringify(patterns));
    };
    const renderPatternOptions = () => {
      if (!patternSelect) return;
      const selected = patternSelect.value;
      const patterns = loadPatterns();
      patternSelect.innerHTML = '<option value="">保存パターンを選択</option>';
      for (const name of Object.keys(patterns).sort((a, b) => a.localeCompare(b, "ja"))) {
        const option = document.createElement("option");
        option.value = name;
        option.textContent = `${name} (${(patterns[name] || []).length}項目)`;
        patternSelect.appendChild(option);
      }
      if (selected && patterns[selected]) patternSelect.value = selected;
    };
    const setCheckedFields = (fields) => {
      const values = new Set(fields || []);
      for (const input of fieldInputs) {
        input.checked = values.has(input.value);
      }
    };
    const currentFields = () => fieldInputs.filter((input) => input.checked).map((input) => input.value);

    renderPatternOptions();
    caseCsvForm.querySelector("[data-case-csv-check-all-off]")?.addEventListener("click", () => {
      setCheckedFields([]);
      setMessage("全項目をOFFにしました。");
    });
    caseCsvForm.querySelector("[data-case-csv-check-default]")?.addEventListener("click", () => {
      setCheckedFields(defaultFields);
      setMessage("初期値に戻しました。");
    });
    caseCsvForm.querySelector("[data-case-csv-pattern-save]")?.addEventListener("click", () => {
      const name = String(patternNameInput?.value || patternSelect?.value || "").trim();
      if (!name) {
        setMessage("パターン名を入力してください。");
        return;
      }
      const fields = currentFields();
      if (!fields.length) {
        setMessage("登録する項目を1つ以上選択してください。");
        return;
      }
      const patterns = loadPatterns();
      patterns[name] = fields;
      savePatterns(patterns);
      renderPatternOptions();
      if (patternSelect) patternSelect.value = name;
      setMessage(`「${name}」を登録しました。`);
    });
    caseCsvForm.querySelector("[data-case-csv-pattern-apply]")?.addEventListener("click", () => {
      const name = String(patternSelect?.value || "").trim();
      const patterns = loadPatterns();
      if (!name || !patterns[name]) {
        setMessage("呼び出すパターンを選択してください。");
        return;
      }
      setCheckedFields(patterns[name]);
      if (patternNameInput) patternNameInput.value = name;
      setMessage(`「${name}」を呼び出しました。`);
    });
    caseCsvForm.querySelector("[data-case-csv-pattern-delete]")?.addEventListener("click", () => {
      const name = String(patternSelect?.value || "").trim();
      const patterns = loadPatterns();
      if (!name || !patterns[name]) {
        setMessage("削除するパターンを選択してください。");
        return;
      }
      delete patterns[name];
      savePatterns(patterns);
      if (patternNameInput?.value === name) patternNameInput.value = "";
      renderPatternOptions();
      setMessage(`「${name}」を削除しました。`);
    });
    patternSelect?.addEventListener("change", () => {
      if (patternNameInput && patternSelect.value) patternNameInput.value = patternSelect.value;
      setMessage("");
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
        const secondaryIndex = header.dataset.sortSecondaryIndex ? Number(header.dataset.sortSecondaryIndex) : null;
        const secondaryType = header.dataset.sortSecondaryType || "number";
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
          if (compared === 0 && secondaryIndex !== null && Number.isFinite(secondaryIndex)) {
            const leftSecondaryValue = parseSortValue(left.row.children[secondaryIndex], secondaryType);
            const rightSecondaryValue = parseSortValue(right.row.children[secondaryIndex], secondaryType);
            if (typeof leftSecondaryValue === "number" && typeof rightSecondaryValue === "number") {
              compared = leftSecondaryValue - rightSecondaryValue;
            } else {
              compared = String(leftSecondaryValue).localeCompare(String(rightSecondaryValue), "ja");
            }
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

  const csvMappingBulkModal = document.getElementById("csv-mapping-selected-bulk-modal");
  const csvMappingBulkOpen = document.querySelector("[data-csv-mapping-bulk-open]");
  const csvMappingSelectedCount = document.querySelector("[data-csv-mapping-selected-count]");
  const csvMappingBulkSelectedLabel = document.querySelector("[data-csv-mapping-bulk-selected-label]");
  const csvMappingBulkSelectedInputs = document.querySelector("[data-csv-mapping-bulk-selected-inputs]");
  const csvMappingColumnCheckboxes = Array.from(document.querySelectorAll("[data-csv-mapping-column-checkbox]"));
  const fillCsvMappingBulkSelectedInputs = (selected) => {
    if (!csvMappingBulkSelectedInputs) return;
    csvMappingBulkSelectedInputs.innerHTML = selected
      .map((checkbox) => `<input type="hidden" name="column_no" value="${escapeHtml(checkbox.value)}">`)
      .join("");
  };
  const updateCsvMappingSelection = () => {
    const selected = csvMappingColumnCheckboxes.filter((checkbox) => checkbox.checked);
    if (csvMappingSelectedCount) csvMappingSelectedCount.textContent = String(selected.length);
    if (csvMappingBulkSelectedLabel) csvMappingBulkSelectedLabel.textContent = `${selected.length}列`;
    for (const checkbox of csvMappingColumnCheckboxes) {
      const row = checkbox.closest("[data-csv-mapping-selectable-row]");
      if (row) row.classList.toggle("is-selected", checkbox.checked);
    }
    if (csvMappingBulkOpen) {
      csvMappingBulkOpen.classList.toggle("is-disabled", selected.length === 0);
      csvMappingBulkOpen.disabled = selected.length === 0;
    }
    return selected;
  };
  if (csvMappingColumnCheckboxes.length) {
    for (const checkbox of csvMappingColumnCheckboxes) {
      checkbox.addEventListener("change", updateCsvMappingSelection);
      const row = checkbox.closest("[data-csv-mapping-selectable-row]");
      if (row) {
        row.addEventListener("click", (event) => {
          if (event.target.closest("button, a, input, select, textarea, label, summary, details, form")) return;
          checkbox.checked = !checkbox.checked;
          updateCsvMappingSelection();
        });
      }
    }
    updateCsvMappingSelection();
  }
  if (csvMappingBulkOpen && csvMappingBulkModal) {
    csvMappingBulkOpen.addEventListener("click", () => {
      const selected = updateCsvMappingSelection();
      if (!selected.length) return;
      fillCsvMappingBulkSelectedInputs(selected);
      csvMappingBulkModal.hidden = false;
      document.body.classList.add("has-open-modal");
      const firstInput = csvMappingBulkModal.querySelector("input[name='bulk_action']");
      if (firstInput) firstInput.focus();
    });
    const csvMappingBulkForm = csvMappingBulkModal.querySelector(".csv-mapping-selected-bulk-form");
    csvMappingBulkForm?.addEventListener("submit", (event) => {
      const selected = updateCsvMappingSelection();
      fillCsvMappingBulkSelectedInputs(selected);
      if (!selected.length) {
        event.preventDefault();
      }
    });
  }

  const csvLedgerFieldModal = document.getElementById("csv-mapping-ledger-field-modal");
  if (csvLedgerFieldModal) {
    let csvLedgerFieldTargetForm = null;
    const columnLabel = csvLedgerFieldModal.querySelector("[data-csv-ledger-field-column-label]");
    const options = Array.from(csvLedgerFieldModal.querySelectorAll("[data-csv-ledger-field-value]"));
    const openLedgerFieldModal = (button) => {
      csvLedgerFieldTargetForm = document.getElementById(button.getAttribute("data-form-id") || "");
      if (!csvLedgerFieldTargetForm) return;
      const currentField = button.getAttribute("data-current-ledger-field") || "";
      if (columnLabel) columnLabel.textContent = `${button.getAttribute("data-column-no") || "-"}列目`;
      for (const option of options) {
        option.classList.toggle("is-selected", option.getAttribute("data-csv-ledger-field-value") === currentField);
      }
      csvLedgerFieldModal.hidden = false;
      document.body.classList.add("has-open-modal");
      const firstOption = csvLedgerFieldModal.querySelector("[data-csv-ledger-field-value]");
      if (firstOption) firstOption.focus();
    };

    for (const button of document.querySelectorAll("[data-csv-ledger-field-picker-open]")) {
      button.addEventListener("click", () => openLedgerFieldModal(button));
    }

    for (const option of options) {
      option.addEventListener("click", () => {
        if (!csvLedgerFieldTargetForm) return;
        const field = option.getAttribute("data-csv-ledger-field-value") || "";
        let targetKind = csvLedgerFieldTargetForm.querySelector('input[name="target_kind"]');
        if (!targetKind) {
          targetKind = document.createElement("input");
          targetKind.type = "hidden";
          targetKind.name = "target_kind";
          csvLedgerFieldTargetForm.appendChild(targetKind);
        }
        targetKind.value = "LEDGER_FIELD";
        let ledgerField = csvLedgerFieldTargetForm.querySelector('input[name="target_ledger_field"]');
        if (!ledgerField) {
          ledgerField = document.createElement("input");
          ledgerField.type = "hidden";
          ledgerField.name = "target_ledger_field";
          csvLedgerFieldTargetForm.appendChild(ledgerField);
        }
        ledgerField.value = field;
        csvLedgerFieldTargetForm.submit();
      });
    }
  }

  const csvExamItemModal = document.getElementById("csv-mapping-exam-item-modal");
  if (csvExamItemModal) {
    let csvExamItemTargetForm = null;
    let csvExamItemCurrentNamecode = "";
    const columnLabel = csvExamItemModal.querySelector("[data-csv-exam-item-column-label]");
    const sourceHeaderLabel = csvExamItemModal.querySelector("[data-csv-exam-item-source-header]");
    const searchInput = csvExamItemModal.querySelector("[data-csv-exam-item-search-input]");
    const typeButtons = Array.from(csvExamItemModal.querySelectorAll("[data-csv-exam-item-type-filter]"));
    const results = csvExamItemModal.querySelector("[data-csv-exam-item-results]");
    const selectedDetail = csvExamItemModal.querySelector("[data-csv-exam-item-selected]");
    const submitButton = csvExamItemModal.querySelector("[data-csv-exam-item-submit]");
    let searchTimer = null;
    let csvExamItemItems = [];
    let csvExamItemSelectedNamecode = "";
    let csvExamItemResultLimit = 80;
    let csvExamItemValueType = "";

    const submitExamItemRule = (namecode) => {
      if (!csvExamItemTargetForm || !namecode) return;
      let targetKind = csvExamItemTargetForm.querySelector('input[name="target_kind"]');
      if (!targetKind) {
        targetKind = document.createElement("input");
        targetKind.type = "hidden";
        targetKind.name = "target_kind";
        csvExamItemTargetForm.appendChild(targetKind);
      }
      targetKind.value = "EXAM_ITEM_VALUE";
      let targetNamecode = csvExamItemTargetForm.querySelector('input[name="target_namecode"]');
      if (!targetNamecode) {
        targetNamecode = document.createElement("input");
        targetNamecode.type = "hidden";
        targetNamecode.name = "target_namecode";
        csvExamItemTargetForm.appendChild(targetNamecode);
      }
      targetNamecode.value = namecode;
      csvExamItemTargetForm.submit();
    };

    const renderExamItemSelectedDetail = (item) => {
      if (!selectedDetail || !submitButton) return;
      if (!item) {
        selectedDetail.hidden = true;
        selectedDetail.innerHTML = `<p class="subtle">候補を選ぶと、ここに健診項目マスターの詳細を表示します。</p>`;
        submitButton.disabled = true;
        return;
      }
      const standardRows = Array.isArray(item.standard_code_rows) ? item.standard_code_rows : [];
      const variantRows = Array.isArray(item.norm_variant_rows) ? item.norm_variant_rows : [];
      const detailRows = [
        ["namecode", item.namecode],
        ["項目名", item.item_name],
        ["カテゴリ", item.category_name],
        ["区分", [item.kubun_no, item.kubun_name].filter(Boolean).join(" / ")],
        ["順番", item.jun_no],
        ["識別項目", [item.identity_item_code, item.identity_item_name].filter(Boolean).join(" / ")],
        ["XML値型", [item.xml_value_type, item.data_type_label].filter(Boolean).join(" / ")],
        ["単位", [item.display_unit, item.ucum_unit].filter(Boolean).join(" / ")],
        ["項目OID", item.item_code_oid],
        ["結果OID", item.result_code_oid],
        ["検査方法", [item.xml_method_code, item.method_name].filter(Boolean).join(" / ")],
        ["値取得", item.value_method],
        ["法定", item.annex2_legal_report_flag === 1 || item.annex2_legal_report_flag === "1" ? "対象" : "対象外"],
        ["実施要件", item.annex2_exec_requirement],
        ["NullFlavor", item.nullflavor_allowed === 1 || item.nullflavor_allowed === "1" ? "可" : "不可"],
      ];
      selectedDetail.hidden = false;
      selectedDetail.innerHTML = `
        <div class="csv-mapping-exam-item-selected-head">
          <div>
            <span class="status-pill">選択中</span>
            <h3>${escapeHtml(item.item_name || item.namecode || "-")}</h3>
            <small>${escapeHtml(item.namecode || "-")}</small>
          </div>
          <small>norm ${escapeHtml(String(standardRows.length))}標準 / ${escapeHtml(String(variantRows.length))}件</small>
        </div>
        <dl class="definition-grid">
          ${detailRows.map(([label, value]) => `
            <div>
              <dt>${escapeHtml(label)}</dt>
              <dd>${escapeHtml(value || "-")}</dd>
            </div>
          `).join("")}
        </dl>
        ${item.notes ? `<p class="csv-mapping-exam-item-notes">${escapeHtml(item.notes)}</p>` : ""}
        <div class="csv-mapping-exam-item-norm">
          <h4>標準コード</h4>
          ${standardRows.length ? `
            <table class="mini-table">
              <thead><tr><th>code</th><th>表示</th><th>入力値</th></tr></thead>
              <tbody>
                ${standardRows.slice(0, 10).map((row) => `
                  <tr>
                    <td>${escapeHtml(row.normalized_code || "-")}</td>
                    <td>${escapeHtml(row.display_name || "-")}</td>
                    <td>${escapeHtml(row.raw_value_utf8 || "-")}</td>
                  </tr>
                `).join("")}
              </tbody>
            </table>
          ` : `<p class="subtle">標準コードはありません。</p>`}
        </div>
        <div class="csv-mapping-exam-item-norm">
          <h4>norm登録</h4>
          ${variantRows.length ? `
            <table class="mini-table">
              <thead><tr><th>入力値</th><th>正規code</th><th>表示</th><th>種別</th></tr></thead>
              <tbody>
                ${variantRows.slice(0, 18).map((row) => `
                  <tr>
                    <td>${escapeHtml(row.raw_value_utf8 || "-")}</td>
                    <td>${escapeHtml(row.normalized_code || "-")}</td>
                    <td>${escapeHtml(row.display_name || "-")}</td>
                    <td>${row.is_canonical === 1 || row.is_canonical === "1" ? `<span class="status-pill status-ready">標準</span>` : `<span class="status-pill status-muted">揺れ</span>`}${row.is_active === 0 || row.is_active === "0" ? ` <span class="status-pill status-danger">無効</span>` : ""}</td>
                  </tr>
                `).join("")}
              </tbody>
            </table>
            ${variantRows.length > 18 ? `<p class="subtle">先頭18件のみ表示しています。全${escapeHtml(String(variantRows.length))}件</p>` : ""}
          ` : `<p class="subtle">norm登録はありません。</p>`}
        </div>
      `;
      submitButton.disabled = false;
    };

    const selectExamItem = (namecode) => {
      csvExamItemSelectedNamecode = namecode || "";
      renderExamItemResults(csvExamItemItems);
      const item = csvExamItemItems.find((candidate) => candidate.namecode === csvExamItemSelectedNamecode);
      renderExamItemSelectedDetail(item);
    };

    const renderExamItemResults = (items) => {
      if (!results) return;
      if (!items.length) {
        results.innerHTML = `<p class="subtle">候補はありません。</p>`;
        return;
      }
      const limitMessage = items.length >= csvExamItemResultLimit
        ? `<p class="subtle csv-mapping-exam-item-limit-note">${escapeHtml(String(csvExamItemResultLimit))}件まで表示しています。項目名やnamecodeでもう少し絞り込んでください。</p>`
        : "";
      results.innerHTML = items.map((item) => {
        const selected = item.namecode === csvExamItemSelectedNamecode;
        const meta = [
          item.namecode,
          item.xml_value_type,
          item.display_unit,
          item.method_name,
        ].filter(Boolean).join(" / ");
        return `
          <button type="button" class="csv-mapping-exam-item-option${selected ? " is-selected" : ""}" data-csv-exam-item-namecode="${escapeHtml(item.namecode)}">
            <strong>${escapeHtml(item.item_name || item.namecode)}</strong>
            <small>${escapeHtml(meta || "-")}</small>
            <span>${escapeHtml(item.category_name || "-")}${item.identity_item_name ? ` / ${escapeHtml(item.identity_item_name)}` : ""}</span>
          </button>
        `;
      }).join("") + limitMessage;
    };

    const searchExamItems = async () => {
      const keyword = String(searchInput?.value || "").trim();
      if (!results) return;
      results.innerHTML = `<p class="subtle">検索中...</p>`;
      try {
        const params = new URLSearchParams();
        params.set("keyword", keyword);
        if (csvExamItemValueType) params.set("value_type", csvExamItemValueType);
        const response = await fetch(`/api/csv-mapping-lab/exam-items?${params.toString()}`);
        if (!response.ok) throw new Error("search_failed");
        const payload = await response.json();
        csvExamItemResultLimit = Number(payload.limit || 80);
        csvExamItemItems = Array.isArray(payload.items) ? payload.items : [];
        renderExamItemResults(csvExamItemItems);
        const currentItem = csvExamItemItems.find((item) => item.namecode === csvExamItemCurrentNamecode);
        renderExamItemSelectedDetail(csvExamItemSelectedNamecode ? csvExamItemItems.find((item) => item.namecode === csvExamItemSelectedNamecode) : currentItem);
        if (!csvExamItemSelectedNamecode && currentItem) {
          csvExamItemSelectedNamecode = currentItem.namecode;
          renderExamItemResults(csvExamItemItems);
          renderExamItemSelectedDetail(currentItem);
        }
      } catch (error) {
        results.innerHTML = `<p class="subtle">検索でエラーが発生しました。</p>`;
      }
    };

    for (const button of document.querySelectorAll("[data-csv-exam-item-picker-open]")) {
      button.addEventListener("click", () => {
        csvExamItemTargetForm = document.getElementById(button.getAttribute("data-form-id") || "");
        if (!csvExamItemTargetForm) return;
        csvExamItemCurrentNamecode = button.getAttribute("data-current-namecode") || "";
        csvExamItemSelectedNamecode = csvExamItemCurrentNamecode;
        csvExamItemItems = [];
        csvExamItemValueType = "";
        typeButtons.forEach((typeButton) => {
          typeButton.classList.toggle("is-selected", !typeButton.getAttribute("data-csv-exam-item-type-filter"));
        });
        const sourceHeader = button.getAttribute("data-header-name") || "-";
        if (columnLabel) columnLabel.textContent = `${button.getAttribute("data-column-no") || "-"}列目`;
        if (sourceHeaderLabel) sourceHeaderLabel.textContent = sourceHeader;
        if (searchInput) searchInput.value = csvExamItemCurrentNamecode || button.getAttribute("data-header-name") || "";
        renderExamItemSelectedDetail(null);
        csvExamItemModal.hidden = false;
        document.body.classList.add("has-open-modal");
        if (searchInput) searchInput.focus();
        searchExamItems();
      });
    }

    if (searchInput) {
      searchInput.addEventListener("input", () => {
        window.clearTimeout(searchTimer);
        searchTimer = window.setTimeout(searchExamItems, 220);
      });
    }

    typeButtons.forEach((button) => {
      button.addEventListener("click", () => {
        csvExamItemValueType = String(button.getAttribute("data-csv-exam-item-type-filter") || "");
        typeButtons.forEach((typeButton) => {
          typeButton.classList.toggle("is-selected", typeButton === button);
        });
        searchExamItems();
      });
    });
    if (results) {
      results.addEventListener("click", (event) => {
        const option = event.target.closest("[data-csv-exam-item-namecode]");
        if (!option) return;
        selectExamItem(option.getAttribute("data-csv-exam-item-namecode") || "");
      });
    }
    if (submitButton) {
      submitButton.addEventListener("click", () => submitExamItemRule(csvExamItemSelectedNamecode));
    }
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

  let aliasFacilityTargetInput = null;
  let aliasFacilityTargetNameInput = null;
  let aliasFacilityTargetDisplayInput = null;
  let aliasFacilityTargetValue = "code";
  let aliasFacilityShouldFillCsvTemplateDefaults = false;
  const codeFromFolderAliasName = (folderName) => {
    const firstPart = String(folderName || "").trim().split("_")[0]?.trim() || "";
    return /^[0-9A-Za-z-]{2,}$/.test(firstPart) ? firstPart : "";
  };
  const yyyymmdd = (date = new Date()) => {
    const year = String(date.getFullYear());
    const month = String(date.getMonth() + 1).padStart(2, "0");
    const day = String(date.getDate()).padStart(2, "0");
    return `${year}${month}${day}`;
  };
  const fillCsvTemplateDefaultsFromFacility = ({ code, name }) => {
    const mappingVersionInput = document.querySelector("[data-csv-template-mapping-version]");
    const formatNameInput = document.querySelector("[data-csv-template-format-name]");
    const cleanCode = String(code || "").trim();
    const cleanName = String(name || "").trim();
    if (mappingVersionInput && !String(mappingVersionInput.value || "").trim() && cleanCode) {
      mappingVersionInput.value = `${cleanCode}_V1_${yyyymmdd()}`;
      mappingVersionInput.dispatchEvent(new Event("input", { bubbles: true }));
      mappingVersionInput.dispatchEvent(new Event("change", { bubbles: true }));
    }
    if (formatNameInput && !String(formatNameInput.value || "").trim() && cleanName) {
      formatNameInput.value = `${cleanName}_V1`;
      formatNameInput.dispatchEvent(new Event("input", { bubbles: true }));
      formatNameInput.dispatchEvent(new Event("change", { bubbles: true }));
    }
  };
  for (const toggle of document.querySelectorAll(".csv-template-active-radio-toggle")) {
    const syncActiveRadioToggle = () => {
      for (const option of toggle.querySelectorAll(".csv-template-active-radio-toggle__option")) {
        const input = option.querySelector("input");
        const checked = Boolean(input?.checked);
        option.classList.toggle("is-active", checked);
        option.classList.toggle("is-on", checked && input?.value === "1");
        option.classList.toggle("is-off", checked && input?.value === "0");
      }
    };
    toggle.addEventListener("change", syncActiveRadioToggle);
    syncActiveRadioToggle();
  }
  for (const button of document.querySelectorAll("[data-alias-facility-picker-open]")) {
    button.addEventListener("click", () => {
      const targetId = button.getAttribute("data-target-input") || "";
      const targetNameId = button.getAttribute("data-target-name-input") || "";
      const targetDisplayId = button.getAttribute("data-target-display-input") || "";
      aliasFacilityTargetInput = targetId ? document.getElementById(targetId) : null;
      aliasFacilityTargetNameInput = targetNameId ? document.getElementById(targetNameId) : null;
      aliasFacilityTargetDisplayInput = targetDisplayId ? document.getElementById(targetDisplayId) : null;
      aliasFacilityTargetValue = button.getAttribute("data-target-value") || "code";
      aliasFacilityShouldFillCsvTemplateDefaults = button.getAttribute("data-csv-template-facility-defaults") === "1";
      const form = button.closest("form");
      const folderInput = form?.querySelector("[data-folder-alias-src]");
      const code = codeFromFolderAliasName(folderInput?.value);
      const modal = document.getElementById(button.getAttribute("data-modal-open") || "");
      if (!modal) return;
      modal.dataset.aliasTargetInput = targetId;
      modal.dataset.aliasTargetNameInput = targetNameId;
      modal.dataset.aliasTargetDisplayInput = targetDisplayId;
      modal.dataset.aliasTargetValue = aliasFacilityTargetValue;
      modal.dataset.csvTemplateFacilityDefaults = aliasFacilityShouldFillCsvTemplateDefaults ? "1" : "0";
      if (!code) return;
      window.setTimeout(() => {
        const codeInput = modal.querySelector("[data-alias-facility-search-code]");
        const prefectureInput = modal.querySelector("[data-alias-facility-search-prefecture]");
        const keywordInput = modal.querySelector("[data-alias-facility-search-keyword]");
        const searchButton = modal.querySelector("[data-alias-facility-search]");
        if (codeInput) codeInput.value = code;
        if (prefectureInput) prefectureInput.value = "";
        if (keywordInput) keywordInput.value = "";
        searchButton?.click();
      }, 0);
    });
  }
  const selectAliasFacility = (element) => {
    if (!(element instanceof Element)) return;
    const modal = element.closest(".edit-modal");
    const modalTargetId = modal?.dataset.aliasTargetInput || "";
    const modalTargetNameId = modal?.dataset.aliasTargetNameInput || "";
    const modalTargetDisplayId = modal?.dataset.aliasTargetDisplayInput || "";
    const modalTargetValue = modal?.dataset.aliasTargetValue || aliasFacilityTargetValue || "code";
    const shouldFillCsvTemplateDefaults =
      (modal?.dataset.csvTemplateFacilityDefaults || "") === "1" || aliasFacilityShouldFillCsvTemplateDefaults;
    const targetInput = modalTargetId ? document.getElementById(modalTargetId) : aliasFacilityTargetInput;
    const targetNameInput = modalTargetNameId ? document.getElementById(modalTargetNameId) : aliasFacilityTargetNameInput;
    const targetDisplayInput = modalTargetDisplayId ? document.getElementById(modalTargetDisplayId) : aliasFacilityTargetDisplayInput;
    if (!targetInput) return;
    const code = String(element.getAttribute("data-facility-code") || "").trim();
    const facilityId = String(element.getAttribute("data-facility-id") || "").trim();
    const name = String(element.getAttribute("data-facility-name") || "").trim();
    const display = String(element.getAttribute("data-facility-display") || "").trim()
      || [name, code || "-", facilityId ? `ID ${facilityId}` : ""].filter(Boolean).join(" / ");
    const nextValue = modalTargetValue === "exam_facility_id" ? facilityId : code;
    if (!nextValue) return;
    targetInput.value = nextValue;
    targetInput.dispatchEvent(new Event("input", { bubbles: true }));
    targetInput.dispatchEvent(new Event("change", { bubbles: true }));
    if (targetNameInput && name) {
      targetNameInput.value = name;
      targetNameInput.dispatchEvent(new Event("input", { bubbles: true }));
      targetNameInput.dispatchEvent(new Event("change", { bubbles: true }));
    }
    if (targetDisplayInput && display) {
      targetDisplayInput.value = display;
      targetDisplayInput.dispatchEvent(new Event("input", { bubbles: true }));
      targetDisplayInput.dispatchEvent(new Event("change", { bubbles: true }));
    }
    if (shouldFillCsvTemplateDefaults) {
      fillCsvTemplateDefaultsFromFacility({ code, name });
    }
    closeModal(modal);
    if (targetDisplayInput instanceof HTMLElement) {
      targetDisplayInput.focus();
    } else if (targetInput instanceof HTMLElement) {
      targetInput.focus();
    }
  };
  const renderAliasFacilityResults = (tbody, items, meta = {}) => {
    if (!tbody) return;
    const totalCount = Number(meta.total_count || 0);
    const limit = Number(meta.limit || 50);
    const hasMore = Boolean(meta.has_more);
    const summaryHtml = totalCount
      ? `<tr class="table-message-row"><td colspan="4">該当 ${escapeHtml(totalCount)} 件。${hasMore ? `${escapeHtml(limit)}件まで表示しています。コードや名称で絞り込んでください。` : `${escapeHtml(items.length)}件を表示しています。`}</td></tr>`
      : "";
    if (!items.length) {
      tbody.innerHTML = '<tr><td colspan="4">一致する健診機関はありません。</td></tr>';
      return;
    }
    tbody.innerHTML = summaryHtml + items.map((item) => {
      const code = item.exam_facility_code || item.medical_institution_code || item.reservation_system_medical_institution_code || "";
      const name = item.exam_facility_display_name || item.exam_facility_name || "名称未設定";
      const facilityId = item.exam_facility_id || "";
      const display = [name, code || "-", facilityId ? `ID ${facilityId}` : ""].filter(Boolean).join(" / ");
      const address = [item.postal_code || "", item.address || ""].filter(Boolean).join(" ");
      const related = item.medical_institution_code && item.medical_institution_code !== code
        ? `医療機関 ${escapeHtml(item.medical_institution_code)}`
        : "";
      const reservationCode = item.reservation_system_medical_institution_code || "";
      return `
        <tr data-alias-facility-row data-facility-id="${escapeHtml(facilityId)}" data-facility-code="${escapeHtml(code)}" data-facility-name="${escapeHtml(name)}" data-facility-display="${escapeHtml(display)}">
          <td>
            <strong>${escapeHtml(name)}</strong>
            <small>${escapeHtml(code)}${related ? ` / ${related}` : ""}</small>
          </td>
          <td>
            <small>内部ID: ${escapeHtml(facilityId || "-")}</small>
            <small>健診機関コード: ${escapeHtml(item.exam_facility_code || "-")}</small>
            <small>医療機関コード: ${escapeHtml(item.medical_institution_code || "-")}</small>
            ${reservationCode ? `<small>予約システム: ${escapeHtml(reservationCode)}</small>` : ""}
          </td>
          <td><small>${escapeHtml(address)}</small></td>
          <td><button type="button" class="ghost-button compact-action-button" data-alias-facility-select data-facility-id="${escapeHtml(facilityId)}" data-facility-code="${escapeHtml(code)}" data-facility-name="${escapeHtml(name)}" data-facility-display="${escapeHtml(display)}">選択</button></td>
        </tr>
      `;
    }).join("");
  };
  for (const modal of document.querySelectorAll("[data-alias-facility-picker-modal]")) {
    const codeInput = modal.querySelector("[data-alias-facility-search-code]");
    const prefectureInput = modal.querySelector("[data-alias-facility-search-prefecture]");
    const keywordInput = modal.querySelector("[data-alias-facility-search-keyword]");
    const searchButton = modal.querySelector("[data-alias-facility-search]");
    const resultsBody = modal.querySelector("[data-alias-facility-results]");
    let isComposingPrefecture = false;
    const completePrefectureIfUnique = () => {
      if (!prefectureInput || isComposingPrefecture) return;
      const value = String(prefectureInput.value || "").trim();
      if (!value) return;
      const listId = prefectureInput.getAttribute("list");
      const options = listId
        ? Array.from(document.getElementById(listId)?.querySelectorAll("option") || [])
          .map((option) => String(option.value || "").trim())
          .filter(Boolean)
        : [];
      if (options.includes(value)) return;
      const matches = options.filter((option) => option.startsWith(value));
      if (matches.length === 1) prefectureInput.value = matches[0];
    };
    const runSearch = async () => {
      completePrefectureIfUnique();
      const code = String(codeInput?.value || "").trim();
      const prefecture = String(prefectureInput?.value || "").trim();
      const keyword = String(keywordInput?.value || "").trim();
      if (!code && !prefecture && !keyword) {
        renderAliasFacilityResults(resultsBody, []);
        return;
      }
      if (code && code.length < 2) {
        if (resultsBody) resultsBody.innerHTML = '<tr><td colspan="4">コードは2桁以上で検索してください。</td></tr>';
        return;
      }
      if (resultsBody) resultsBody.innerHTML = '<tr><td colspan="4">検索しています...</td></tr>';
      try {
        const params = new URLSearchParams();
        if (code) {
          params.set("code", code);
          params.set("code_match", "partial");
        }
        if (prefecture) params.set("prefecture", prefecture);
        if (keyword) params.set("q", keyword);
        const payload = await fetchJson(`/admin/facility-master/search?${params.toString()}`);
        renderAliasFacilityResults(resultsBody, payload.items || [], payload);
      } catch (error) {
        if (resultsBody) resultsBody.innerHTML = `<tr><td colspan="4">検索でエラーが発生しました。${escapeHtml(error.message || "")}</td></tr>`;
      }
    };
    searchButton?.addEventListener("click", runSearch);
    prefectureInput?.addEventListener("compositionstart", () => {
      isComposingPrefecture = true;
    });
    prefectureInput?.addEventListener("compositionend", () => {
      isComposingPrefecture = false;
      completePrefectureIfUnique();
    });
    prefectureInput?.addEventListener("input", completePrefectureIfUnique);
    prefectureInput?.addEventListener("blur", completePrefectureIfUnique);
    for (const input of [codeInput, prefectureInput, keywordInput]) {
      input?.addEventListener("keydown", (event) => {
        if (event.key === "Enter") {
          event.preventDefault();
          runSearch();
        }
      });
    }
    resultsBody?.addEventListener("click", (event) => {
      const target = event.target instanceof Element ? event.target : null;
      const select = target?.closest("[data-alias-facility-select]");
      const row = target?.closest("[data-alias-facility-row]");
      if (select) {
        event.stopPropagation();
        selectAliasFacility(select);
      } else if (row) {
        selectAliasFacility(row);
      }
    });
  }

  const normalizeFolderAliasName = (value) => String(value || "")
    .trim()
    .replace(/[\\\/]+/g, "_")
    .replace(/[\s　]+/g, "_")
    .replace(/_+/g, "_");
  for (const srcInput of document.querySelectorAll("[data-folder-alias-src]")) {
    const form = srcInput.closest("form");
    const normInput = form?.querySelector("[data-folder-alias-norm]");
    if (!normInput) continue;
    let previousGenerated = normalizeFolderAliasName(srcInput.value);
    if (!normInput.value) {
      normInput.value = previousGenerated;
    }
    srcInput.addEventListener("input", () => {
      const nextGenerated = normalizeFolderAliasName(srcInput.value);
      const currentNorm = String(normInput.value || "").trim();
      if (!currentNorm || currentNorm === previousGenerated) {
        normInput.value = nextGenerated;
      }
      previousGenerated = nextGenerated;
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

  const personColumnEditor = document.querySelector("[data-person-column-editor]");
  if (personColumnEditor) {
    const cardsContainer = personColumnEditor.querySelector("[data-person-column-cards]");
    const assignButton = personColumnEditor.querySelector("[data-person-column-mode='assign']");
    const reorderButton = personColumnEditor.querySelector("[data-person-column-mode='reorder']");
    const doneButton = personColumnEditor.querySelector("[data-person-column-done]");
    const editingStatus = personColumnEditor.querySelector("[data-person-column-editing-status]");
    const picker = document.getElementById("person-column-picker-modal");
    const pickerTarget = picker?.querySelector("[data-person-column-picker-target]");
    const pickerApply = picker?.querySelector("[data-person-column-picker-apply]");
    let editMode = "";
    let activeCard = null;
    let selectedOption = null;
    let draggedCard = null;

    const syncColumnNames = () => {
      const cards = Array.from(personColumnEditor.querySelectorAll("[data-person-column-card]"));
      cards.forEach((card, index) => {
        const input = card.querySelector("[data-person-column-input]");
        const order = card.querySelector(".person-column-card-order");
        card.dataset.columnIndex = String(index);
        if (input) input.name = `col_${index}`;
        if (order) order.textContent = `${index + 1}列目`;
      });
    };

    const setColumnEditMode = (mode) => {
      editMode = mode;
      personColumnEditor.dataset.columnMode = mode;
      assignButton?.classList.toggle("is-active", mode === "assign");
      reorderButton?.classList.toggle("is-active", mode === "reorder");
      if (doneButton) doneButton.hidden = !mode;
      if (editingStatus) {
        editingStatus.hidden = !mode;
        editingStatus.textContent = mode === "assign" ? "項目編集中" : mode === "reorder" ? "入れ替え編集中" : "";
      }
      for (const card of personColumnEditor.querySelectorAll("[data-person-column-card]")) {
        const selectButton = card.querySelector("[data-person-column-open]");
        const isAssign = mode === "assign";
        const isReorder = mode === "reorder";
        if (selectButton) selectButton.hidden = !isAssign;
        card.draggable = isReorder;
        card.classList.toggle("is-reorderable", isReorder);
      }
    };

    const closePicker = () => {
      if (picker) picker.hidden = true;
      activeCard = null;
      selectedOption = null;
      for (const option of picker?.querySelectorAll("[data-person-column-option]") || []) {
        option.classList.remove("is-selected");
      }
    };

    const openPicker = (card) => {
      activeCard = card;
      selectedOption = null;
      const index = Number(card.dataset.columnIndex || "0") + 1;
      if (pickerTarget) pickerTarget.textContent = `${index}列目に割り当てる項目を選びます。`;
      const currentValue = card.querySelector("[data-person-column-input]")?.value || "";
      for (const option of picker?.querySelectorAll("[data-person-column-option]") || []) {
        const selected = option.dataset.value === currentValue;
        const optionValue = option.dataset.value || "";
        const usage = option.querySelector("[data-person-column-option-usage]");
        if (usage) {
          const usedColumns = optionValue
            ? Array.from(personColumnEditor.querySelectorAll("[data-person-column-card]"))
              .map((columnCard, columnIndex) => (
                (columnCard.querySelector("[data-person-column-input]")?.value || "") === optionValue
                  ? `${columnIndex + 1}列目`
                  : ""
              ))
              .filter(Boolean)
            : [];
          usage.textContent = usedColumns.length ? `現在 ${usedColumns.join(" / ")}` : "";
        }
        option.classList.toggle("is-selected", selected);
        if (selected) selectedOption = option;
      }
      if (picker) picker.hidden = false;
    };

    assignButton?.addEventListener("click", () => setColumnEditMode(editMode === "assign" ? "" : "assign"));
    reorderButton?.addEventListener("click", () => setColumnEditMode(editMode === "reorder" ? "" : "reorder"));
    doneButton?.addEventListener("click", () => setColumnEditMode(""));

    personColumnEditor.addEventListener("click", (event) => {
      const target = event.target instanceof Element ? event.target : null;
      const openButton = target?.closest("[data-person-column-open]");
      if (!openButton) return;
      const card = openButton.closest("[data-person-column-card]");
      if (card) openPicker(card);
    });

    picker?.querySelector("[data-person-column-picker-close]")?.addEventListener("click", closePicker);
    picker?.querySelector("[data-person-column-picker-cancel]")?.addEventListener("click", closePicker);
    picker?.addEventListener("click", (event) => {
      if (event.target === picker) closePicker();
    });
    for (const option of picker?.querySelectorAll("[data-person-column-option]") || []) {
      option.addEventListener("click", () => {
        selectedOption = option;
        for (const peer of picker.querySelectorAll("[data-person-column-option]")) {
          peer.classList.toggle("is-selected", peer === option);
        }
      });
    }
    pickerApply?.addEventListener("click", () => {
      if (!activeCard || !selectedOption) return;
      const input = activeCard.querySelector("[data-person-column-input]");
      const label = activeCard.querySelector("[data-person-column-label]");
      const nextValue = selectedOption.dataset.value || "";
      const nextLabel = selectedOption.dataset.label || "未使用";
      const currentValue = input?.value || "";
      const currentLabel = label?.textContent || "未使用";
      if (nextValue) {
        const duplicateCard = Array.from(personColumnEditor.querySelectorAll("[data-person-column-card]")).find((card) => {
          if (card === activeCard) return false;
          return (card.querySelector("[data-person-column-input]")?.value || "") === nextValue;
        });
        if (duplicateCard) {
          const duplicateInput = duplicateCard.querySelector("[data-person-column-input]");
          const duplicateLabel = duplicateCard.querySelector("[data-person-column-label]");
          if (duplicateInput) duplicateInput.value = currentValue;
          if (duplicateLabel) duplicateLabel.textContent = currentLabel;
          duplicateCard.classList.toggle("is-unused", !currentValue);
        }
      }
      if (input) input.value = nextValue;
      if (label) label.textContent = nextLabel;
      activeCard.classList.toggle("is-unused", !nextValue);
      closePicker();
    });

    cardsContainer?.addEventListener("dragstart", (event) => {
      const target = event.target instanceof Element ? event.target : null;
      draggedCard = target?.closest("[data-person-column-card]");
      if (!draggedCard || editMode !== "reorder") {
        event.preventDefault();
        return;
      }
      draggedCard.classList.add("is-dragging");
      event.dataTransfer.effectAllowed = "move";
    });
    cardsContainer?.addEventListener("dragend", () => {
      draggedCard?.classList.remove("is-dragging");
      draggedCard = null;
      syncColumnNames();
    });
    cardsContainer?.addEventListener("dragover", (event) => {
      if (!draggedCard || editMode !== "reorder") return;
      event.preventDefault();
      const eventTarget = event.target instanceof Element ? event.target : null;
      const target = eventTarget?.closest("[data-person-column-card]");
      if (!target || target === draggedCard) return;
      const rect = target.getBoundingClientRect();
      const after = event.clientY > rect.top + rect.height / 2 || event.clientX > rect.left + rect.width / 2;
      cardsContainer.insertBefore(draggedCard, after ? target.nextSibling : target);
    });

    syncColumnNames();
    for (const card of personColumnEditor.querySelectorAll("[data-person-column-card]")) {
      const input = card.querySelector("[data-person-column-input]");
      card.classList.toggle("is-unused", !input?.value);
    }
    setColumnEditMode("");
  }

  const manualFacilityInput = document.querySelector("#manual-entry-facility-input");
  if (manualFacilityInput) {
    const manualFacilityNameInput = document.querySelector("#manual-entry-facility-name-input");
    const setManualFacility = (code, name = "") => {
      manualFacilityInput.value = code || "";
      manualFacilityInput.dispatchEvent(new Event("input", { bubbles: true }));
      if (manualFacilityNameInput) {
        manualFacilityNameInput.value = name || "";
        manualFacilityNameInput.dispatchEvent(new Event("input", { bubbles: true }));
      }
      const caseFacilityInput = document.querySelector("#manual-entry-case-search-facility");
      if (caseFacilityInput) caseFacilityInput.value = code || "";
      document.querySelector("#manual-entry-facility-picker-modal [data-modal-close]")?.click();
    };

    for (const button of document.querySelectorAll("[data-manual-entry-facility-select]")) {
      button.addEventListener("click", (event) => {
        event.stopPropagation();
        setManualFacility(button.dataset.facilityCode || "", button.dataset.facilityName || "");
      });
    }

    for (const row of document.querySelectorAll("[data-manual-entry-facility-row]")) {
      row.addEventListener("click", () => {
        setManualFacility(row.dataset.facilityCode || "", row.dataset.facilityName || "");
      });
    }
  }

  const manualCaseFacilityInput = document.querySelector("#manual-entry-case-search-facility");
  const manualCaseFacilitySummary = document.getElementById("manual-entry-case-facility-codes-summary");
  const manualCaseFacilityValues = () => {
    if (!manualCaseFacilityInput) return [];
    return manualCaseFacilityInput.value
      .split(/[\s,，、]+/)
      .map((value) => value.trim())
      .filter(Boolean);
  };
  const updateManualCaseFacilityPickerState = () => {
    const values = new Set(manualCaseFacilityValues());
    if (manualCaseFacilitySummary) {
      manualCaseFacilitySummary.textContent = values.size ? `${values.size}施設を指定中` : "未指定: 全施設";
    }
    for (const button of document.querySelectorAll("[data-manual-entry-case-facility-select]")) {
      const code = String(button.getAttribute("data-facility-code") || "").trim();
      const added = code && values.has(code);
      button.textContent = added ? "解除" : "追加";
      button.classList.toggle("is-active", Boolean(added));
      button.disabled = false;
    }
    for (const row of document.querySelectorAll("[data-manual-entry-case-facility-row]")) {
      const code = String(row.getAttribute("data-facility-code") || "").trim();
      row.classList.toggle("is-selected", Boolean(code && values.has(code)));
    }
  };
  const selectManualCaseFacility = (element) => {
    if (!manualCaseFacilityInput || !(element instanceof Element)) return;
    const code = String(element.getAttribute("data-facility-code") || "").trim();
    if (!code) return;
    const values = manualCaseFacilityValues();
    if (values.includes(code)) {
      manualCaseFacilityInput.value = values.filter((value) => value !== code).join(", ");
    } else {
      values.push(code);
      manualCaseFacilityInput.value = values.join(", ");
    }
    manualCaseFacilityInput.dispatchEvent(new Event("input", { bubbles: true }));
    updateManualCaseFacilityPickerState();
  };
  if (manualCaseFacilityInput) {
    manualCaseFacilityInput.addEventListener("input", updateManualCaseFacilityPickerState);
    updateManualCaseFacilityPickerState();
  }
  for (const button of document.querySelectorAll("[data-manual-entry-case-facility-select]")) {
    button.addEventListener("click", (event) => {
      event.stopPropagation();
      selectManualCaseFacility(button);
    });
  }
  for (const row of document.querySelectorAll("[data-manual-entry-case-facility-row]")) {
    row.addEventListener("click", () => selectManualCaseFacility(row));
  }
  for (const button of document.querySelectorAll("[data-manual-entry-case-facility-clear]")) {
    button.addEventListener("click", () => {
      if (!manualCaseFacilityInput) return;
      manualCaseFacilityInput.value = "";
      manualCaseFacilityInput.dispatchEvent(new Event("input", { bubbles: true }));
      updateManualCaseFacilityPickerState();
      closeModal(button.closest(".edit-modal"));
    });
  }

  const manualSubscriberResults = document.querySelector("[data-manual-entry-subscriber-results]");
  let refreshManualEntryFilledCount = () => 0;
  let refreshManualCodeToggle = () => {};
  let scheduleManualEntryDraftSave = () => {};
  if (manualSubscriberResults) {
    const searchButton = document.querySelector("[data-manual-entry-subscriber-search]");
    const applyButton = document.querySelector("[data-manual-entry-subscriber-apply]");
    const caseSearchButton = document.querySelector("[data-manual-entry-case-search]");
    const selectedCasePanel = document.querySelector("[data-manual-entry-selected-case]");
    const selectedCaseTitle = document.querySelector("[data-manual-entry-selected-case-title]");
    const selectedCaseDetail = document.querySelector("[data-manual-entry-selected-case-detail]");
    const selectedCaseDetailLink = document.querySelector("[data-manual-entry-selected-case-detail-link]");
    const casePanel = document.querySelector("[data-manual-entry-case-panel]");
    const caseResults = document.querySelector("[data-manual-entry-case-results]");
    const caseCount = document.querySelector("[data-manual-entry-case-count]");
    const searchQ = document.querySelector("#manual-entry-subscriber-search-q");
    const searchKana = document.querySelector("#manual-entry-subscriber-search-kana");
    const searchSymbol = document.querySelector("#manual-entry-subscriber-search-symbol");
    const searchNumber = document.querySelector("#manual-entry-subscriber-search-number");
    const caseSearchFacility = document.querySelector("#manual-entry-case-search-facility");
    const caseSearchEvent = document.querySelector("#manual-entry-case-search-event");
    const caseSearchNameFull = document.querySelector("#manual-entry-case-search-name-full");
    const caseSearchKana = document.querySelector("#manual-entry-case-search-kana");
    const caseSearchHia = document.querySelector("#manual-entry-case-search-hia");
    const caseSearchSymbol = document.querySelector("#manual-entry-case-search-symbol");
    const caseSearchNumber = document.querySelector("#manual-entry-case-search-number");
    const caseSearchExamMonth = document.querySelector("#manual-entry-case-search-exam-month");
    const caseSearchQualificationLostDate = document.querySelector("#manual-entry-case-search-qualification-lost-date");
    const caseSearchQualificationLostStatus = document.querySelector("#manual-entry-case-search-qualification-lost-status");
    const caseSearchLimit = document.querySelector("#manual-entry-case-search-limit");
    let selectedSubscriber = null;
    let selectedManualCaseId = "";
    const manualPersonFloat = document.querySelector("[data-manual-entry-person-float]");
    const manualPersonFloatName = document.querySelector("[data-manual-entry-float-name]");
    const manualPersonFloatHia = document.querySelector("[data-manual-entry-float-hia]");
    const manualPersonFloatInsurance = document.querySelector("[data-manual-entry-float-insurance]");
    const manualPersonFloatVisit = document.querySelector("[data-manual-entry-float-visit]");
    const manualPersonFloatDraft = document.querySelector("[data-manual-entry-float-draft]");
    const manualEntryFilledCount = document.querySelector("[data-manual-entry-filled-count]");
    const manualEntryDraftSaveStatus = document.querySelector("[data-manual-entry-draft-save-status]");
    const manualEntryDraftSaveButton = document.querySelector("[data-manual-entry-draft-save]");
    let currentManualDraftId = "";
    let manualEntrySaveTimer = null;

    const setValue = (selector, value) => {
      const input = document.querySelector(selector);
      if (input) {
        input.value = value || "";
        input.dispatchEvent(new Event("input", { bubbles: true }));
      }
    };

    const updateManualPersonFloat = () => {
      if (!manualPersonFloat) return;
      const nameKana = document.querySelector("#manual-entry-name-kana-input")?.value?.trim() || "";
      const nameFull = document.querySelector("#manual-entry-name-full-input")?.value?.trim() || "";
      const hiaId = document.querySelector("#manual-entry-hia-subscriber-id-input")?.value?.trim() || "";
      const symbol = document.querySelector("#manual-entry-insurance-symbol-input")?.value?.trim() || "";
      const number = document.querySelector("#manual-entry-insurance-number-input")?.value?.trim() || "";
      const branch = document.querySelector("#manual-entry-insurance-branch-input")?.value?.trim() || "";
      const facilityCode = document.querySelector("#manual-entry-facility-input")?.value?.trim() || "";
      const facilityName = document.querySelector("#manual-entry-facility-name-input")?.value?.trim() || "";
      const facility = facilityName || facilityCode;
      const examDate = document.querySelector("#manual-entry-exam-date-input")?.value?.trim() || "";
      const displayName = nameKana || nameFull || "未選択";
      const insuranceText = [symbol, number].filter(Boolean).join("-");
      const branchText = branch ? `-${branch}` : "";
      if (manualPersonFloatName) manualPersonFloatName.textContent = displayName;
      if (manualPersonFloatDraft) manualPersonFloatDraft.textContent = currentManualDraftId ? `draft ${currentManualDraftId}` : "draft -";
      if (manualPersonFloatHia) manualPersonFloatHia.textContent = hiaId ? `HIA ${hiaId}` : "HIA -";
      if (manualPersonFloatInsurance) manualPersonFloatInsurance.textContent = insuranceText ? `記号番号 ${insuranceText}${branchText}` : "記号番号 -";
      if (manualPersonFloatVisit) manualPersonFloatVisit.textContent = facility || examDate ? `${facility || "施設-"} / ${examDate || "受診日-"}` : "施設/受診日 -";
      manualPersonFloat.classList.toggle("is-empty", displayName === "未選択" && !hiaId && !insuranceText && !facility && !examDate);
    };

    const collectManualEntryBasic = () => ({
      event_id: document.querySelector("select[name='event_id']")?.value || "2",
      entry_purpose: document.querySelector("select[name='entry_purpose']")?.value || "PAPER_ONLY",
      exam_export_case_id: selectedManualCaseId || "",
      subscriber_id: document.querySelector("#manual-entry-subscriber-id-input")?.value?.trim() || "",
      hia_subscriber_id: document.querySelector("#manual-entry-hia-subscriber-id-input")?.value?.trim() || "",
      insurer_number: document.querySelector("#manual-entry-insurer-number-input")?.value?.trim() || "",
      insurance_symbol: document.querySelector("#manual-entry-insurance-symbol-input")?.value?.trim() || "",
      insurance_number: document.querySelector("#manual-entry-insurance-number-input")?.value?.trim() || "",
      insurance_branch_number: document.querySelector("#manual-entry-insurance-branch-input")?.value?.trim() || "",
      postal_code: document.querySelector("#manual-entry-postal-code-input")?.value?.trim() || "",
      address: document.querySelector("#manual-entry-address-input")?.value?.trim() || "",
      name_full: document.querySelector("#manual-entry-name-full-input")?.value?.trim() || "",
      name_kana: document.querySelector("#manual-entry-name-kana-input")?.value?.trim() || "",
      birthdate: document.querySelector("#manual-entry-birthdate-input")?.value?.trim() || "",
      gender_code: document.querySelector("#manual-entry-gender-input")?.value?.trim() || "",
      facility_code: document.querySelector("#manual-entry-facility-input")?.value?.trim() || "",
      facility_name: document.querySelector("#manual-entry-facility-name-input")?.value?.trim() || "",
      facility_document_id: document.querySelector("input[name='facility_document_id']")?.value?.trim() || "",
      exam_date: document.querySelector("#manual-entry-exam-date-input")?.value?.trim() || "",
    });

    const collectManualEntryValues = () => {
      const values = [];
      for (const input of document.querySelectorAll(".manual-entry-value-input")) {
        const rawValue = input.value?.trim() || "";
        if (!rawValue) continue;
        const row = input.closest("[data-manual-entry-item-row]");
        if (!row) continue;
        const codeSelect = input.closest(".manual-entry-value-control")?.querySelector("[data-manual-code-select]");
        const selectedOption = codeSelect?.selectedOptions?.[0];
        const methodSelect = row.querySelector(".manual-entry-method-select");
        const methodOption = methodSelect?.selectedOptions?.[0];
        const valueType = (row.getAttribute("data-xml-value-type") || "").toUpperCase();
        const isCodeValue = valueType === "CD" || valueType === "CO";
        values.push({
          namecode: row.getAttribute("data-namecode") || "",
          namecode_display_name: row.getAttribute("data-item-name") || "",
          identity_item_code: row.getAttribute("data-identity-item-code") || "",
          identity_item_name: row.getAttribute("data-identity-item-name") || "",
          xml_value_type: valueType,
          raw_value: rawValue,
          normalized_value: isCodeValue ? "" : rawValue,
          code_system: row.getAttribute("data-code-system") || "",
          code_value: isCodeValue ? rawValue : "",
          code_display: selectedOption && selectedOption.value ? selectedOption.textContent?.trim() || "" : "",
          display_unit: row.getAttribute("data-display-unit") || "",
          ucum_unit: row.getAttribute("data-ucum-unit") || "",
          method_code: methodSelect?.value || row.getAttribute("data-method-code") || "",
          method_name: methodOption?.textContent?.trim() || row.getAttribute("data-method-name") || "",
          occurrence_no: 1,
        });
      }
      return values;
    };

    const manualTextByteLength = (value) => {
      return Array.from(String(value || "")).reduce((total, char) => total + (char.charCodeAt(0) <= 0x7f ? 1 : 2), 0);
    };

    const refreshManualTextLimit = (input) => {
      if (!input?.matches?.("[data-manual-text-limit]")) return null;
      const limit = Number(input.getAttribute("data-manual-text-limit") || "0");
      const length = manualTextByteLength(input.value || "");
      const meter = input.closest(".manual-entry-value-control")?.querySelector("[data-manual-text-meter]");
      const exceeded = limit > 0 && length > limit;
      input.classList.toggle("is-text-limit-exceeded", exceeded);
      if (meter) {
        meter.textContent = `${length} / ${limit} byte${exceeded ? " 超過" : ""}`;
        meter.classList.toggle("is-exceeded", exceeded);
      }
      return { length, limit, exceeded };
    };

    const refreshManualTextLimits = () => {
      let firstExceeded = null;
      let exceededCount = 0;
      for (const input of document.querySelectorAll("[data-manual-text-limit]")) {
        const result = refreshManualTextLimit(input);
        if (result?.exceeded) {
          exceededCount += 1;
          if (!firstExceeded) firstExceeded = input;
        }
      }
      return { exceededCount, firstExceeded };
    };

    const scrollToManualTextLimitInput = (input) => {
      if (!input) return;
      const category = input.closest("[data-manual-entry-category]");
      if (category && "open" in category) category.open = true;
      const row = input.closest("[data-manual-entry-item-row]");
      if (row) {
        row.classList.add("is-scroll-target");
        window.setTimeout(() => row.classList.remove("is-scroll-target"), 2400);
      }
      window.setTimeout(() => {
        (row || input).scrollIntoView({ behavior: "smooth", block: "center" });
        input.focus?.();
      }, 50);
    };

    refreshManualEntryFilledCount = () => {
      const count = collectManualEntryValues().length;
      if (manualEntryFilledCount) manualEntryFilledCount.textContent = String(count);
      for (const category of document.querySelectorAll("[data-manual-entry-category]")) {
        const categoryCount = Array.from(category.querySelectorAll(".manual-entry-value-input"))
          .filter((input) => String(input.value || "").trim() !== "").length;
        const categoryCountNode = category.querySelector("[data-manual-entry-category-filled-count]");
        if (categoryCountNode) categoryCountNode.textContent = String(categoryCount);
      }
      return count;
    };

    const setManualEntrySaveStatus = (text, className = "status-muted") => {
      if (!manualEntryDraftSaveStatus) return;
      manualEntryDraftSaveStatus.textContent = text;
      manualEntryDraftSaveStatus.className = `status-pill ${className}`;
    };

    const saveManualEntryDraft = async ({ silent = false, allowEmpty = false } = {}) => {
      const values = collectManualEntryValues();
      refreshManualEntryFilledCount();
      const textLimitState = refreshManualTextLimits();
      if (textLimitState.exceededCount > 0) {
        if (!silent) {
          setManualEntrySaveStatus(`ST文字数超過 ${textLimitState.exceededCount}件`, "status-danger");
          scrollToManualTextLimitInput(textLimitState.firstExceeded);
        }
        return;
      }
      if (!values.length && !allowEmpty) {
        if (!silent) setManualEntrySaveStatus("入力値なし", "status-warning");
        return;
      }
      const basic = collectManualEntryBasic();
      const basicHasValue = ["facility_code", "facility_name", "exam_date", "hia_subscriber_id", "insurance_symbol", "insurance_number", "postal_code", "address", "name_full", "name_kana"]
        .some((key) => String(basic[key] || "").trim() !== "");
      if (!basicHasValue) {
        if (!silent) setManualEntrySaveStatus("基本情報不足", "status-warning");
        return;
      }
      setManualEntrySaveStatus("保存中", "status-pending");
      if (manualEntryDraftSaveButton) manualEntryDraftSaveButton.disabled = true;
      try {
        const payload = await postJson("/api/manual-exam-entry-drafts/save", {
          draft_id: currentManualDraftId,
          basic,
          values,
        });
        currentManualDraftId = String(payload.draft_id || currentManualDraftId || "");
        setManualEntrySaveStatus(`保存済 ${payload.value_count || values.length}件`, "status-ready");
        updateManualPersonFloat();
      } catch (error) {
        setManualEntrySaveStatus("保存エラー", "status-danger");
        if (!silent) window.alert(`下書き保存でエラーが発生しました。${error?.message || ""}`);
      } finally {
        if (manualEntryDraftSaveButton) manualEntryDraftSaveButton.disabled = false;
      }
    };

    scheduleManualEntryDraftSave = () => {
      refreshManualEntryFilledCount();
      if (manualEntrySaveTimer) window.clearTimeout(manualEntrySaveTimer);
      manualEntrySaveTimer = window.setTimeout(() => {
        saveManualEntryDraft({ silent: true });
      }, 900);
    };

    const applyManualEntryDraftValues = (values) => {
      if (!Array.isArray(values) || !values.length) return;
      for (const value of values) {
        const rows = Array.from(document.querySelectorAll(`[data-manual-entry-item-row][data-namecode="${CSS.escape(String(value.namecode || ""))}"]`));
        const row = rows.find((candidate) => {
          const methodSelect = candidate.querySelector(".manual-entry-method-select");
          return !value.method_code || !methodSelect || methodSelect.value === value.method_code;
        }) || rows[0];
        if (!row) continue;
        const input = row.querySelector(".manual-entry-value-input");
        if (!input) continue;
        input.value = value.raw_value || value.normalized_value || value.code_value || "";
        input.dispatchEvent(new Event("input", { bubbles: true }));
        refreshManualCodeToggle(input);
      }
      refreshManualEntryFilledCount();
    };

    refreshManualCodeToggle = (input) => {
      const control = input?.closest(".manual-entry-value-control");
      const group = control?.querySelector("[data-manual-code-toggle-group]");
      if (!group) return;
      const currentValue = String(input.value || "").trim();
      for (const button of group.querySelectorAll("[data-manual-code-toggle]")) {
        const selected = button.value === currentValue;
        button.classList.toggle("is-selected", selected);
        button.setAttribute("aria-pressed", selected ? "true" : "false");
      }
    };

    for (const input of document.querySelectorAll(".manual-entry-form input, .manual-entry-form select, .manual-entry-form textarea")) {
      input.addEventListener("input", () => {
        refreshManualTextLimit(input);
        updateManualPersonFloat();
        refreshManualEntryFilledCount();
      });
      input.addEventListener("change", () => {
        refreshManualTextLimit(input);
        updateManualPersonFloat();
        refreshManualEntryFilledCount();
      });
    }
    refreshManualTextLimits();
    manualEntryDraftSaveButton?.addEventListener("click", () => saveManualEntryDraft({ silent: false }));

    const setManualSubscriberMessage = (message) => {
      manualSubscriberResults.innerHTML = `<tr><td colspan="4">${escapeHtml(message)}</td></tr>`;
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
      setValue("#manual-entry-insurer-number-input", person.insurer_number);
      setValue("#manual-entry-hia-subscriber-id-input", person.hia_subscriber_id);
      setValue("#manual-entry-name-full-input", person.name_full);
      setValue("#manual-entry-name-kana-input", person.name_kana);
      setValue("#manual-entry-insurance-symbol-input", person.insurance_symbol);
      setValue("#manual-entry-insurance-number-input", person.insurance_number);
      setValue("#manual-entry-insurance-branch-input", person.insurance_branch_number);
      setValue("#manual-entry-postal-code-input", person.postal_code || person.subscriber_postal_code);
      setValue("#manual-entry-address-input", person.address || person.subscriber_address_line);
      setValue("#manual-entry-birthdate-input", person.birth || person.birthdate);
      setValue("#manual-entry-gender-input", person.gender_label);
      updateManualPersonFloat();
    };

    for (const button of document.querySelectorAll("[data-manual-entry-basic-clear]")) {
      button.addEventListener("click", () => {
        if (!window.confirm("基本情報をクリアします。よろしいですか？")) return;
        for (const input of document.querySelectorAll(".manual-entry-form input, .manual-entry-form textarea")) {
          if (input.name === "event_id") continue;
          input.value = "";
          input.dispatchEvent(new Event("input", { bubbles: true }));
        }
        const purpose = document.querySelector("select[name='entry_purpose']");
        if (purpose) purpose.value = "PAPER_ONLY";
        selectedSubscriber = null;
        if (selectedCasePanel) selectedCasePanel.hidden = true;
        if (casePanel) casePanel.hidden = true;
        setManualSubscriberSelected(null);
        currentManualDraftId = "";
        setManualEntrySaveStatus("未保存", "status-muted");
        updateManualPersonFloat();
      });
    }

    for (const button of document.querySelectorAll("[data-manual-entry-values-clear]")) {
      button.addEventListener("click", () => {
        if (!window.confirm("検査項目の入力値をクリアします。よろしいですか？")) return;
        for (const input of document.querySelectorAll(".manual-entry-value-input")) {
          input.value = "";
          input.dispatchEvent(new Event("input", { bubbles: true }));
        }
        for (const select of document.querySelectorAll("[data-manual-code-select]")) {
          select.value = "";
        }
        for (const input of document.querySelectorAll(".manual-entry-value-input")) {
          refreshManualCodeToggle(input);
        }
        for (const checkbox of document.querySelectorAll("input[name^='include_']")) {
          checkbox.checked = false;
        }
        refreshManualEntryFilledCount();
        if (currentManualDraftId) {
          saveManualEntryDraft({ silent: true, allowEmpty: true });
        } else {
          setManualEntrySaveStatus("未保存", "status-muted");
        }
      });
    }

    const fillManualEntryFromCase = (item) => {
      fillManualEntryFromPerson(item);
      setValue("#manual-entry-facility-input", item.facility_code);
      setValue("#manual-entry-facility-name-input", item.facility_name);
      setValue("#manual-entry-case-search-facility", item.facility_code);
      setValue("input[name='facility_document_id']", item.facility_document_id);
      setValue("#manual-entry-exam-date-input", item.exam_date);
      const purpose = document.querySelector("select[name='entry_purpose']");
      if (purpose) purpose.value = "SUPPLEMENT";
      if (selectedCasePanel) selectedCasePanel.hidden = false;
      document.querySelector("#manual-entry-case-starter")?.classList.add("is-selected");
      selectedManualCaseId = String(item.exam_export_case_id || "");
      if (selectedCaseTitle) {
        selectedCaseTitle.textContent = `case ${item.exam_export_case_id || "-"} / ${item.name_kana || "-"}`;
      }
      if (selectedCaseDetail) {
        selectedCaseDetail.textContent = `${item.facility_name || "-"} / ${item.exam_date || "-"} / ${item.source_mode || "-"} / 出力 ${item.export_readiness_status || "-"}`;
      }
      if (selectedCaseDetailLink) {
        selectedCaseDetailLink.href = `/exam-export-cases/${encodeURIComponent(item.exam_export_case_id || "")}`;
        selectedCaseDetailLink.hidden = !item.exam_export_case_id;
      }
      updateManualPersonFloat();
      renderManualCaseRows([item]);
      if (casePanel) casePanel.classList.add("is-selected");
      document.querySelector("#manual-entry-case-picker-modal [data-modal-close]")?.click();
      document.querySelector("#manual-entry-basic")?.scrollIntoView({ behavior: "smooth", block: "start" });
    };

    const setManualCaseMessage = (message, label = "未検索") => {
      if (casePanel) casePanel.hidden = false;
      if (caseResults) {
        caseResults.innerHTML = `<tr><td colspan="4">${escapeHtml(message)}</td></tr>`;
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
        caseResults.innerHTML = `<tr><td colspan="4">一致するcaseはありません。紙のみ新規sourceとして作成する場合は、基本情報を入力してください。</td></tr>`;
        return;
      }
      caseResults.innerHTML = "";
      for (const item of items) {
        const sourceText = `XML ${item.xml_count || 0} / CSV ${item.csv_count || 0} / 紙 ${item.paper_count || 0}`;
        const legal = `${item.legal_check_result || "PENDING"}${item.legal_reason_summary ? ` / ${item.legal_reason_summary}` : ""}`;
        const specific = `${item.specific_check_result || "PENDING"}${item.specific_reason_summary ? ` / ${item.specific_reason_summary}` : ""}`;
        const row = document.createElement("tr");
        const caseId = String(item.exam_export_case_id || "");
        const isSelected = selectedManualCaseId !== "" && selectedManualCaseId === caseId;
        row.setAttribute("data-manual-entry-case-row", "true");
        row.setAttribute("data-case-id", caseId);
        row.classList.toggle("is-selected", isSelected);
        row.innerHTML = `
          <td>
            <div class="manual-entry-case-actions">
              <button type="button" class="ghost-button compact-action-button ${isSelected ? "is-active" : ""}" data-manual-entry-case-apply>${isSelected ? "使用中" : "使う"}</button>
              <a class="ghost-button compact-action-button" href="/exam-export-cases/${encodeURIComponent(item.exam_export_case_id)}">詳細</a>
            </div>
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
        `;
        row.querySelector("[data-manual-entry-case-apply]")?.addEventListener("click", () => {
          fillManualEntryFromCase(item);
        });
        caseResults.appendChild(row);
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
        const payload = await fetchJson(`/api/manual-exam-entry/cases?${params.toString()}`);
        renderManualCaseRows(Array.isArray(payload.items) ? payload.items : []);
      } catch (error) {
        setManualCaseMessage(`case確認でエラーが発生しました。${error?.message || ""}`, "エラー");
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
        const payload = await fetchJson(`/api/manual-exam-entry/subscribers?${params.toString()}`);
        renderManualSubscriberRows(Array.isArray(payload.items) ? payload.items : []);
      } catch (error) {
        setManualSubscriberMessage(`検索でエラーが発生しました。${error?.message || ""}`);
        setManualSubscriberSelected(null);
      }
    };

    const searchManualCases = async () => {
      const eventId = caseSearchEvent?.value || document.querySelector("select[name='event_id']")?.value || "2";
      const params = new URLSearchParams({
        event_id: eventId,
        facility_codes: caseSearchFacility ? caseSearchFacility.value.trim() : "",
        exam_month: caseSearchExamMonth ? caseSearchExamMonth.value.trim() : "",
        name_full: caseSearchNameFull ? caseSearchNameFull.value.trim() : "",
        name_kana: caseSearchKana ? caseSearchKana.value.trim() : "",
        hia_subscriber_id: caseSearchHia ? caseSearchHia.value.trim() : "",
        insurance_symbol: caseSearchSymbol ? caseSearchSymbol.value.trim() : "",
        insurance_number: caseSearchNumber ? caseSearchNumber.value.trim() : "",
        qualification_lost_status: caseSearchQualificationLostStatus ? caseSearchQualificationLostStatus.value.trim() : "",
        qualification_lost_date: caseSearchQualificationLostDate ? caseSearchQualificationLostDate.value.trim() : "",
        limit: caseSearchLimit ? caseSearchLimit.value.trim() : "50",
      });
      if (![...params.values()].some((value) => value && value !== eventId)) {
        setManualCaseMessage("検索条件を入力してください。", "未検索");
        return;
      }
      setManualCaseMessage("caseを検索中...", "検索中");
      if (casePanel) casePanel.classList.remove("is-selected");
      document.querySelector("#manual-entry-case-picker-modal [data-modal-close]")?.click();
      try {
        const payload = await fetchJson(`/api/manual-exam-entry/case-candidates?${params.toString()}`);
        renderManualCaseRows(Array.isArray(payload.items) ? payload.items : []);
        document.querySelector("#manual-entry-case-context")?.scrollIntoView({ behavior: "smooth", block: "start" });
      } catch (error) {
        setManualCaseMessage(`case検索でエラーが発生しました。${error?.message || ""}`, "エラー");
      }
    };

    searchButton?.addEventListener("click", searchManualSubscribers);
    caseSearchButton?.addEventListener("click", searchManualCases);
    for (const button of document.querySelectorAll("[data-manual-entry-case-toggle]")) {
      button.addEventListener("click", () => {
        const target = button.getAttribute("data-manual-entry-case-toggle");
        const value = button.getAttribute("data-filter-value") || "";
        if (!target || target !== "qualification_lost_status" || !caseSearchQualificationLostStatus) return;
        const nextValue = caseSearchQualificationLostStatus.value === value ? "" : value;
        caseSearchQualificationLostStatus.value = nextValue;
        for (const peer of document.querySelectorAll(`[data-manual-entry-case-toggle="${target}"]`)) {
          peer.classList.toggle("is-active", nextValue !== "" && peer.getAttribute("data-filter-value") === nextValue);
        }
      });
    }
    for (const input of [searchQ, searchKana, searchSymbol, searchNumber]) {
      input?.addEventListener("keydown", (event) => {
        if (event.key === "Enter") {
          event.preventDefault();
          searchManualSubscribers();
        }
      });
    }
    for (const input of [
      caseSearchFacility,
      caseSearchNameFull,
      caseSearchKana,
      caseSearchHia,
      caseSearchSymbol,
      caseSearchNumber,
      caseSearchExamMonth,
      caseSearchQualificationLostDate,
      caseSearchLimit,
    ]) {
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
    const applyManualEntryInitialParams = () => {
      const params = new URLSearchParams(window.location.search);
      const mode = params.get("manual_new") || "";
      if (!mode) return;
      const person = {
        subscriber_id: params.get("subscriber_id") || "",
        hia_subscriber_id: params.get("hia_subscriber_id") || "",
        insurer_number: params.get("insurer_number") || "",
        name_full: params.get("name_full") || "",
        name_kana: params.get("name_kana") || "",
        insurance_symbol: params.get("insurance_symbol") || "",
        insurance_number: params.get("insurance_number") || "",
        insurance_branch_number: params.get("insurance_branch_number") || "",
        postal_code: params.get("postal_code") || "",
        address: params.get("address") || "",
        birth: params.get("birthdate") || "",
        gender_label: params.get("gender_label") || "",
      };
      const eventId = params.get("event_id") || "";
      if (eventId) setValue("select[name='event_id']", eventId);
      if (mode === "case") {
        fillManualEntryFromCase({
          ...person,
          exam_export_case_id: params.get("case_id") || "",
          facility_code: params.get("facility_code") || "",
          facility_name: params.get("facility_name") || "",
          exam_date: params.get("exam_date") || "",
          source_mode: params.get("source_mode") || "",
          export_readiness_status: params.get("export_readiness_status") || "",
          legal_check_result: params.get("legal_check_result") || "",
          legal_reason_summary: params.get("legal_reason_summary") || "",
          specific_check_result: params.get("specific_check_result") || "",
          specific_reason_summary: params.get("specific_reason_summary") || "",
          source_count: params.get("source_count") || "",
          xml_count: params.get("xml_count") || "",
          csv_count: params.get("csv_count") || "",
          paper_count: params.get("paper_count") || "",
          case_value_count: params.get("case_value_count") || "",
        });
        return;
      }
      fillManualEntryFromPerson(person);
      const purpose = document.querySelector("select[name='entry_purpose']");
      if (purpose) purpose.value = "PAPER_ONLY";
      document.querySelector("#manual-entry-basic")?.scrollIntoView({ behavior: "smooth", block: "start" });
    };
    applyManualEntryInitialParams();
    const applyManualEntryInitialDraft = () => {
      const node = document.querySelector("#manual-entry-initial-draft-json");
      if (!node || !node.textContent.trim()) return;
      let draft = null;
      try {
        draft = JSON.parse(node.textContent);
      } catch (_error) {
        draft = null;
      }
      if (!draft || !draft.manual_exam_entry_draft_id) return;
      currentManualDraftId = String(draft.manual_exam_entry_draft_id || "");
      if (draft.event_id) setValue("select[name='event_id']", draft.event_id);
      const person = {
        subscriber_id: draft.subscriber_id || "",
        hia_subscriber_id: draft.hia_subscriber_id || "",
        insurer_number: draft.insurer_number || "",
        name_full: draft.name_full || "",
        name_kana: draft.name_kana || "",
        insurance_symbol: draft.insurance_symbol || "",
        insurance_number: draft.insurance_number || "",
        insurance_branch_number: draft.insurance_branch_number || "",
        postal_code: draft.postal_code || "",
        address: draft.address || "",
        birth: draft.birthdate || "",
        gender_label: draft.gender_code || "",
      };
      if (draft.exam_export_case_id) {
        fillManualEntryFromCase({
          ...person,
          exam_export_case_id: draft.exam_export_case_id,
          facility_code: draft.facility_code || "",
          facility_name: draft.facility_name || "",
          facility_document_id: draft.facility_document_id || "",
          exam_date: draft.exam_date || "",
          source_mode: "MANUAL_DRAFT",
          export_readiness_status: draft.draft_status || "DRAFT",
        });
      } else {
        fillManualEntryFromPerson(person);
        setValue("#manual-entry-facility-input", draft.facility_code || "");
        setValue("#manual-entry-facility-name-input", draft.facility_name || "");
        setValue("input[name='facility_document_id']", draft.facility_document_id || "");
        setValue("#manual-entry-exam-date-input", draft.exam_date || "");
      }
      const purpose = document.querySelector("select[name='entry_purpose']");
      if (purpose && draft.entry_purpose) purpose.value = draft.entry_purpose;
      applyManualEntryDraftValues(draft.values || []);
      setManualEntrySaveStatus("下書き読込", "status-ready");
      updateManualPersonFloat();
    };
    applyManualEntryInitialDraft();
    updateManualPersonFloat();
    refreshManualEntryFilledCount();
  }

  const manualDraftPersonResults = document.querySelector("[data-manual-draft-person-results]");
  if (manualDraftPersonResults) {
    const draftNewToggle = document.querySelector("[data-manual-draft-new-toggle]");
    const draftNewToggleArea = document.querySelector("[data-manual-draft-new-toggle-area]");
    const draftNewActions = document.querySelector("[data-manual-draft-new-actions]");
    const draftPersonEvent = document.querySelector("#manual-draft-person-event");
    const draftPersonQ = document.querySelector("#manual-draft-person-q");
    const draftPersonKana = document.querySelector("#manual-draft-person-kana");
    const draftPersonSymbol = document.querySelector("#manual-draft-person-symbol");
    const draftPersonNumber = document.querySelector("#manual-draft-person-number");
    const draftCaseEvent = document.querySelector("#manual-draft-case-event");
    const draftCaseFacility = document.querySelector("#manual-draft-case-facility");
    const draftCaseFacilitySummary = document.querySelector("#manual-draft-case-facility-summary");
    const draftCaseSymbol = document.querySelector("#manual-draft-case-symbol");
    const draftCaseNumber = document.querySelector("#manual-draft-case-number");
    const draftCaseKana = document.querySelector("#manual-draft-case-kana");
    const draftCaseNameFull = document.querySelector("#manual-draft-case-name-full");
    const draftCaseHia = document.querySelector("#manual-draft-case-hia");
    const draftCaseExamMonth = document.querySelector("#manual-draft-case-exam-month");
    const draftCaseLimit = document.querySelector("#manual-draft-case-limit");
    const draftCaseResults = document.querySelector("[data-manual-draft-case-results]");
    const draftFacilityTarget = document.querySelector("[data-manual-draft-facility-target]");
    let activeDraftFacilityTarget = null;
    const draftDeleteModal = document.querySelector("#manual-draft-delete-confirm-modal");
    const draftDeleteLabel = document.querySelector("[data-manual-draft-delete-label]");
    const draftDeleteConfirmButton = document.querySelector("[data-manual-draft-delete-confirm]");
    let activeDraftDeleteButton = null;
    const draftApplyModal = document.querySelector("#manual-draft-apply-confirm-modal");
    const draftApplyLabel = document.querySelector("[data-manual-draft-apply-label]");
    const draftApplyConfirmButton = document.querySelector("[data-manual-draft-apply-confirm]");
    let activeDraftApplyButton = null;
    const draftCheckDetailModal = document.querySelector("#manual-draft-check-detail-modal");
    const draftCheckDetailMain = document.querySelector("[data-manual-draft-check-detail-main]");
    const draftCheckDetailMeta = document.querySelector("[data-manual-draft-check-detail-meta]");
    const draftCheckDetailBody = document.querySelector("[data-manual-draft-check-detail-body]");
    const draftCaseMatchModal = document.querySelector("#manual-draft-case-match-modal");
    const draftCaseMatchMessage = document.querySelector("[data-manual-draft-case-match-message]");
    const draftCaseMatchCriteria = document.querySelector("[data-manual-draft-case-match-criteria]");
    const draftCaseMatchResults = document.querySelector("[data-manual-draft-case-match-results]");
    const draftCaseMatchNearby = document.querySelector("[data-manual-draft-case-match-nearby]");

    const toggleManualDraftNewActions = () => {
      if (!draftNewActions) return;
      const expanded = draftNewToggleArea?.getAttribute("aria-expanded") === "true" || draftNewToggle?.getAttribute("aria-expanded") === "true";
      draftNewActions.hidden = expanded;
      const nextExpanded = expanded ? "false" : "true";
      draftNewToggleArea?.setAttribute("aria-expanded", nextExpanded);
      draftNewToggle?.setAttribute("aria-expanded", nextExpanded);
      if (draftNewToggle) draftNewToggle.textContent = expanded ? "開く" : "畳む";
    };

    draftNewToggleArea?.addEventListener("click", toggleManualDraftNewActions);
    draftNewToggleArea?.addEventListener("keydown", (event) => {
      if (event.key !== "Enter" && event.key !== " ") return;
      event.preventDefault();
      toggleManualDraftNewActions();
    });

    const reloadDraftListWithMessage = (message) => {
      const query = new URLSearchParams(window.location.search);
      if (message) query.set("message", message);
      window.location.href = `/manual-exam-entry-drafts${query.toString() ? `?${query.toString()}` : ""}`;
    };

    const setActiveDraftFacilityTarget = (button) => {
      if (!button) {
        activeDraftFacilityTarget = null;
        return;
      }
      activeDraftFacilityTarget = {
        draftId: button.getAttribute("data-draft-id") || "",
        eventId: button.getAttribute("data-event-id") || "2",
        facilityCode: button.getAttribute("data-facility-code") || "",
        facilityName: button.getAttribute("data-facility-name") || "",
        examDate: button.getAttribute("data-exam-date") || "",
      };
      if (draftFacilityTarget) {
        const current = activeDraftFacilityTarget.facilityName || activeDraftFacilityTarget.facilityCode || "未設定";
        draftFacilityTarget.textContent = `draft ${activeDraftFacilityTarget.draftId} の健診機関を変更します。現在: ${current}`;
      }
      for (const row of document.querySelectorAll("[data-manual-draft-facility-row]")) {
        const code = row.getAttribute("data-facility-code") || "";
        row.classList.toggle("is-selected", Boolean(code && code === activeDraftFacilityTarget.facilityCode));
      }
      for (const selectButton of document.querySelectorAll("[data-manual-draft-facility-select]")) {
        const code = selectButton.getAttribute("data-facility-code") || "";
        const selected = code && code === activeDraftFacilityTarget.facilityCode;
        selectButton.textContent = selected ? "選択中" : "選択";
        selectButton.classList.toggle("is-active", Boolean(selected));
      }
    };

    const updateDraftBasicInfo = async (target, { facilityCode = null, facilityName = null, examDate = null } = {}) => {
      if (!target?.draftId) return;
      const nextFacilityCode = facilityCode ?? target.facilityCode;
      const nextFacilityName = facilityName ?? target.facilityName;
      const nextExamDate = examDate ?? target.examDate ?? "";
      try {
        const payload = await postJson(`/api/manual-exam-entry-drafts/${encodeURIComponent(target.draftId)}/basic-info`, {
          event_id: target.eventId,
          facility_code: nextFacilityCode,
          facility_name: nextFacilityName,
          exam_date: nextExamDate,
        });
        reloadDraftListWithMessage(payload.message || "基本情報を更新しました。");
      } catch (error) {
        if (draftFacilityTarget) {
          draftFacilityTarget.textContent = `基本情報更新でエラーが発生しました。${error?.message || ""}`;
        }
      }
    };

    const updateDraftFacility = async (element) => {
      if (!(element instanceof Element)) return;
      const facilityCode = element.getAttribute("data-facility-code") || "";
      const facilityName = element.getAttribute("data-facility-name") || facilityCode;
      if (!facilityCode) return;
      await updateDraftBasicInfo(activeDraftFacilityTarget, { facilityCode, facilityName });
    };

    const draftTargetFromElement = (element) => ({
      draftId: element.getAttribute("data-draft-id") || "",
      eventId: element.getAttribute("data-event-id") || "2",
      facilityCode: element.getAttribute("data-facility-code") || "",
      facilityName: element.getAttribute("data-facility-name") || "",
      examDate: element.getAttribute("data-exam-date") || "",
    });

    const toggleDraftDateEditor = (button) => {
      const container = button.closest(".manual-draft-date-edit");
      const input = container?.querySelector("[data-manual-draft-date-input]");
      if (!input) return;
      input.focus();
      if (typeof input.showPicker === "function") {
        try {
          input.showPicker();
        } catch (_error) {
          // Safari may block programmatic picker opening; focus still leaves it editable.
        }
      }
    };

    const updateDraftDateFromInput = async (input) => {
      const container = input.closest(".manual-draft-date-edit");
      const toggle = container?.querySelector("[data-manual-draft-date-toggle]");
      if (!(toggle instanceof Element)) return;
      const nextDate = input.value?.trim() || "";
      if (nextDate === (toggle.getAttribute("data-exam-date") || "")) return;
      await updateDraftBasicInfo(draftTargetFromElement(toggle), { examDate: nextDate });
    };

    const openManualDraftDeleteModal = (button) => {
      const draftId = button.getAttribute("data-draft-id") || "";
      const label = button.getAttribute("data-draft-label") || `draft ${draftId}`;
      if (!draftId) return;
      activeDraftDeleteButton = button;
      if (draftDeleteLabel) draftDeleteLabel.textContent = `${label} / draft ${draftId}`;
      if (draftDeleteConfirmButton) {
        draftDeleteConfirmButton.disabled = false;
        draftDeleteConfirmButton.textContent = "削除する";
      }
      if (draftDeleteModal) {
        draftDeleteModal.hidden = false;
        document.body.classList.add("has-open-modal");
        draftDeleteConfirmButton?.focus();
      }
    };

    const deleteManualDraft = async () => {
      const button = activeDraftDeleteButton;
      if (!button) return;
      const draftId = button.getAttribute("data-draft-id") || "";
      if (!draftId) return;
      const originalText = button.textContent || "削除";
      button.disabled = true;
      button.textContent = "削除中";
      if (draftDeleteConfirmButton) {
        draftDeleteConfirmButton.disabled = true;
        draftDeleteConfirmButton.textContent = "削除中";
      }
      try {
        const payload = await postJson(`/api/manual-exam-entry-drafts/${encodeURIComponent(draftId)}/delete`, {});
        reloadDraftListWithMessage(payload.message || "仮登録を削除しました。");
      } catch (error) {
        button.disabled = false;
        button.textContent = originalText;
        if (draftDeleteConfirmButton) {
          draftDeleteConfirmButton.disabled = false;
          draftDeleteConfirmButton.textContent = "削除する";
        }
        window.alert(`仮登録削除でエラーが発生しました。${error?.message || ""}`);
      }
    };

    const openManualDraftApplyModal = (button) => {
      const draftId = button.getAttribute("data-draft-id") || "";
      const label = button.getAttribute("data-draft-label") || `draft ${draftId}`;
      const valueCount = button.getAttribute("data-value-count") || "0";
      if (!draftId) return;
      activeDraftApplyButton = button;
      if (draftApplyLabel) draftApplyLabel.textContent = `${label} / draft ${draftId} / 入力値 ${valueCount}件`;
      if (draftApplyConfirmButton) {
        draftApplyConfirmButton.disabled = false;
        draftApplyConfirmButton.textContent = "本データ反映する";
      }
      if (draftApplyModal) {
        draftApplyModal.hidden = false;
        document.body.classList.add("has-open-modal");
        draftApplyConfirmButton?.focus();
      }
    };

    const applyManualDraft = async () => {
      const button = activeDraftApplyButton;
      if (!button) return;
      const draftId = button.getAttribute("data-draft-id") || "";
      if (!draftId) return;
      const originalText = button.textContent || "本データ反映";
      button.disabled = true;
      button.textContent = "反映中";
      if (draftApplyConfirmButton) {
        draftApplyConfirmButton.disabled = true;
        draftApplyConfirmButton.textContent = "反映中";
      }
      try {
        const payload = await postJson(`/api/manual-exam-entry-drafts/${encodeURIComponent(draftId)}/apply`, {});
        reloadDraftListWithMessage(payload.message || "本データ反映しました。");
      } catch (error) {
        button.disabled = false;
        button.textContent = originalText;
        if (draftApplyConfirmButton) {
          draftApplyConfirmButton.disabled = false;
          draftApplyConfirmButton.textContent = "本データ反映する";
        }
        window.alert(`本データ反映でエラーが発生しました。${error?.message || ""}`);
      }
    };

    const checkManualDraft = async (button) => {
      const draftId = button?.getAttribute("data-draft-id") || "";
      if (!draftId) return;
      const originalText = button.textContent || "参考チェック";
      button.disabled = true;
      button.textContent = "確認中";
      try {
        const payload = await postJson(`/api/manual-exam-entry-drafts/${encodeURIComponent(draftId)}/check`, {});
        reloadDraftListWithMessage(payload.message || "参考チェックを実行しました。");
      } catch (error) {
        button.disabled = false;
        button.textContent = originalText;
        window.alert(`参考チェックでエラーが発生しました。${error?.message || ""}`);
      }
    };

    const checkVisibleManualDrafts = async (button) => {
      const rows = Array.from(document.querySelectorAll("[data-manual-draft-list-row]"));
      const draftIds = rows
        .filter((row) => Number(row.getAttribute("data-value-count") || "0") > 0)
        .map((row) => Number(row.getAttribute("data-draft-id") || "0"))
        .filter(Boolean);
      if (!draftIds.length) {
        window.alert("参考チェック対象がありません。入力値のある仮登録が対象です。");
        return;
      }
      const originalText = button?.textContent || "表示中を参考チェック";
      if (button) {
        button.disabled = true;
        button.textContent = "確認中";
      }
      try {
        const payload = await postJson("/api/manual-exam-entry-drafts/check", { draft_ids: draftIds });
        reloadDraftListWithMessage(payload.message || "表示中の参考チェックを実行しました。");
      } catch (error) {
        if (button) {
          button.disabled = false;
          button.textContent = originalText;
        }
        window.alert(`参考チェックでエラーが発生しました。${error?.message || ""}`);
      }
    };

    const manualDraftCheckStatusLabel = (status) => {
      const labels = {
        OK: "OK",
        NG: "NG",
        STALE: "要再チェック",
        UNDETERMINABLE: "判定不能",
        UNCHECKED: "未チェック",
      };
      return labels[status] || status || "-";
    };

    const manualDraftCheckStatusClass = (status) => {
      if (["OK", "CALCULATED", "ALTERNATIVE", "NOT_APPLICABLE"].includes(status)) return "status-ready";
      if (["NG", "MISSING", "INVALID"].includes(status)) return "status-danger";
      if (["STALE", "UNDETERMINABLE", "PENDING"].includes(status)) return "status-pending";
      return "status-muted";
    };

    const openManualDraftCheckDetail = (button) => {
      const draftId = button?.getAttribute("data-draft-id") || "";
      const jsonElement = Array.from(document.querySelectorAll("[data-manual-draft-check-detail-json]"))
        .find((element) => element.getAttribute("data-manual-draft-check-detail-json") === draftId);
      if (!jsonElement) return;
      let detail;
      try {
        detail = JSON.parse(jsonElement.textContent || "{}");
      } catch (_error) {
        detail = {};
      }
      if (draftCheckDetailMain) {
        draftCheckDetailMain.textContent = `draft ${detail.draft_id || draftId}`;
      }
      if (draftCheckDetailMeta) {
        draftCheckDetailMeta.textContent = ` / ${detail.checked_by || "-"} / ${detail.checked_at || "-"}`;
      }
      if (draftCheckDetailBody) {
        const rows = Array.isArray(detail.details) ? detail.details : [];
        if (!rows.length) {
          draftCheckDetailBody.innerHTML = '<tr><td colspan="5">詳細はありません。</td></tr>';
        } else {
          draftCheckDetailBody.innerHTML = rows.map((row) => {
            const rowStatus = row.status || "-";
            return `
              <tr>
                <td>${escapeHtml(row.scope || "-")}</td>
                <td><strong>${escapeHtml(row.detail_no || "-")}</strong></td>
                <td>${escapeHtml(row.name || "-")}</td>
                <td><span class="status-pill ${manualDraftCheckStatusClass(rowStatus)}">${escapeHtml(rowStatus)}</span></td>
                <td>${escapeHtml(row.reason || "")}</td>
              </tr>
            `;
          }).join("");
        }
      }
      if (draftCheckDetailModal) {
        draftCheckDetailModal.hidden = false;
        document.body.classList.add("has-open-modal");
      }
    };

    const caseMatchStatusClass = (status) => {
      if (status === "MATCHED") return "status-ready";
      if (status === "MISSING_KEYS" || status === "NO_MATCH") return "status-danger";
      return "status-muted";
    };

    const caseLink = (caseId) => {
      if (!caseId) return "-";
      return `<a class="text-link-button" href="/exam-export-cases/${encodeURIComponent(caseId)}">${escapeHtml(caseId)}</a>`;
    };

    const renderManualDraftCaseMatch = (payload) => {
      if (draftCaseMatchMessage) {
        draftCaseMatchMessage.innerHTML = `
          <span class="status-pill ${caseMatchStatusClass(payload.status)}">${escapeHtml(payload.status || "-")}</span>
          ${escapeHtml(payload.message || "")}
        `;
      }
      const criteria = Array.isArray(payload.criteria) ? payload.criteria : [];
      if (draftCaseMatchCriteria) {
        draftCaseMatchCriteria.innerHTML = criteria.length
          ? criteria.map((item) => {
            const hasValue = String(item.value ?? "").trim() !== "";
            return `
              <tr>
                <td><strong>${escapeHtml(item.label || item.key || "-")}</strong>${item.note ? `<small>${escapeHtml(item.note)}</small>` : ""}</td>
                <td>${escapeHtml(item.value ?? "-")}</td>
                <td><span class="status-pill ${hasValue ? "status-ready" : "status-danger"}">${hasValue ? "あり" : "不足"}</span></td>
              </tr>
            `;
          }).join("")
          : '<tr><td colspan="3">確認条件がありません。</td></tr>';
      }
      const matches = Array.isArray(payload.matches) ? payload.matches : [];
      if (draftCaseMatchResults) {
        draftCaseMatchResults.innerHTML = matches.length
          ? matches.map((item) => `
            <tr>
              <td>${caseLink(item.exam_export_case_id)}</td>
              <td>${escapeHtml(item.exam_date || "-")}</td>
              <td>${escapeHtml(item.facility_name || item.facility_code || item.exam_facility_id || "-")}</td>
              <td>${escapeHtml(item.insurer_number || "-")}</td>
            </tr>
          `).join("")
          : '<tr><td colspan="4">完全一致するcaseはありません。</td></tr>';
      }
      const nearby = Array.isArray(payload.nearby_cases) ? payload.nearby_cases : [];
      if (draftCaseMatchNearby) {
        draftCaseMatchNearby.innerHTML = nearby.length
          ? nearby.map((item) => {
            const mismatches = Array.isArray(item.mismatches) && item.mismatches.length ? item.mismatches.join(" / ") : "完全一致";
            const statusClass = mismatches === "完全一致" ? "status-ready" : "status-pending";
            return `
              <tr>
                <td>${caseLink(item.exam_export_case_id)}</td>
                <td>${escapeHtml(item.exam_date || "-")}</td>
                <td>${escapeHtml(item.facility_name || item.facility_code || item.exam_facility_id || "-")}</td>
                <td>${escapeHtml(item.insurer_number || "-")}</td>
                <td><span class="status-pill ${statusClass}">${escapeHtml(mismatches)}</span></td>
              </tr>
            `;
          }).join("")
          : '<tr><td colspan="5">同じevent/subscriberのcaseはありません。</td></tr>';
      }
    };

    const openManualDraftCaseMatch = async (button) => {
      const draftId = button?.getAttribute("data-draft-id") || "";
      if (!draftId) return;
      const originalText = button.textContent || "caseチェック";
      button.disabled = true;
      button.textContent = "確認中";
      if (draftCaseMatchMessage) draftCaseMatchMessage.textContent = `draft ${draftId} を確認中です。`;
      if (draftCaseMatchCriteria) draftCaseMatchCriteria.innerHTML = '<tr><td colspan="3">確認中...</td></tr>';
      if (draftCaseMatchResults) draftCaseMatchResults.innerHTML = '<tr><td colspan="4">確認中...</td></tr>';
      if (draftCaseMatchNearby) draftCaseMatchNearby.innerHTML = '<tr><td colspan="5">確認中...</td></tr>';
      if (draftCaseMatchModal) {
        draftCaseMatchModal.hidden = false;
        document.body.classList.add("has-open-modal");
      }
      try {
        const payload = await fetchJson(`/api/manual-exam-entry-drafts/${encodeURIComponent(draftId)}/case-match`);
        renderManualDraftCaseMatch(payload);
      } catch (error) {
        if (draftCaseMatchMessage) {
          draftCaseMatchMessage.innerHTML = `<span class="status-pill status-danger">ERROR</span> ${escapeHtml(error?.message || "")}`;
        }
      } finally {
        button.disabled = false;
        button.textContent = originalText;
      }
    };

    const createManualDraftFromPerson = async (item, button) => {
      const originalText = button?.textContent || "";
      if (button) {
        button.disabled = true;
        button.textContent = "作成中";
      }
      try {
        const payload = await postJson("/api/manual-exam-entry-drafts/from-person", {
          event_id: draftPersonEvent?.value || "2",
          person: item,
        });
        reloadDraftListWithMessage(payload.message || "仮登録を作成しました。");
      } catch (error) {
        if (button) {
          button.disabled = false;
          button.textContent = originalText || "仮登録に追加";
        }
        setDraftPersonMessage(`仮登録作成でエラーが発生しました。${error?.message || ""}`);
      }
    };

    const createManualDraftFromCase = async (item, button) => {
      const originalText = button?.textContent || "";
      if (button) {
        button.disabled = true;
        button.textContent = "作成中";
      }
      try {
        const payload = await postJson("/api/manual-exam-entry-drafts/from-case", {
          event_id: item.event_id || draftCaseEvent?.value || "2",
          case: item,
        });
        reloadDraftListWithMessage(payload.message || "仮登録を作成しました。");
      } catch (error) {
        if (button) {
          button.disabled = false;
          button.textContent = originalText || "仮登録に追加";
        }
        setDraftCaseMessage(`仮登録作成でエラーが発生しました。${error?.message || ""}`);
      }
    };

    for (const button of document.querySelectorAll("[data-manual-draft-facility-change]")) {
      button.addEventListener("click", () => setActiveDraftFacilityTarget(button));
    }
    for (const row of document.querySelectorAll("[data-manual-draft-facility-row]")) {
      row.addEventListener("click", () => updateDraftFacility(row));
    }
    for (const button of document.querySelectorAll("[data-manual-draft-facility-select]")) {
      button.addEventListener("click", (event) => {
        event.stopPropagation();
        updateDraftFacility(button);
      });
    }
    for (const button of document.querySelectorAll("[data-manual-draft-date-toggle]")) {
      button.addEventListener("click", () => toggleDraftDateEditor(button));
    }
    for (const input of document.querySelectorAll("[data-manual-draft-date-input]")) {
      input.addEventListener("change", () => {
        updateDraftDateFromInput(input);
      });
    }
    for (const button of document.querySelectorAll("[data-manual-draft-delete]")) {
      button.addEventListener("click", () => openManualDraftDeleteModal(button));
    }
    draftDeleteConfirmButton?.addEventListener("click", deleteManualDraft);
    for (const button of document.querySelectorAll("[data-manual-draft-apply]")) {
      button.addEventListener("click", () => openManualDraftApplyModal(button));
    }
    draftApplyConfirmButton?.addEventListener("click", applyManualDraft);
    for (const button of document.querySelectorAll("[data-manual-draft-check]")) {
      button.addEventListener("click", () => checkManualDraft(button));
    }
    document.querySelector("[data-manual-draft-check-visible]")?.addEventListener("click", (event) => {
      checkVisibleManualDrafts(event.currentTarget);
    });
    for (const button of document.querySelectorAll("[data-manual-draft-check-detail]")) {
      button.addEventListener("click", () => openManualDraftCheckDetail(button));
    }
    for (const button of document.querySelectorAll("[data-manual-draft-case-match]")) {
      button.addEventListener("click", () => openManualDraftCaseMatch(button));
    }

    const setDraftPersonMessage = (message) => {
      manualDraftPersonResults.innerHTML = `<tr><td colspan="4">${escapeHtml(message)}</td></tr>`;
    };

    const setDraftCaseMessage = (message) => {
      if (draftCaseResults) {
        draftCaseResults.innerHTML = `<tr><td colspan="5">${escapeHtml(message)}</td></tr>`;
      }
    };

    const renderDraftPersonRows = (items) => {
      if (!items.length) {
        setDraftPersonMessage("一致する加入者はいません。");
        return;
      }
      manualDraftPersonResults.innerHTML = "";
      for (const item of items) {
        const row = document.createElement("tr");
        const insurance = `${item.insurance_symbol || "-"}-${item.insurance_number || "-"} 枝番 ${item.insurance_branch_number || "-"}`;
        const hia = `HIA ${item.hia_subscriber_id || "-"} / subscriber ${item.subscriber_id || "-"}`;
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
            <small>case ${escapeHtml(item.candidate_case_count || "0")}件</small>
          </td>
          <td><button type="button" class="primary-button compact-action-button" data-manual-draft-person-start>仮登録に追加</button></td>
        `;
        row.addEventListener("dblclick", () => createManualDraftFromPerson(item, row.querySelector("[data-manual-draft-person-start]")));
        row.querySelector("[data-manual-draft-person-start]")?.addEventListener("click", (event) => createManualDraftFromPerson(item, event.currentTarget));
        manualDraftPersonResults.appendChild(row);
      }
    };

    const renderDraftCaseRows = (items) => {
      if (!draftCaseResults) return;
      if (!items.length) {
        setDraftCaseMessage("一致するcaseはありません。");
        return;
      }
      draftCaseResults.innerHTML = "";
      for (const item of items) {
        const row = document.createElement("tr");
        const sourceText = `XML ${item.xml_count || 0} / CSV ${item.csv_count || 0} / 紙 ${item.paper_count || 0}`;
        const legal = `${item.legal_check_result || "PENDING"}${item.legal_reason_summary ? ` / ${item.legal_reason_summary}` : ""}`;
        const specific = `${item.specific_check_result || "PENDING"}${item.specific_reason_summary ? ` / ${item.specific_reason_summary}` : ""}`;
        row.innerHTML = `
          <td>
            <strong>${escapeHtml(item.exam_export_case_id || "-")}</strong>
            <small>${escapeHtml(item.name_kana || "-")} / HIA ${escapeHtml(item.hia_subscriber_id || "-")}</small>
          </td>
          <td>
            <strong title="${escapeHtml(item.facility_name || "")}">${escapeHtml(item.facility_name || "-")}</strong>
            <small>${escapeHtml(item.exam_date || "-")} / ${escapeHtml(item.facility_code || "-")}</small>
          </td>
          <td>
            <strong>${escapeHtml(sourceText)}</strong>
            <small>構成source ${escapeHtml(item.source_count || "0")} / 値 ${escapeHtml(item.case_value_count || "0")}</small>
          </td>
          <td>
            <strong>法定 ${escapeHtml(legal)}</strong>
            <small>特定 ${escapeHtml(specific)} / 出力 ${escapeHtml(item.export_readiness_status || "-")}</small>
          </td>
          <td><button type="button" class="primary-button compact-action-button" data-manual-draft-case-start>仮登録に追加</button></td>
        `;
        row.addEventListener("dblclick", () => createManualDraftFromCase(item, row.querySelector("[data-manual-draft-case-start]")));
        row.querySelector("[data-manual-draft-case-start]")?.addEventListener("click", (event) => createManualDraftFromCase(item, event.currentTarget));
        draftCaseResults.appendChild(row);
      }
    };

    const searchDraftPersons = async () => {
      const eventId = draftPersonEvent?.value || "2";
      const params = new URLSearchParams({
        event_id: eventId,
        q: draftPersonQ?.value.trim() || "",
        name_kana: draftPersonKana?.value.trim() || "",
        insurance_symbol: draftPersonSymbol?.value.trim() || "",
        insurance_number: draftPersonNumber?.value.trim() || "",
      });
      if (![...params.values()].some((value) => value && value !== eventId)) {
        setDraftPersonMessage("検索条件を入力してください。");
        return;
      }
      setDraftPersonMessage("検索中...");
      try {
        const payload = await fetchJson(`/api/manual-exam-entry/subscribers?${params.toString()}`);
        renderDraftPersonRows(Array.isArray(payload.items) ? payload.items : []);
      } catch (error) {
        setDraftPersonMessage(`検索でエラーが発生しました。${error?.message || ""}`);
      }
    };

    const searchDraftCases = async () => {
      const eventId = draftCaseEvent?.value || "2";
      const params = new URLSearchParams({
        event_id: eventId,
        facility_codes: draftCaseFacility?.value.trim() || "",
        exam_month: draftCaseExamMonth?.value.trim() || "",
        name_full: draftCaseNameFull?.value.trim() || "",
        name_kana: draftCaseKana?.value.trim() || "",
        hia_subscriber_id: draftCaseHia?.value.trim() || "",
        insurance_symbol: draftCaseSymbol?.value.trim() || "",
        insurance_number: draftCaseNumber?.value.trim() || "",
        limit: draftCaseLimit?.value.trim() || "50",
      });
      if (![...params.values()].some((value) => value && value !== eventId)) {
        setDraftCaseMessage("検索条件を入力してください。");
        return;
      }
      setDraftCaseMessage("検索中...");
      try {
        const payload = await fetchJson(`/api/manual-exam-entry/case-candidates?${params.toString()}`);
        renderDraftCaseRows(Array.isArray(payload.items) ? payload.items : []);
      } catch (error) {
        setDraftCaseMessage(`検索でエラーが発生しました。${error?.message || ""}`);
      }
    };

    const draftCaseFacilityValues = () => {
      if (!draftCaseFacility) return [];
      return draftCaseFacility.value
        .split(/[\s,，、]+/)
        .map((value) => value.trim())
        .filter(Boolean);
    };
    const updateDraftCaseFacilityState = () => {
      const values = new Set(draftCaseFacilityValues());
      if (draftCaseFacilitySummary) {
        draftCaseFacilitySummary.textContent = values.size ? `${values.size}施設を指定中` : "未指定: 全施設";
      }
      for (const row of document.querySelectorAll("[data-manual-draft-case-facility-row]")) {
        const code = String(row.getAttribute("data-facility-code") || "").trim();
        row.classList.toggle("is-selected", Boolean(code && values.has(code)));
      }
      for (const button of document.querySelectorAll("[data-manual-draft-case-facility-select]")) {
        const code = String(button.getAttribute("data-facility-code") || "").trim();
        const added = code && values.has(code);
        button.textContent = added ? "解除" : "追加";
        button.classList.toggle("is-active", Boolean(added));
      }
    };
    const toggleDraftCaseFacility = (element) => {
      if (!draftCaseFacility || !(element instanceof Element)) return;
      const code = String(element.getAttribute("data-facility-code") || "").trim();
      if (!code) return;
      const values = draftCaseFacilityValues();
      draftCaseFacility.value = values.includes(code)
        ? values.filter((value) => value !== code).join(", ")
        : [...values, code].join(", ");
      draftCaseFacility.dispatchEvent(new Event("input", { bubbles: true }));
      updateDraftCaseFacilityState();
    };

    document.querySelector("[data-manual-draft-person-search]")?.addEventListener("click", searchDraftPersons);
    document.querySelector("[data-manual-draft-case-search]")?.addEventListener("click", searchDraftCases);
    for (const input of [draftPersonQ, draftPersonKana, draftPersonSymbol, draftPersonNumber]) {
      input?.addEventListener("keydown", (event) => {
        if (event.key === "Enter") {
          event.preventDefault();
          searchDraftPersons();
        }
      });
    }
    for (const input of [draftCaseFacility, draftCaseSymbol, draftCaseNumber, draftCaseKana, draftCaseNameFull, draftCaseHia, draftCaseExamMonth, draftCaseLimit]) {
      input?.addEventListener("keydown", (event) => {
        if (event.key === "Enter") {
          event.preventDefault();
          searchDraftCases();
        }
      });
    }
    for (const row of document.querySelectorAll("[data-manual-draft-case-facility-row]")) {
      row.addEventListener("click", () => toggleDraftCaseFacility(row));
    }
    for (const button of document.querySelectorAll("[data-manual-draft-case-facility-select]")) {
      button.addEventListener("click", (event) => {
        event.stopPropagation();
        toggleDraftCaseFacility(button);
      });
    }
    document.querySelector("[data-manual-draft-case-facility-clear]")?.addEventListener("click", () => {
      if (!draftCaseFacility) return;
      draftCaseFacility.value = "";
      draftCaseFacility.dispatchEvent(new Event("input", { bubbles: true }));
      updateDraftCaseFacilityState();
      closeModal(document.querySelector("#manual-draft-case-facility-picker-modal"));
    });
    draftCaseFacility?.addEventListener("input", updateDraftCaseFacilityState);
    updateDraftCaseFacilityState();
  }

  const manualLedgerRevertModal = document.querySelector("#manual-ledger-revert-confirm-modal");
  if (manualLedgerRevertModal) {
    const manualLedgerRevertLabel = document.querySelector("[data-manual-ledger-revert-label]");
    const manualLedgerRevertConfirm = document.querySelector("[data-manual-ledger-revert-confirm]");
    let activeManualLedgerRevertButton = null;

    const openManualLedgerRevertModal = (button) => {
      const ledgerId = button.getAttribute("data-ledger-id") || "";
      const draftId = button.getAttribute("data-draft-id") || "";
      const valueCount = button.getAttribute("data-value-count") || "0";
      if (!ledgerId || !draftId) return;
      activeManualLedgerRevertButton = button;
      if (manualLedgerRevertLabel) {
        manualLedgerRevertLabel.textContent = `ledger ${ledgerId} / draft ${draftId} / 正式値 ${valueCount}件`;
      }
      if (manualLedgerRevertConfirm) {
        manualLedgerRevertConfirm.disabled = false;
        manualLedgerRevertConfirm.textContent = "draftへ戻す";
      }
      manualLedgerRevertModal.hidden = false;
      document.body.classList.add("has-open-modal");
      manualLedgerRevertConfirm?.focus();
    };

    const revertManualLedger = async () => {
      const button = activeManualLedgerRevertButton;
      if (!button) return;
      const ledgerId = button.getAttribute("data-ledger-id") || "";
      if (!ledgerId) return;
      const originalText = button.textContent || "戻す";
      button.disabled = true;
      button.textContent = "戻し中";
      if (manualLedgerRevertConfirm) {
        manualLedgerRevertConfirm.disabled = true;
        manualLedgerRevertConfirm.textContent = "戻し中";
      }
      try {
        const payload = await postJson(`/admin/manual-exam-ledgers/${encodeURIComponent(ledgerId)}/revert`, {});
        const query = new URLSearchParams();
        query.set("message", payload.message || "draftへ戻しました。");
        window.location.href = `/admin/manual-exam-ledgers?${query.toString()}`;
      } catch (error) {
        button.disabled = false;
        button.textContent = originalText;
        if (manualLedgerRevertConfirm) {
          manualLedgerRevertConfirm.disabled = false;
          manualLedgerRevertConfirm.textContent = "draftへ戻す";
        }
        window.alert(`draft戻しでエラーが発生しました。${error?.message || ""}`);
      }
    };

    for (const button of document.querySelectorAll("[data-manual-ledger-revert-open]")) {
      button.addEventListener("click", () => openManualLedgerRevertModal(button));
    }
    manualLedgerRevertConfirm?.addEventListener("click", revertManualLedger);
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
      input.addEventListener("input", () => {
        refreshManualMethodGroup(input.dataset.manualMethodGroup || "");
        scheduleManualEntryDraftSave();
      });
      refreshManualMethodGroup(input.dataset.manualMethodGroup || "");
    }

    for (const select of document.querySelectorAll("[data-manual-code-select]")) {
      select.addEventListener("change", () => {
        const input = select.closest(".manual-entry-value-control")?.querySelector(".manual-entry-value-input");
        if (!input || !select.value) return;
        input.value = select.value;
        input.dispatchEvent(new Event("input", { bubbles: true }));
        scheduleManualEntryDraftSave();
      });
    }

    for (const button of document.querySelectorAll("[data-manual-code-toggle]")) {
      button.addEventListener("click", () => {
        const input = button.closest(".manual-entry-value-control")?.querySelector(".manual-entry-value-input");
        if (!input) return;
        input.value = button.value || "";
        input.dispatchEvent(new Event("input", { bubbles: true }));
        refreshManualCodeToggle(input);
        scheduleManualEntryDraftSave();
      });
    }

    for (const button of document.querySelectorAll("[data-manual-code-toggle-clear]")) {
      button.addEventListener("click", () => {
        const input = button.closest(".manual-entry-value-control")?.querySelector(".manual-entry-value-input");
        if (!input) return;
        input.value = "";
        input.dispatchEvent(new Event("input", { bubbles: true }));
        refreshManualCodeToggle(input);
        scheduleManualEntryDraftSave();
      });
    }

    for (const input of manualValueInputs) {
      refreshManualCodeToggle(input);
    }
  }

  const manualRandomTimeInputs = Array.from(document.querySelectorAll("[data-manual-random-time-input]"));
  const manualBloodTimeInput = document.querySelector("[data-manual-blood-time-input]");
  const manualBloodTimeRow = document.querySelector("[data-manual-blood-time-row]");
  const manualBloodTimeBadge = document.querySelector("[data-manual-blood-time-required-badge]");
  if (manualRandomTimeInputs.length && manualBloodTimeInput && manualBloodTimeRow) {
    const hasRandomTimeValue = () => manualRandomTimeInputs.some((input) => String(input.value || "").trim() !== "");
    const hasBloodTimeValue = () => String(manualBloodTimeInput.value || "").trim() !== "";
    const refreshBloodTimeRequirement = ({ scrollIfMissing = false } = {}) => {
      const required = hasRandomTimeValue();
      manualBloodTimeInput.toggleAttribute("required", required);
      manualBloodTimeRow.classList.toggle("is-manual-required", required && !hasBloodTimeValue());
      if (manualBloodTimeBadge) manualBloodTimeBadge.hidden = !required;
      if (required && !hasBloodTimeValue() && scrollIfMissing) {
        manualBloodTimeRow.scrollIntoView({ behavior: "smooth", block: "center" });
        manualBloodTimeInput.focus({ preventScroll: true });
      }
    };
    for (const input of manualRandomTimeInputs) {
      input.addEventListener("input", () => refreshBloodTimeRequirement());
      input.addEventListener("change", () => refreshBloodTimeRequirement({ scrollIfMissing: true }));
    }
    manualBloodTimeInput.addEventListener("input", () => refreshBloodTimeRequirement());
    refreshBloodTimeRequirement();
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
    const openManualEntrySearchMatches = () => {
      const keyword = normalize(manualEntryItemSearchInput?.value || "");
      if (!keyword) return;
      for (const category of categories) {
        const hasMatch = Array.from(category.querySelectorAll("tbody tr[data-filter-text]"))
          .some((row) => normalize(row.dataset.filterText).includes(keyword));
        if (hasMatch) category.open = true;
      }
    };
    const renderFloatingItemResults = () => {
      if (!manualEntryFloatingItemSearchInput || !manualEntryFloatingItemResults) return;
      const keyword = normalize(manualEntryFloatingItemSearchInput.value);
      if (!keyword) {
        manualEntryFloatingItemResults.hidden = true;
        manualEntryFloatingItemResults.setAttribute("hidden", "");
        manualEntryFloatingItemResults.classList.remove("is-visible");
        manualEntryFloatingItemResults.style.display = "";
        manualEntryFloatingItemResults.innerHTML = "";
        return;
      }
      const matches = itemRows
        .map((row, index) => ({ row, index }))
        .filter(({ row }) => normalize(row.getAttribute("data-filter-text")).includes(keyword))
        .slice(0, 10);
      manualEntryFloatingItemResults.hidden = false;
      manualEntryFloatingItemResults.removeAttribute("hidden");
      manualEntryFloatingItemResults.classList.add("is-visible");
      manualEntryFloatingItemResults.style.display = "grid";
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
    manualEntryItemSearchInput?.addEventListener("input", openManualEntrySearchMatches);
    manualEntryItemSearchInput?.addEventListener("keydown", (event) => {
      if (event.key === "Enter") {
        event.preventDefault();
        jumpToManualEntryItem();
      }
    });
    manualEntryFloatingItemSearchInput?.addEventListener("input", () => {
      renderFloatingItemResults();
    });
    manualEntryFloatingItemSearchInput?.addEventListener("compositionend", () => {
      renderFloatingItemResults();
    });
    manualEntryFloatingItemSearchInput?.addEventListener("keyup", () => {
      renderFloatingItemResults();
    });
    manualEntryFloatingItemSearchInput?.addEventListener("keydown", (event) => {
      if (event.key === "Enter") {
        event.preventDefault();
        const firstCard = manualEntryFloatingItemResults?.querySelector("[data-manual-entry-floating-item-jump]");
        if (firstCard instanceof HTMLElement) {
          firstCard.click();
        } else {
          renderFloatingItemResults();
        }
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

  const csvTemplatePreviewDataElement = document.querySelector("#csv-template-header-preview-data");
  const csvTemplatePreview = document.querySelector("[data-csv-template-inline-preview]");
  if (csvTemplatePreviewDataElement && csvTemplatePreview) {
    let previewPayload = { header_rows: [], columns: [] };
    try {
      previewPayload = JSON.parse(csvTemplatePreviewDataElement.textContent || "{}");
    } catch (_error) {
      previewPayload = { header_rows: [], columns: [] };
    }
    const previewTableBody = csvTemplatePreview.querySelector(".csv-template-inline-preview-table tbody");
    const headerStructureInput = document.querySelector("select[name='header_structure_type']");
    const activeHeaderRowInput = document.querySelector("input[name='active_header_row_no']");
    const dataStartRowInput = document.querySelector("input[name='data_start_row_no']");
    const headerRows = Array.isArray(previewPayload.header_rows) ? previewPayload.header_rows : [];
    const columns = Array.isArray(previewPayload.columns) ? previewPayload.columns : [];
    const maxColumnCount = Math.max(
      columns.length,
      ...headerRows.map((row) => (Array.isArray(row) ? row.length : 0)),
    );

    const positiveNumber = (value, fallback) => {
      const parsed = Number.parseInt(String(value || ""), 10);
      return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
    };
    const rowTitleCell = (title, detail, extraClass = "") => `
      <th class="csv-template-inline-preview-table__row-title ${extraClass}" scope="row">
        <span>${escapeHtml(title)}</span>
        <small>${escapeHtml(detail)}</small>
      </th>
    `;
    const valueCell = (value, column, tagName = "td", extraClass = "") => {
      const classNames = [column?.is_mapped ? "is-used" : "", extraClass].filter(Boolean).join(" ");
      return `<${tagName} class="${classNames}">${escapeHtml(value || "-")}</${tagName}>`;
    };
    const buildContextValues = (activeHeaderIndex) => {
      const contextRows = headerRows.slice(0, Math.max(activeHeaderIndex, 0));
      return Array.from({ length: maxColumnCount }, (_, columnIndex) => {
        const values = contextRows
          .map((row) => (Array.isArray(row) ? String(row[columnIndex] || "").trim() : ""))
          .filter(Boolean);
        return values.join(" / ") || "-";
      });
    };
    const renderCsvTemplatePreview = () => {
      if (!previewTableBody || !maxColumnCount) return;
      const dataStartRow = positiveNumber(dataStartRowInput?.value, 2);
      const inferredHeaderRow = Math.max(dataStartRow - 1, 1);
      const activeHeaderRow = positiveNumber(activeHeaderRowInput?.value, inferredHeaderRow);
      const activeHeaderIndex = Math.max(0, Math.min(activeHeaderRow - 1, Math.max(headerRows.length - 1, 0)));
      const isContextHeader = headerStructureInput?.value === "CONTEXT_HEADER";
      const contextLabel =
        activeHeaderRow > 2 ? `1-${activeHeaderRow - 1}行目` : activeHeaderRow === 2 ? "1行目" : "なし";
      const rows = [];
      rows.push(`
        <tr>
          ${rowTitleCell("列番", "CSV列")}
          ${Array.from({ length: maxColumnCount }, (_, index) => valueCell(String(index + 1), columns[index], "th")).join("")}
        </tr>
      `);
      if (isContextHeader) {
        const contextValues = buildContextValues(activeHeaderIndex);
        rows.push(`
          <tr>
            ${rowTitleCell("コンテキスト", contextLabel)}
            ${contextValues.map((value, index) => valueCell(value, columns[index])).join("")}
          </tr>
        `);
      }
      const activeRow = Array.isArray(headerRows[activeHeaderIndex]) ? headerRows[activeHeaderIndex] : [];
      rows.push(`
        <tr>
          ${rowTitleCell("ヘッダー", `${activeHeaderRow}行目`, "is-header-row")}
          ${Array.from({ length: maxColumnCount }, (_, index) =>
            valueCell(activeRow[index] || columns[index]?.name || "-", columns[index], "td", "csv-template-inline-preview-table__header"),
          ).join("")}
        </tr>
      `);
      rows.push(`
        <tr>
          ${rowTitleCell("データ", `${dataStartRow}行目`)}
          ${Array.from({ length: maxColumnCount }, (_, index) => valueCell("-", columns[index])).join("")}
        </tr>
      `);
      previewTableBody.innerHTML = rows.join("");
    };

    headerStructureInput?.addEventListener("change", renderCsvTemplatePreview);
    activeHeaderRowInput?.addEventListener("input", renderCsvTemplatePreview);
    dataStartRowInput?.addEventListener("input", renderCsvTemplatePreview);
    renderCsvTemplatePreview();
  }

  const csvTemplateTargetModal = document.getElementById("csv-template-target-item-modal");
  if (csvTemplateTargetModal) {
    const csvTemplateLedgerModal = document.getElementById("csv-template-ledger-field-modal");
    const modeButtons = Array.from(csvTemplateTargetModal.querySelectorAll("[data-csv-template-target-mode]"));
    const typeButtons = Array.from(csvTemplateTargetModal.querySelectorAll("[data-csv-template-target-type-filter]"));
    const searchInput = csvTemplateTargetModal.querySelector("[data-csv-template-target-search-input]");
    const results = csvTemplateTargetModal.querySelector("[data-csv-template-target-results]");
    const detail = csvTemplateTargetModal.querySelector("[data-csv-template-target-detail]");
    const detailDrawer = csvTemplateTargetModal.querySelector("[data-csv-template-target-detail-drawer]");
    const detailToggle = csvTemplateTargetModal.querySelector("[data-csv-template-target-detail-toggle]");
    const detailClose = csvTemplateTargetModal.querySelector("[data-csv-template-target-detail-close]");
    const selectedList = csvTemplateTargetModal.querySelector("[data-csv-template-target-selected-list]");
    const selectedCount = csvTemplateTargetModal.querySelector("[data-csv-template-target-selected-count]");
    const applyButton = csvTemplateTargetModal.querySelector("[data-csv-template-target-apply]");
    const draftList = document.querySelector("[data-csv-template-target-draft-list]");
    const draftSaveButton = document.querySelector("[data-csv-template-target-save]");
    const draftSaveMessage = document.querySelector("[data-csv-template-target-save-message]");
    const ruleEditButtons = Array.from(document.querySelectorAll("[data-csv-template-rule-edit]"));
    const ruleDeleteButtons = Array.from(document.querySelectorAll("[data-csv-template-rule-delete]"));
    const headerSearchInput = csvTemplateTargetModal.querySelector("[data-csv-template-header-search-input]");
    const headerCandidateList = csvTemplateTargetModal.querySelector("[data-csv-template-header-candidate-list]");
    const headerCandidateCards = Array.from(csvTemplateTargetModal.querySelectorAll("[data-csv-template-header-candidate]"));
    const ledgerCards = Array.from(document.querySelectorAll("[data-csv-template-ledger-modal-field]"));
    const ledgerModeButtons = Array.from(document.querySelectorAll("[data-csv-template-ledger-mode]"));
    const ledgerHeaderCards = Array.from(document.querySelectorAll("[data-csv-template-ledger-header-candidate]"));
    const ledgerHeaderList = document.querySelector("[data-csv-template-ledger-header-candidate-list]");
    const ledgerHeaderSearchInput = document.querySelector("[data-csv-template-ledger-header-search-input]");
    const ledgerSelectedList = document.querySelector("[data-csv-template-ledger-selected-list]");
    const ledgerSelectedCount = document.querySelector("[data-csv-template-ledger-selected-count]");
    const ledgerApplyButton = document.querySelector("[data-csv-template-ledger-apply]");
    const ledgerDetail = document.querySelector("[data-csv-template-ledger-detail]");
    const ledgerDetailDrawer = document.querySelector("[data-csv-template-ledger-detail-drawer]");
    const ledgerDetailToggle = document.querySelector("[data-csv-template-ledger-detail-toggle]");
    const ledgerDetailClose = document.querySelector("[data-csv-template-ledger-detail-close]");
    const modalTitle = csvTemplateTargetModal.querySelector("#csv-template-target-item-title");
    const modalLead = csvTemplateTargetModal.querySelector(".edit-modal__head .subtle");
    let targetMode = "one";
    let targetKind = "EXAM_ITEM_VALUE";
    let targetValueType = "";
    let targetItems = [];
    let selectedHeaders = [];
    let focusedItem = null;
    let focusedLedger = null;
    let ledgerMode = "one";
    let ledgerSelectedHeaders = [];
    let draftItems = [];
    let editingDraftId = null;
    let searchTimer = null;
    const templateIdMatch = window.location.pathname.match(/\/admin\/csv-mapping-templates\/(\d+)\/edit/);
    const csvTemplateId = templateIdMatch ? templateIdMatch[1] : "";

    const selectedTarget = () => focusedLedger || focusedItem;

    const setDetailDrawerOpen = (open) => {
      if (detailDrawer) detailDrawer.classList.toggle("is-open", Boolean(open && selectedTarget()));
    };

    const setLedgerDetailDrawerOpen = (open) => {
      if (ledgerDetailDrawer) ledgerDetailDrawer.classList.toggle("is-open", Boolean(open && focusedLedger));
    };

    const setComposerMessage = (message) => {
      if (draftSaveMessage) draftSaveMessage.textContent = message;
    };

    const saveTemplateMappingItem = async (item, button) => {
      if (!csvTemplateId) {
        setComposerMessage("先に基本情報を保存してください。");
        return;
      }
      if (button) {
        button.disabled = true;
        button.classList.add("disabled");
      }
      setComposerMessage(item.ruleId ? "変更を保存中..." : "マッピングを保存中...");
      try {
        const payload = await postJson(`/api/admin/csv-mapping-templates/${csvTemplateId}/screen-rules`, {
          items: [item],
        });
        const message = payload?.message || (item.ruleId ? "マッピングを変更しました。" : "マッピングを追加しました。");
        window.location.href = `${window.location.pathname}?message=${encodeURIComponent(message)}`;
      } catch (error) {
        setComposerMessage(error.message || "保存でエラーが発生しました。");
        if (button) {
          button.disabled = false;
          button.classList.remove("disabled");
        }
      }
    };

    const updateDraftSaveState = () => {
      if (!draftSaveButton) return;
      const canSave = Boolean(csvTemplateId) && draftItems.length > 0;
      draftSaveButton.disabled = !canSave;
      draftSaveButton.classList.toggle("disabled", !canSave);
      if (draftSaveMessage) {
        draftSaveMessage.textContent = draftItems.length
          ? `${draftItems.length}件を保存できます。`
          : "追加予定はありません。";
      }
    };

    const resetTargetDraftForm = (nextKind = "exam") => {
      editingDraftId = null;
      selectedHeaders = [];
      setTargetKind(nextKind);
    };

    const renderDraftItems = () => {
      if (!draftList) return;
      if (!draftItems.length) {
        draftList.innerHTML = `<p class="subtle">追加する項目をモーダルから選ぶと、ここに仮配置されます。</p>`;
        updateDraftSaveState();
        return;
      }
      draftList.innerHTML = draftItems.map((draft) => {
        const headerLabel = draft.headers.map((header) => `${header.columnNo}列目 ${header.headerName || "-"}`).join(" + ");
        return `
          <article class="csv-template-target-draft-card" data-csv-template-target-draft-id="${escapeHtml(draft.id)}">
            <span class="status-pill ${draft.mode === "many" ? "status-pending" : "status-ready"}">${draft.mode === "many" ? "1:n 結合" : "1:1"}</span>
            <div>
              <strong>${escapeHtml(draft.targetName || draft.targetCode || "-")}</strong>
              <small>${escapeHtml(draft.targetKind)} / ${escapeHtml(draft.targetMeta || "-")}</small>
              <small>${escapeHtml(headerLabel)}</small>
            </div>
            <div class="csv-template-target-draft-card__actions">
              <button type="button" class="ghost-button compact-action-button" data-csv-template-target-draft-edit="${escapeHtml(draft.id)}">編集</button>
              <button type="button" class="ghost-button compact-action-button" data-csv-template-target-draft-delete="${escapeHtml(draft.id)}">削除</button>
            </div>
          </article>
        `;
      }).join("");
      updateDraftSaveState();
    };

    const renderTargetDetail = (item) => {
      if (!detail) return;
      if (!item) {
        detail.innerHTML = `<p class="subtle">候補を選ぶと、ここに詳細を表示します。</p>`;
        setDetailDrawerOpen(false);
        return;
      }
      if (item.targetKind === "LEDGER_FIELD") {
        detail.innerHTML = `
          <div class="csv-template-target-detail__head">
            <span class="status-pill">基本・受診情報</span>
            <h3>${escapeHtml(item.label || item.value || "-")}</h3>
            <small>${escapeHtml(item.value || "-")}</small>
          </div>
          <dl class="definition-grid">
            <div>
              <dt>保存先</dt>
              <dd>LEDGER_FIELD</dd>
            </div>
            <div>
              <dt>field</dt>
              <dd>${escapeHtml(item.value || "-")}</dd>
            </div>
            <div>
              <dt>説明</dt>
              <dd>${escapeHtml(item.hint || "-")}</dd>
            </div>
          </dl>
        `;
        return;
      }
      const standardRows = Array.isArray(item.standard_code_rows) ? item.standard_code_rows : [];
      const variantRows = Array.isArray(item.norm_variant_rows) ? item.norm_variant_rows : [];
      const rows = [
        ["namecode", item.namecode],
        ["項目名", item.item_name],
        ["カテゴリ", item.category_name],
        ["識別項目", [item.identity_item_code, item.identity_item_name].filter(Boolean).join(" / ")],
        ["XML値型", [item.xml_value_type, item.data_type_label].filter(Boolean).join(" / ")],
        ["単位", [item.display_unit, item.ucum_unit].filter(Boolean).join(" / ")],
        ["項目OID", item.item_code_oid],
        ["結果OID", item.result_code_oid],
        ["値取得", item.value_method],
        ["実施要件", item.annex2_exec_requirement],
      ];
      detail.innerHTML = `
        <div class="csv-template-target-detail__head">
          <span class="status-pill">項目詳細</span>
          <h3>${escapeHtml(item.item_name || item.namecode || "-")}</h3>
          <small>${escapeHtml(item.namecode || "-")}</small>
        </div>
        <dl class="definition-grid">
          ${rows.map(([label, value]) => `
            <div>
              <dt>${escapeHtml(label)}</dt>
              <dd>${escapeHtml(value || "-")}</dd>
            </div>
          `).join("")}
        </dl>
        <div class="csv-template-target-detail__norm">
          <strong>norm</strong>
          <small>標準 ${escapeHtml(String(standardRows.length))}件 / 揺れ含む ${escapeHtml(String(variantRows.length))}件</small>
        </div>
      `;
    };

    const renderSelectedTargets = () => {
      if (!selectedList) return;
      if (detailToggle) {
        detailToggle.disabled = !selectedTarget();
        detailToggle.classList.toggle("disabled", !selectedTarget());
      }
      if (selectedCount) selectedCount.textContent = `${selectedHeaders.length}件`;
      if (applyButton) {
        applyButton.disabled = !selectedTarget() || selectedHeaders.length === 0;
        applyButton.classList.toggle("disabled", !selectedTarget() || selectedHeaders.length === 0);
      }
      if (!selectedHeaders.length) {
        selectedList.innerHTML = `<p class="subtle">下の候補ヘッダーから選んでください。</p>`;
        return;
      }
      selectedList.innerHTML = selectedHeaders.map((header) => `
        <article class="csv-template-target-selected-card">
          <div>
            <strong>${escapeHtml(header.headerName || "-")}</strong>
            <small>${escapeHtml(header.columnNo || "-")}列目 / ${escapeHtml(header.headerContext || "contextなし")}</small>
          </div>
          <button type="button" class="ghost-button compact-action-button" data-csv-template-target-remove="${escapeHtml(header.columnNo || "")}">外す</button>
        </article>
      `).join("");
    };

    const renderTargetResults = () => {
      if (!results) return;
      if (!targetItems.length) {
        results.innerHTML = `<p class="subtle">候補はありません。</p>`;
        return;
      }
      results.innerHTML = targetItems.map((item) => {
        const isSelected = focusedItem && focusedItem.namecode === item.namecode;
        const meta = [item.namecode, item.xml_value_type, item.display_unit, item.method_name].filter(Boolean).join(" / ");
        return `
          <button type="button" class="csv-mapping-exam-item-option${isSelected ? " is-selected" : ""}" data-csv-template-target-namecode="${escapeHtml(item.namecode || "")}">
            <strong>${escapeHtml(item.item_name || item.namecode || "-")}</strong>
            <small>${escapeHtml(meta || "-")}</small>
            <span>${escapeHtml(item.category_name || "-")}${item.identity_item_name ? ` / ${escapeHtml(item.identity_item_name)}` : ""}</span>
          </button>
        `;
      }).join("");
    };

    const renderHeaderCandidateCards = () => {
      for (const card of headerCandidateCards) {
        const columnNo = card.getAttribute("data-column-no") || "";
        const isSelected = selectedHeaders.some((header) => header.columnNo === columnNo);
        card.classList.toggle("is-selected", isSelected);
      }
      renderSelectedTargets();
    };

    const filterHeaderCandidateCards = () => {
      const keyword = String(headerSearchInput?.value || "").trim().toLowerCase();
      for (const card of headerCandidateCards) {
        const text = String(card.getAttribute("data-filter-text") || "").toLowerCase();
        card.hidden = Boolean(keyword) && !text.includes(keyword);
      }
    };

    const searchTemplateTargetItems = async () => {
      if (!results) return;
      const keyword = String(searchInput?.value || "").trim();
      results.innerHTML = `<p class="subtle">検索中...</p>`;
      try {
        const params = new URLSearchParams();
        params.set("keyword", keyword);
        if (targetValueType) params.set("value_type", targetValueType);
        const response = await fetch(`/api/csv-mapping-lab/exam-items?${params.toString()}`);
        if (!response.ok) throw new Error("search_failed");
        const payload = await response.json();
        targetItems = Array.isArray(payload.items) ? payload.items : [];
        renderTargetResults();
      } catch (_error) {
        results.innerHTML = `<p class="subtle">検索でエラーが発生しました。</p>`;
      }
    };

    const selectTemplateTargetItem = (namecode) => {
      const item = targetItems.find((candidate) => candidate.namecode === namecode);
      if (!item) return;
      focusedItem = item;
      focusedLedger = null;
      renderTargetDetail(item);
      renderSelectedTargets();
      renderTargetResults();
    };

    const selectTemplateLedgerField = (card) => {
      focusedLedger = {
        targetKind: "LEDGER_FIELD",
        value: card.getAttribute("data-ledger-field") || "",
        label: card.getAttribute("data-ledger-label") || "",
        hint: card.getAttribute("data-ledger-hint") || "",
      };
      focusedItem = null;
      for (const ledgerCard of ledgerCards) {
        ledgerCard.classList.toggle("is-selected", ledgerCard === card);
      }
      renderTargetDetail(focusedLedger);
      renderSelectedTargets();
    };

    const setTargetKind = (nextKind) => {
      targetKind = nextKind === "ledger" ? "LEDGER_FIELD" : "EXAM_ITEM_VALUE";
      focusedItem = null;
      focusedLedger = null;
      selectedHeaders = [];
      for (const ledgerCard of ledgerCards) {
        ledgerCard.classList.remove("is-selected");
      }
      if (modalTitle) {
        modalTitle.textContent = targetKind === "LEDGER_FIELD" ? "基本・受診情報を追加" : "健診項目を追加";
      }
      if (modalLead) {
        modalLead.textContent = targetKind === "LEDGER_FIELD"
          ? "追加する基本・受診情報を1つ選び、方式を決めてCSVヘッダー候補をセットします。"
          : "追加する健診項目を1つ選び、方式を決めてCSVヘッダー候補をセットします。";
      }
      renderTargetDetail(null);
      setDetailDrawerOpen(false);
      renderSelectedTargets();
      renderTargetResults();
      renderHeaderCandidateCards();
      if (targetKind === "EXAM_ITEM_VALUE") searchTemplateTargetItems();
    };

    const renderLedgerDetail = (field) => {
      if (!ledgerDetail) return;
      if (!field) {
        ledgerDetail.innerHTML = `<p class="subtle">候補を選ぶと、ここに基本・受診情報の詳細を表示します。</p>`;
        setLedgerDetailDrawerOpen(false);
        return;
      }
      ledgerDetail.innerHTML = `
        <div class="csv-template-target-detail__head">
          <span class="status-pill">基本・受診情報</span>
          <h3>${escapeHtml(field.label || field.value || "-")}</h3>
          <small>${escapeHtml(field.value || "-")}</small>
        </div>
        <dl class="definition-grid">
          <div>
            <dt>保存先</dt>
            <dd>LEDGER_FIELD</dd>
          </div>
          <div>
            <dt>field</dt>
            <dd>${escapeHtml(field.value || "-")}</dd>
          </div>
          <div>
            <dt>説明</dt>
            <dd>${escapeHtml(field.hint || "-")}</dd>
          </div>
        </dl>
      `;
    };

    const renderLedgerSelectedHeaders = () => {
      if (ledgerSelectedCount) ledgerSelectedCount.textContent = `${ledgerSelectedHeaders.length}件`;
      if (ledgerDetailToggle) {
        ledgerDetailToggle.disabled = !focusedLedger;
        ledgerDetailToggle.classList.toggle("disabled", !focusedLedger);
      }
      if (ledgerApplyButton) {
        ledgerApplyButton.disabled = !focusedLedger || ledgerSelectedHeaders.length === 0;
        ledgerApplyButton.classList.toggle("disabled", !focusedLedger || ledgerSelectedHeaders.length === 0);
      }
      if (!ledgerSelectedList) return;
      if (!ledgerSelectedHeaders.length) {
        ledgerSelectedList.innerHTML = `<p class="subtle">下の候補ヘッダーから選んでください。</p>`;
        return;
      }
      ledgerSelectedList.innerHTML = ledgerSelectedHeaders.map((header) => `
        <article class="csv-template-target-selected-card">
          <div>
            <strong>${escapeHtml(header.headerName || "-")}</strong>
            <small>${escapeHtml(header.columnNo || "-")}列目 / ${escapeHtml(header.headerContext || "contextなし")}</small>
          </div>
          <button type="button" class="ghost-button compact-action-button" data-csv-template-ledger-header-remove="${escapeHtml(header.columnNo || "")}">外す</button>
        </article>
      `).join("");
    };

    const renderLedgerHeaderCards = () => {
      for (const card of ledgerHeaderCards) {
        const columnNo = card.getAttribute("data-column-no") || "";
        card.classList.toggle("is-selected", ledgerSelectedHeaders.some((header) => header.columnNo === columnNo));
      }
      renderLedgerSelectedHeaders();
    };

    const filterLedgerHeaderCards = () => {
      const keyword = String(ledgerHeaderSearchInput?.value || "").trim().toLowerCase();
      for (const card of ledgerHeaderCards) {
        const text = String(card.getAttribute("data-filter-text") || "").toLowerCase();
        card.hidden = Boolean(keyword) && !text.includes(keyword);
      }
    };

    const selectLedgerFieldCard = (card) => {
      focusedLedger = {
        targetKind: "LEDGER_FIELD",
        value: card.getAttribute("data-ledger-field") || "",
        label: card.getAttribute("data-ledger-label") || "",
        hint: card.getAttribute("data-ledger-hint") || "",
      };
      for (const ledgerCard of ledgerCards) {
        ledgerCard.classList.toggle("is-selected", ledgerCard === card);
      }
      renderLedgerDetail(focusedLedger);
      renderLedgerSelectedHeaders();
    };

    const resetLedgerModal = () => {
      editingDraftId = null;
      focusedLedger = null;
      ledgerSelectedHeaders = [];
      ledgerMode = "one";
      ledgerModeButtons.forEach((button) => {
        button.classList.toggle("is-selected", button.getAttribute("data-csv-template-ledger-mode") === "one");
      });
      for (const card of ledgerCards) card.classList.remove("is-selected");
      if (ledgerApplyButton) ledgerApplyButton.textContent = "マッピングに追加";
      renderLedgerDetail(null);
      setLedgerDetailDrawerOpen(false);
      renderLedgerHeaderCards();
      filterLedgerHeaderCards();
    };

    detailToggle?.addEventListener("click", () => setDetailDrawerOpen(true));
    detailClose?.addEventListener("click", () => setDetailDrawerOpen(false));
    ledgerDetailToggle?.addEventListener("click", () => setLedgerDetailDrawerOpen(true));
    ledgerDetailClose?.addEventListener("click", () => setLedgerDetailDrawerOpen(false));

    const openLedgerModal = () => {
      if (!csvTemplateLedgerModal) return;
      csvTemplateLedgerModal.hidden = false;
      document.body.classList.add("has-open-modal");
    };

    const loadLedgerDraftForEdit = (draft) => {
      resetLedgerModal();
      editingDraftId = draft.ruleId || null;
      ledgerMode = draft.mode || "one";
      ledgerSelectedHeaders = Array.isArray(draft.headers) ? [...draft.headers] : [];
      ledgerModeButtons.forEach((button) => {
        button.classList.toggle("is-selected", button.getAttribute("data-csv-template-ledger-mode") === ledgerMode);
      });
      const card = ledgerCards.find((ledgerCard) => ledgerCard.getAttribute("data-ledger-field") === draft.targetCode);
      if (card) {
        selectLedgerFieldCard(card);
      } else {
        focusedLedger = {
          targetKind: "LEDGER_FIELD",
          value: draft.targetCode || "",
          label: draft.targetName || "",
          hint: draft.targetMeta || "",
        };
        renderLedgerDetail(focusedLedger);
      }
      renderLedgerHeaderCards();
      if (ledgerApplyButton) ledgerApplyButton.textContent = "変更を保存";
      openLedgerModal();
    };

    const loadDraftForEdit = (draft) => {
      if (draft.targetKind === "LEDGER_FIELD") {
        loadLedgerDraftForEdit(draft);
        return;
      }
      editingDraftId = draft.ruleId || null;
      setTargetKind(draft.targetKind === "LEDGER_FIELD" ? "ledger" : "exam");
      targetMode = draft.mode || "one";
      modeButtons.forEach((modeButton) => {
        modeButton.classList.toggle("is-selected", modeButton.getAttribute("data-csv-template-target-mode") === targetMode);
      });
      selectedHeaders = Array.isArray(draft.headers) ? [...draft.headers] : [];
      if (draft.targetKind === "LEDGER_FIELD") {
        const card = ledgerCards.find((ledgerCard) => ledgerCard.getAttribute("data-ledger-field") === draft.targetCode);
        if (card) {
          selectTemplateLedgerField(card);
        } else {
          focusedLedger = {
            targetKind: "LEDGER_FIELD",
            value: draft.targetCode || "",
            label: draft.targetName || "",
            hint: draft.targetMeta || "",
          };
          renderTargetDetail(focusedLedger);
        }
      } else {
        focusedItem = {
          targetKind: "EXAM_ITEM_VALUE",
          namecode: draft.targetCode || "",
          item_name: draft.targetName || "",
          category_name: draft.targetCategory || "",
          xml_value_type: draft.targetValueType || "",
          standard_code_rows: [],
          norm_variant_rows: [],
        };
        renderTargetDetail(focusedItem);
      }
      renderSelectedTargets();
      renderTargetResults();
      renderHeaderCandidateCards();
      if (applyButton) applyButton.textContent = "変更を保存";
      csvTemplateTargetModal.hidden = false;
      document.body.classList.add("has-open-modal");
    };

    modeButtons.forEach((button) => {
      button.addEventListener("click", () => {
        targetMode = button.getAttribute("data-csv-template-target-mode") || "one";
        modeButtons.forEach((modeButton) => modeButton.classList.toggle("is-selected", modeButton === button));
        if (targetMode === "one" && selectedHeaders.length > 1) {
          selectedHeaders = selectedHeaders.slice(0, 1);
          renderSelectedTargets();
          renderHeaderCandidateCards();
        }
      });
    });

    typeButtons.forEach((button) => {
      button.addEventListener("click", () => {
        targetValueType = button.getAttribute("data-csv-template-target-type-filter") || "";
        typeButtons.forEach((typeButton) => typeButton.classList.toggle("is-selected", typeButton === button));
        searchTemplateTargetItems();
      });
    });

    searchInput?.addEventListener("input", () => {
      window.clearTimeout(searchTimer);
      searchTimer = window.setTimeout(searchTemplateTargetItems, 220);
    });

    results?.addEventListener("click", (event) => {
      const target = event.target;
      if (!(target instanceof Element)) return;
      const option = target.closest("[data-csv-template-target-namecode]");
      if (!option) return;
      selectTemplateTargetItem(option.getAttribute("data-csv-template-target-namecode") || "");
    });

    ledgerCards.forEach((card) => {
      card.addEventListener("click", () => selectLedgerFieldCard(card));
    });

    ledgerModeButtons.forEach((button) => {
      button.addEventListener("click", () => {
        ledgerMode = button.getAttribute("data-csv-template-ledger-mode") || "one";
        ledgerModeButtons.forEach((modeButton) => modeButton.classList.toggle("is-selected", modeButton === button));
        if (ledgerMode === "one" && ledgerSelectedHeaders.length > 1) {
          ledgerSelectedHeaders = ledgerSelectedHeaders.slice(0, 1);
          renderLedgerHeaderCards();
        }
      });
    });

    ledgerHeaderList?.addEventListener("click", (event) => {
      const target = event.target;
      if (!(target instanceof Element)) return;
      const card = target.closest("[data-csv-template-ledger-header-candidate]");
      if (!card) return;
      const header = {
        columnNo: card.getAttribute("data-column-no") || "",
        headerName: card.getAttribute("data-header-name") || "",
        headerContext: card.getAttribute("data-header-context") || "",
      };
      const alreadySelected = ledgerSelectedHeaders.some((selected) => selected.columnNo === header.columnNo);
      if (alreadySelected) {
        ledgerSelectedHeaders = ledgerSelectedHeaders.filter((selected) => selected.columnNo !== header.columnNo);
      } else if (ledgerMode === "one") {
        ledgerSelectedHeaders = [header];
      } else {
        ledgerSelectedHeaders = [...ledgerSelectedHeaders, header];
      }
      renderLedgerHeaderCards();
    });

    ledgerHeaderSearchInput?.addEventListener("input", filterLedgerHeaderCards);

    ledgerSelectedList?.addEventListener("click", (event) => {
      const target = event.target;
      if (!(target instanceof Element)) return;
      const removeButton = target.closest("[data-csv-template-ledger-header-remove]");
      if (!removeButton) return;
      ledgerSelectedHeaders = ledgerSelectedHeaders.filter((header) => header.columnNo !== removeButton.getAttribute("data-csv-template-ledger-header-remove"));
      renderLedgerHeaderCards();
    });

    ledgerApplyButton?.addEventListener("click", () => {
      if (!focusedLedger || !ledgerSelectedHeaders.length) return;
      const draft = {
        ruleId: editingDraftId || null,
        mode: ledgerMode,
        targetKind: "LEDGER_FIELD",
        targetName: focusedLedger.label,
        targetCode: focusedLedger.value,
        targetMeta: `LEDGER_FIELD / ${focusedLedger.hint || "-"}`,
        targetCategory: "",
        targetValueType: "",
        headers: [...ledgerSelectedHeaders],
      };
      saveTemplateMappingItem(draft, ledgerApplyButton);
    });

    document.querySelectorAll("[data-csv-template-target-open-kind]").forEach((button) => {
      button.addEventListener("click", () => {
        if (applyButton) applyButton.textContent = "マッピングに追加";
        resetTargetDraftForm(button.getAttribute("data-csv-template-target-open-kind") || "exam");
      });
    });

    document.querySelectorAll("[data-modal-open='csv-template-ledger-field-modal']").forEach((button) => {
      button.addEventListener("click", resetLedgerModal);
    });

    headerCandidateList?.addEventListener("click", (event) => {
      const target = event.target;
      if (!(target instanceof Element)) return;
      const card = target.closest("[data-csv-template-header-candidate]");
      if (!card) return;
      const header = {
        columnNo: card.getAttribute("data-column-no") || "",
        headerName: card.getAttribute("data-header-name") || "",
        headerContext: card.getAttribute("data-header-context") || "",
      };
      const alreadySelected = selectedHeaders.some((selected) => selected.columnNo === header.columnNo);
      if (alreadySelected) {
        selectedHeaders = selectedHeaders.filter((selected) => selected.columnNo !== header.columnNo);
      } else if (targetMode === "one") {
        selectedHeaders = [header];
      } else {
        selectedHeaders = [...selectedHeaders, header];
      }
      renderHeaderCandidateCards();
    });

    headerSearchInput?.addEventListener("input", filterHeaderCandidateCards);

    selectedList?.addEventListener("click", (event) => {
      const target = event.target;
      if (!(target instanceof Element)) return;
      const removeButton = target.closest("[data-csv-template-target-remove]");
      if (!removeButton) return;
      selectedHeaders = selectedHeaders.filter((header) => header.columnNo !== removeButton.getAttribute("data-csv-template-target-remove"));
      renderSelectedTargets();
      renderHeaderCandidateCards();
    });

    applyButton?.addEventListener("click", () => {
      const target = selectedTarget();
      if (!target || !selectedHeaders.length) return;
      const targetName = targetKind === "LEDGER_FIELD" ? focusedLedger.label : focusedItem.item_name;
      const targetCode = targetKind === "LEDGER_FIELD" ? focusedLedger.value : focusedItem.namecode;
      const targetMeta = targetKind === "LEDGER_FIELD"
        ? `LEDGER_FIELD / ${focusedLedger.hint || "-"}`
        : `${focusedItem.namecode || "-"} / ${focusedItem.category_name || "-"} / ${focusedItem.xml_value_type || "-"}`;
      const draft = {
        ruleId: editingDraftId || null,
        mode: targetMode,
        targetKind,
        targetName,
        targetCode,
        targetMeta,
        targetCategory: focusedItem?.category_name || "",
        targetValueType: focusedItem?.xml_value_type || "",
        headers: [...selectedHeaders],
      };
      saveTemplateMappingItem(draft, applyButton);
    });

    draftList?.addEventListener("click", (event) => {
      const target = event.target;
      if (!(target instanceof Element)) return;
      const editButton = target.closest("[data-csv-template-target-draft-edit]");
      if (editButton) {
        const draft = draftItems.find((item) => item.id === editButton.getAttribute("data-csv-template-target-draft-edit"));
        if (draft) loadDraftForEdit(draft);
        return;
      }
      const deleteButton = target.closest("[data-csv-template-target-draft-delete]");
      if (deleteButton) {
        draftItems = draftItems.filter((item) => item.id !== deleteButton.getAttribute("data-csv-template-target-draft-delete"));
        renderDraftItems();
      }
    });

    draftSaveButton?.addEventListener("click", async () => {
      if (!csvTemplateId || !draftItems.length) return;
      draftSaveButton.disabled = true;
      draftSaveButton.classList.add("disabled");
      if (draftSaveMessage) draftSaveMessage.textContent = "保存中...";
      try {
        const payload = await postJson(`/api/admin/csv-mapping-templates/${csvTemplateId}/screen-rules`, {
          items: draftItems,
        });
        const message = payload?.message || "追加したマッピングを保存しました。";
        window.location.href = `${window.location.pathname}?message=${encodeURIComponent(message)}`;
      } catch (error) {
        if (draftSaveMessage) draftSaveMessage.textContent = error.message || "保存でエラーが発生しました。";
        updateDraftSaveState();
      }
    });

    ruleEditButtons.forEach((button) => {
      button.addEventListener("click", () => {
        try {
          const draft = JSON.parse(button.getAttribute("data-csv-template-rule-edit") || "{}");
          if (draft?.ruleId) loadDraftForEdit(draft);
        } catch (_error) {
          setComposerMessage("編集データを読み取れませんでした。");
        }
      });
    });

    ruleDeleteButtons.forEach((button) => {
      button.addEventListener("click", async () => {
        const ruleId = button.getAttribute("data-csv-template-rule-delete") || "";
        if (!csvTemplateId || !ruleId) return;
        const label = button.getAttribute("data-csv-template-rule-delete-label") || "このルール";
        if (!window.confirm(`${label} を削除します。よろしいですか？`)) return;
        button.disabled = true;
        button.classList.add("disabled");
        setComposerMessage("マッピングルールを削除中...");
        try {
          const response = await fetch(`/api/admin/csv-mapping-templates/${csvTemplateId}/screen-rules/${encodeURIComponent(ruleId)}`, {
            method: "DELETE",
            headers: { Accept: "application/json" },
          });
          const payload = await response.json().catch(() => ({}));
          if (!response.ok) throw new Error(payload.message || "削除でエラーが発生しました。");
          const message = payload.message || "マッピングルールを削除しました。";
          window.location.href = `${window.location.pathname}?message=${encodeURIComponent(message)}`;
        } catch (error) {
          setComposerMessage(error.message || "削除でエラーが発生しました。");
          button.disabled = false;
          button.classList.remove("disabled");
        }
      });
    });

    csvTemplateTargetModal.addEventListener("click", (event) => {
      const target = event.target;
      if (target instanceof Element && target.matches("[data-modal-close], [data-modal-close] *")) {
        focusedItem = null;
      }
    });

    searchTemplateTargetItems();
    setTargetKind("exam");
    renderSelectedTargets();
    renderHeaderCandidateCards();
    filterHeaderCandidateCards();
    renderDraftItems();
  }

  const externalFeedbackMemberResults = document.querySelector("[data-external-feedback-member-results]");
  if (externalFeedbackMemberResults) {
    const searchButton = document.querySelector("[data-external-feedback-member-search]");
    const applyButton = document.querySelector("[data-external-feedback-member-apply]");
    const emptyRow = externalFeedbackMemberResults.querySelector("[data-external-feedback-member-empty]");
    let selectedMember = null;

    const setExternalFeedbackMemberMessage = (message) => {
      externalFeedbackMemberResults.innerHTML = `<tr><td colspan="5">${escapeHtml(message)}</td></tr>`;
    };

    const setSelectedExternalFeedbackMember = (member) => {
      selectedMember = member;
      for (const row of externalFeedbackMemberResults.querySelectorAll("[data-external-feedback-member-row]")) {
        const isSelected = selectedMember && row.dataset.memberId === String(selectedMember.xml_export_member_id || "");
        row.classList.toggle("is-selected", Boolean(isSelected));
        const button = row.querySelector("[data-external-feedback-member-pick]");
        if (button) {
          button.textContent = isSelected ? "選択中" : "選ぶ";
          button.classList.toggle("is-active", Boolean(isSelected));
        }
      }
      if (applyButton) {
        applyButton.disabled = !selectedMember;
        applyButton.classList.toggle("disabled", !selectedMember);
      }
    };

    const renderExternalFeedbackMemberRows = (items) => {
      externalFeedbackMemberResults.innerHTML = "";
      if (!items.length) {
        if (emptyRow) {
          externalFeedbackMemberResults.appendChild(emptyRow);
          emptyRow.hidden = false;
          emptyRow.querySelector("td").textContent = "一致するHIA memberはありません。";
        } else {
          setExternalFeedbackMemberMessage("一致するHIA memberはありません。");
        }
        setSelectedExternalFeedbackMember(null);
        return;
      }
      for (const item of items) {
        const row = document.createElement("tr");
        row.setAttribute("data-external-feedback-member-row", "true");
        row.dataset.memberId = String(item.xml_export_member_id || "");
        const person = `${item.name_kana || "-"} / ${item.name_full || "-"}`;
        const insurance = `${item.insurance_symbol || "-"}-${item.insurance_number || "-"}`;
        row.innerHTML = `
          <td><strong>${escapeHtml(item.xml_export_member_id || "-")}</strong><small>zip ${escapeHtml(item.xml_export_zip_id || "-")} / list ${escapeHtml(item.xml_export_list_id || "-")}</small></td>
          <td><strong>${escapeHtml(person)}</strong><small>HIA ${escapeHtml(item.hia_subscriber_id || "-")} / subscriber ${escapeHtml(item.subscriber_id || "-")}</small><small>${escapeHtml(insurance)} / ${escapeHtml(item.exam_date || "-")}</small></td>
          <td><strong>${escapeHtml(item.zip_file_name || "-")}</strong><small>${escapeHtml(item.person_xml_file_name || "-")}</small><small>${escapeHtml(item.facility_name || "-")}</small></td>
          <td><strong>case ${escapeHtml(item.exam_export_case_id || "-")}</strong><small>出力 ${escapeHtml(item.export_readiness_status || "-")} / XML ${escapeHtml(item.xml_export_status || "-")}</small><small>HIA ${escapeHtml(item.hia_upload_status || "-")}</small></td>
          <td><button type="button" class="ghost-button compact-action-button" data-external-feedback-member-pick>選ぶ</button></td>
        `;
        row.addEventListener("click", (event) => {
          if (event.target.closest("button")) {
            event.preventDefault();
          }
          setSelectedExternalFeedbackMember(item);
        });
        row.querySelector("[data-external-feedback-member-pick]")?.addEventListener("click", (event) => {
          event.preventDefault();
          setSelectedExternalFeedbackMember(item);
        });
        externalFeedbackMemberResults.appendChild(row);
      }
      setSelectedExternalFeedbackMember(null);
    };

    searchButton?.addEventListener("click", async () => {
      const params = new URLSearchParams({
        event_id: document.querySelector("#external-feedback-event-id")?.value?.trim() || "",
        q: document.querySelector("#external-feedback-member-search-q")?.value?.trim() || "",
        name_kana: document.querySelector("#external-feedback-member-search-kana")?.value?.trim() || "",
        xml_export_member_id: document.querySelector("#external-feedback-member-search-member-id")?.value?.trim() || "",
        exam_export_case_id: document.querySelector("#external-feedback-member-search-case-id")?.value?.trim() || "",
      });
      setExternalFeedbackMemberMessage("検索中です...");
      try {
        const payload = await fetchJson(`/api/external-feedback/hia-members?${params.toString()}`);
        renderExternalFeedbackMemberRows(payload.items || []);
      } catch (error) {
        setExternalFeedbackMemberMessage(`検索でエラーが発生しました。${error.message || ""}`);
      }
    });

    for (const input of document.querySelectorAll("#external-feedback-member-picker-modal input")) {
      input.addEventListener("keydown", (event) => {
        if (event.key !== "Enter") return;
        event.preventDefault();
        searchButton?.click();
      });
    }

    applyButton?.addEventListener("click", () => {
      if (!selectedMember) return;
      const setValue = (selector, value) => {
        const input = document.querySelector(selector);
        if (input) input.value = value || "";
      };
      setValue("#external-feedback-member-id", selectedMember.xml_export_member_id);
      setValue("input[name='xml_export_zip_id']", selectedMember.xml_export_zip_id);
      setValue("input[name='xml_export_list_id']", selectedMember.xml_export_list_id);
      setValue("#external-feedback-event-id", selectedMember.event_id);
      setValue("input[name='exam_export_case_id']", selectedMember.exam_export_case_id);
      setValue("input[name='source_xml_file_name']", selectedMember.person_xml_file_name);
      setValue("input[name='source_zip_file_name']", selectedMember.zip_file_name);
      document.querySelector("#external-feedback-member-picker-modal [data-modal-close]")?.click();
    });
  }
})();
