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
  const filters = new Map();

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
      input.value = input.value === value ? "" : value;
      form.requestSubmit();
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
      const firstInput = modal.querySelector("input:not([disabled]), select, textarea, button");
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
  const ledgerFacilityForm = ledgerFacilityInput ? ledgerFacilityInput.closest("form") : null;
  for (const button of document.querySelectorAll("[data-ledger-facility-select]")) {
    button.addEventListener("click", () => {
      if (!ledgerFacilityInput) return;
      const query = String(button.getAttribute("data-facility-query") || "").trim();
      if (!query) return;
      ledgerFacilityInput.value = query;
      ledgerFacilityInput.dispatchEvent(new Event("input", { bubbles: true }));
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
  for (const button of document.querySelectorAll("[data-ledger-facility-clear]")) {
    button.addEventListener("click", () => {
      if (!ledgerFacilityInput) return;
      ledgerFacilityInput.value = "";
      ledgerFacilityInput.dispatchEvent(new Event("input", { bubbles: true }));
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
  const caseFacilityForm = caseFacilityInput ? caseFacilityInput.closest("form") : null;
  for (const button of document.querySelectorAll("[data-case-facility-select]")) {
    button.addEventListener("click", () => {
      if (!caseFacilityInput) return;
      const query = String(button.getAttribute("data-facility-query") || "").trim();
      if (!query) return;
      caseFacilityInput.value = query;
      caseFacilityInput.dispatchEvent(new Event("input", { bubbles: true }));
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
  for (const button of document.querySelectorAll("[data-case-facility-clear]")) {
    button.addEventListener("click", () => {
      if (!caseFacilityInput) return;
      caseFacilityInput.value = "";
      caseFacilityInput.dispatchEvent(new Event("input", { bubbles: true }));
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

  const processingOverlay = document.querySelector("[data-processing-overlay]");
  const processingTitle = document.querySelector("[data-processing-overlay-title]");
  const processingMessage = document.querySelector("[data-processing-overlay-message]");
  for (const form of document.querySelectorAll("[data-processing-form]")) {
    form.addEventListener("submit", () => {
      const button = form.querySelector("button[type='submit']");
      const message = form.getAttribute("data-processing-message") || "処理しています";
      if (processingTitle) processingTitle.textContent = message;
      if (processingMessage) processingMessage.textContent = "ファイル数や内容によって時間がかかることがあります。";
      if (processingOverlay) processingOverlay.hidden = false;
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
})();
