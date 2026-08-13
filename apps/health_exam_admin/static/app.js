(() => {
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
    };
    filters.set(tableSelector, tableFilters);
    const emptyMessage = table.dataset.emptyMessage || "一致する行はありません。";
    const emptyRow = document.createElement("tr");
    emptyRow.hidden = true;
    emptyRow.innerHTML = `<td colspan="${table.querySelectorAll("thead th").length || 1}">${emptyMessage}</td>`;
    table.querySelector("tbody").appendChild(emptyRow);

    const applyFilter = () => {
      const keyword = normalize(input.value);
      let visibleCount = 0;
      for (const row of rows) {
        const keywordMatched = !keyword || normalize(row.dataset.filterText).includes(keyword);
        let togglesMatched = true;
        for (const [field, values] of tableFilters.toggleGroups.entries()) {
          if (!values.size) continue;
          if (!values.has(String(row.dataset[field] || ""))) {
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

    button.setAttribute("aria-pressed", "false");
    button.addEventListener("click", () => {
      const group = tableFilters.toggleGroups.get(field) || new Set();
      if (group.has(value)) {
        group.delete(value);
        button.classList.remove("is-active");
        button.setAttribute("aria-pressed", "false");
      } else {
        group.add(value);
        button.classList.add("is-active");
        button.setAttribute("aria-pressed", "true");
      }
      tableFilters.toggleGroups.set(field, group);
      tableFilters.applyFilter();
    });
  }

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
})();
