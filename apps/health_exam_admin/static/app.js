(() => {
  const normalize = (value) => String(value || "").toLocaleLowerCase().replace(/\s+/g, "");

  for (const input of document.querySelectorAll("[data-live-filter-input]")) {
    const tableSelector = input.getAttribute("data-live-filter-input");
    const table = tableSelector ? document.querySelector(tableSelector) : null;
    if (!table) continue;

    const rows = Array.from(table.querySelectorAll("tbody tr[data-filter-text]"));
    const emptyMessage = table.dataset.emptyMessage || "一致する行はありません。";
    const emptyRow = document.createElement("tr");
    emptyRow.hidden = true;
    emptyRow.innerHTML = `<td colspan="${table.querySelectorAll("thead th").length || 1}">${emptyMessage}</td>`;
    table.querySelector("tbody").appendChild(emptyRow);

    const applyFilter = () => {
      const keyword = normalize(input.value);
      let visibleCount = 0;
      for (const row of rows) {
        const matched = !keyword || normalize(row.dataset.filterText).includes(keyword);
        row.hidden = !matched;
        if (matched) visibleCount += 1;
      }
      emptyRow.hidden = visibleCount !== 0;
    };

    input.addEventListener("input", applyFilter);
    applyFilter();
  }
})();
