

import csv
import codecs
import hashlib
import json
from dataclasses import dataclass
from typing import List, Dict, Any, Iterator, Optional


@dataclass(frozen=True)
class CsvHeaderColumn:
    column_no: int
    context: str | None
    header_name: str | None
    occurrence: int


@dataclass(frozen=True)
class CsvHeaderSet:
    header_rows: List[List[str]]
    active_header_row_no: int | None
    normalized_columns: List[CsvHeaderColumn]
    header_sha256: str


@dataclass(frozen=True)
class CsvLoadResult:
    path: str
    encoding: str
    delimiter: str
    quote_char: str
    header_set: CsvHeaderSet
    rows: List[List[str]]
    data_start_row_no: int


class CSVLoader:
    def __init__(
        self,
        path: str,
        header_count: int = 1,
        delimiter: str = ",",
        encoding: Optional[str] = None,
        quote_char: str = '"',
    ):
        self.path = path
        self.header_count = header_count
        self.delimiter = delimiter
        self.encoding = encoding
        self.quote_char = quote_char or '"'

        self._headers: List[List[str]] = []
        self._rows: List[List[str]] = []

    # ----------------------------
    # public API
    # ----------------------------
    def load(self) -> None:
        enc = self._detect_encoding() if not self.encoding else self.encoding
        self.encoding = enc

        with codecs.open(self.path, "r", encoding=enc) as f:
            reader = csv.reader(f, delimiter=self.delimiter, quotechar=self.quote_char)

            for i, row in enumerate(reader):
                row = self._normalize_row(row)

                if i < self.header_count:
                    self._headers.append(row)
                else:
                    self._rows.append(row)

    def get_headers(self) -> List[List[str]]:
        return self._headers

    def get_header_dict(self) -> Dict[str, int]:
        """
        最終ヘッダー行をキーにして index を返す
        """
        if not self._headers:
            raise ValueError("CSV not loaded")

        header = self._headers[-1]
        return {col: idx for idx, col in enumerate(header)}

    def iter_rows(self) -> Iterator[List[str]]:
        for row in self._rows:
            yield row

    def iter_dict_rows(self) -> Iterator[Dict[str, Any]]:
        header_map = self.get_header_dict()
        header = self._headers[-1]

        for row in self._rows:
            yield {
                header[i]: row[i] if i < len(row) else None
                for i in range(len(header))
            }

    def count_rows(self, exclude_header: bool = True) -> int:
        return len(self._rows) if exclude_header else len(self._rows) + len(self._headers)

    # ----------------------------
    # internal
    # ----------------------------
    def _detect_encoding(self) -> str:
        """
        簡易エンコーディング判定（UTF-8 BOM / UTF-8 / CP932）
        """
        with open(self.path, "rb") as f:
            raw = f.read(4096)

        if raw.startswith(codecs.BOM_UTF8):
            return "utf-8-sig"

        try:
            raw.decode("utf-8")
            return "utf-8"
        except UnicodeDecodeError:
            return "cp932"

    def _normalize_row(self, row: List[str]) -> List[str]:
        """
        BOM除去・前後空白削除
        """
        normalized = []
        for i, col in enumerate(row):
            if i == 0:
                col = col.lstrip("\ufeff")
            normalized.append(col.strip())
        return normalized


def load_csv(
    path: str,
    header_count: int = 1,
    delimiter: str = ",",
    encoding: Optional[str] = None,
    quote_char: str = '"',
) -> CSVLoader:
    loader = CSVLoader(
        path=path,
        header_count=header_count,
        delimiter=delimiter,
        encoding=encoding,
        quote_char=quote_char,
    )
    loader.load()
    return loader


def _json_sha256(value: Any) -> str:
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def row_sha256(row: List[str]) -> str:
    """Return a stable hash for a parsed CSV row, preserving column order."""

    return _json_sha256([str(cell) if cell is not None else None for cell in row])


def _build_header_columns(
    header_rows: List[List[str]],
    active_header_row_no: int | None,
) -> List[CsvHeaderColumn]:
    if not header_rows:
        return []

    active_index = (active_header_row_no - 1) if active_header_row_no else len(header_rows) - 1
    active_index = max(0, min(active_index, len(header_rows) - 1))
    active_row = header_rows[active_index]
    max_len = max(len(row) for row in header_rows)

    occurrences: dict[tuple[str | None, str | None], int] = {}
    columns: List[CsvHeaderColumn] = []
    carried_context: str | None = None

    for index in range(max_len):
        header_name = active_row[index] if index < len(active_row) else None
        header_name = header_name or None

        context: str | None = None
        if len(header_rows) > 1:
            context_parts: list[str] = []
            for row_index, row in enumerate(header_rows):
                if row_index == active_index:
                    continue
                part = row[index] if index < len(row) else ""
                if part:
                    carried_context = part
                if carried_context:
                    context_parts.append(carried_context)
            context = " / ".join(context_parts) if context_parts else None

        key = (context, header_name)
        occurrence = occurrences.get(key, 0) + 1
        occurrences[key] = occurrence
        columns.append(
            CsvHeaderColumn(
                column_no=index + 1,
                context=context,
                header_name=header_name,
                occurrence=occurrence,
            )
        )

    return columns


def _header_snapshot(header_set: CsvHeaderSet) -> dict[str, Any]:
    return {
        "active_header_row_no": header_set.active_header_row_no,
        "header_rows": header_set.header_rows,
        "normalized_columns": [
            {
                "column_no": column.column_no,
                "context": column.context,
                "header_name": column.header_name,
                "occurrence": column.occurrence,
            }
            for column in header_set.normalized_columns
        ],
    }


def load_csv_result(
    path: str,
    header_count: int = 1,
    delimiter: str = ",",
    encoding: Optional[str] = None,
    quote_char: str = '"',
    active_header_row_no: int | None = None,
    data_start_row_no: int | None = None,
) -> CsvLoadResult:
    """Load CSV into a structured result without changing the existing CSVLoader API."""

    loader = CSVLoader(
        path=path,
        header_count=header_count,
        delimiter=delimiter,
        encoding=encoding,
        quote_char=quote_char,
    )
    loader.load()
    header_rows = loader.get_headers()
    header_columns = _build_header_columns(header_rows, active_header_row_no)
    header_set_without_hash = CsvHeaderSet(
        header_rows=header_rows,
        active_header_row_no=active_header_row_no,
        normalized_columns=header_columns,
        header_sha256="",
    )
    header_sha256 = _json_sha256(_header_snapshot(header_set_without_hash))
    header_set = CsvHeaderSet(
        header_rows=header_rows,
        active_header_row_no=active_header_row_no,
        normalized_columns=header_columns,
        header_sha256=header_sha256,
    )

    return CsvLoadResult(
        path=path,
        encoding=loader.encoding or encoding or "unknown",
        delimiter=delimiter,
        quote_char=quote_char,
        header_set=header_set,
        rows=list(loader.iter_rows()),
        data_start_row_no=data_start_row_no or header_count + 1,
    )
