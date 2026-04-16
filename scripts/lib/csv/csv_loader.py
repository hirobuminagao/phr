

import csv
import codecs
from typing import List, Dict, Any, Tuple, Iterator, Optional


class CSVLoader:
    def __init__(
        self,
        path: str,
        header_count: int = 1,
        delimiter: str = ",",
        encoding: Optional[str] = None,
    ):
        self.path = path
        self.header_count = header_count
        self.delimiter = delimiter
        self.encoding = encoding

        self._headers: List[List[str]] = []
        self._rows: List[List[str]] = []

    # ----------------------------
    # public API
    # ----------------------------
    def load(self) -> None:
        enc = self._detect_encoding() if not self.encoding else self.encoding

        with codecs.open(self.path, "r", encoding=enc) as f:
            reader = csv.reader(f, delimiter=self.delimiter)

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
) -> CSVLoader:
    loader = CSVLoader(
        path=path,
        header_count=header_count,
        delimiter=delimiter,
        encoding=encoding,
    )
    loader.load()
    return loader