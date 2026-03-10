"""
Data Warehouse Dimensional Modeling Toolkit

Star/snowflake schema generator, SCD Types 1/2/3, fact tables,
surrogate key management, and ETL patterns.

Author: Gabriel Demetrios Lafis
"""

import sqlite3
import hashlib
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple


class SurrogateKeyManager:
    """Manages surrogate key generation and natural-key mapping."""

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self.cursor = conn.cursor()
        self._ensure_registry()

    def _ensure_registry(self):
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS _sk_registry (
                table_name TEXT,
                natural_key TEXT,
                surrogate_key INTEGER,
                created_at TEXT DEFAULT (datetime('now')),
                PRIMARY KEY (table_name, natural_key)
            )
        """)
        self.conn.commit()

    def get_or_create(self, table_name: str, natural_key: str) -> int:
        """Return existing SK or assign a new one."""
        self.cursor.execute(
            "SELECT surrogate_key FROM _sk_registry WHERE table_name = ? AND natural_key = ?",
            (table_name, natural_key),
        )
        row = self.cursor.fetchone()
        if row:
            return row[0]
        self.cursor.execute(
            "SELECT COALESCE(MAX(surrogate_key), 0) + 1 FROM _sk_registry WHERE table_name = ?",
            (table_name,),
        )
        new_sk = self.cursor.fetchone()[0]
        self.cursor.execute(
            "INSERT INTO _sk_registry (table_name, natural_key, surrogate_key) VALUES (?, ?, ?)",
            (table_name, natural_key, new_sk),
        )
        self.conn.commit()
        return new_sk

    def lookup(self, table_name: str, natural_key: str) -> Optional[int]:
        self.cursor.execute(
            "SELECT surrogate_key FROM _sk_registry WHERE table_name = ? AND natural_key = ?",
            (table_name, natural_key),
        )
        row = self.cursor.fetchone()
        return row[0] if row else None


class DimensionTable:
    """Base dimension table with configurable SCD strategy."""

    SCD_TYPES = (1, 2, 3)

    def __init__(self, conn: sqlite3.Connection, name: str, columns: Dict[str, str],
                 natural_key: str, scd_type: int = 2):
        if scd_type not in self.SCD_TYPES:
            raise ValueError(f"scd_type must be one of {self.SCD_TYPES}")
        self.conn = conn
        self.cursor = conn.cursor()
        self.name = name
        self.columns = columns
        self.natural_key = natural_key
        self.scd_type = scd_type
        self.sk_manager = SurrogateKeyManager(conn)
        self._create_table()

    def _create_table(self):
        col_defs = ["sk_id INTEGER PRIMARY KEY AUTOINCREMENT"]
        for col, dtype in self.columns.items():
            col_defs.append(f"{col} {dtype}")
        if self.scd_type == 2:
            col_defs += [
                "valid_from TEXT NOT NULL DEFAULT (date('now'))",
                "valid_to TEXT DEFAULT '9999-12-31'",
                "is_current INTEGER DEFAULT 1",
                "row_hash TEXT",
            ]
        elif self.scd_type == 3:
            for col in self.columns:
                if col != self.natural_key:
                    col_defs.append(f"previous_{col} {self.columns[col]}")
            col_defs.append("last_changed TEXT")
        sql = f"CREATE TABLE IF NOT EXISTS {self.name} ({', '.join(col_defs)})"
        self.cursor.execute(sql)
        self.conn.commit()

    def _hash(self, row: Dict) -> str:
        data = "|".join(str(row.get(c, "")) for c in self.columns if c != self.natural_key)
        return hashlib.md5(data.encode()).hexdigest()

    def load(self, row: Dict[str, Any], effective_date: str = None) -> str:
        """Load a row using the configured SCD strategy.

        Returns 'inserted', 'updated', or 'unchanged'.
        """
        if effective_date is None:
            effective_date = datetime.now().strftime("%Y-%m-%d")
        if self.scd_type == 1:
            return self._load_scd1(row)
        elif self.scd_type == 2:
            return self._load_scd2(row, effective_date)
        else:
            return self._load_scd3(row, effective_date)

    def _load_scd1(self, row: Dict) -> str:
        nk_val = row[self.natural_key]
        self.cursor.execute(
            f"SELECT sk_id FROM {self.name} WHERE {self.natural_key} = ?", (nk_val,)
        )
        existing = self.cursor.fetchone()
        if existing is None:
            cols = ", ".join(row.keys())
            phs = ", ".join(["?"] * len(row))
            self.cursor.execute(f"INSERT INTO {self.name} ({cols}) VALUES ({phs})", list(row.values()))
            self.conn.commit()
            return "inserted"
        sets = ", ".join(f"{c} = ?" for c in row if c != self.natural_key)
        vals = [row[c] for c in row if c != self.natural_key] + [nk_val]
        self.cursor.execute(f"UPDATE {self.name} SET {sets} WHERE {self.natural_key} = ?", vals)
        self.conn.commit()
        return "updated"

    def _load_scd2(self, row: Dict, effective_date: str) -> str:
        nk_val = row[self.natural_key]
        new_hash = self._hash(row)
        self.cursor.execute(
            f"SELECT sk_id, row_hash FROM {self.name} WHERE {self.natural_key} = ? AND is_current = 1",
            (nk_val,),
        )
        existing = self.cursor.fetchone()
        if existing is None:
            cols = ", ".join(list(row.keys()) + ["valid_from", "row_hash"])
            phs = ", ".join(["?"] * (len(row) + 2))
            self.cursor.execute(
                f"INSERT INTO {self.name} ({cols}) VALUES ({phs})",
                list(row.values()) + [effective_date, new_hash],
            )
            self.conn.commit()
            return "inserted"
        if existing[1] == new_hash:
            return "unchanged"
        self.cursor.execute(
            f"UPDATE {self.name} SET valid_to = ?, is_current = 0 WHERE sk_id = ?",
            (effective_date, existing[0]),
        )
        cols = ", ".join(list(row.keys()) + ["valid_from", "row_hash"])
        phs = ", ".join(["?"] * (len(row) + 2))
        self.cursor.execute(
            f"INSERT INTO {self.name} ({cols}) VALUES ({phs})",
            list(row.values()) + [effective_date, new_hash],
        )
        self.conn.commit()
        return "updated"

    def _load_scd3(self, row: Dict, effective_date: str) -> str:
        nk_val = row[self.natural_key]
        self.cursor.execute(
            f"SELECT * FROM {self.name} WHERE {self.natural_key} = ?", (nk_val,)
        )
        existing = self.cursor.fetchone()
        if existing is None:
            cols = ", ".join(row.keys())
            phs = ", ".join(["?"] * len(row))
            self.cursor.execute(f"INSERT INTO {self.name} ({cols}) VALUES ({phs})", list(row.values()))
            self.conn.commit()
            return "inserted"
        col_names = [d[0] for d in self.cursor.description]
        old = dict(zip(col_names, existing))
        updates = {}
        for c in self.columns:
            if c != self.natural_key and row.get(c) != old.get(c):
                updates[f"previous_{c}"] = old.get(c)
                updates[c] = row[c]
        if not updates:
            return "unchanged"
        updates["last_changed"] = effective_date
        sets = ", ".join(f"{k} = ?" for k in updates)
        self.cursor.execute(
            f"UPDATE {self.name} SET {sets} WHERE {self.natural_key} = ?",
            list(updates.values()) + [nk_val],
        )
        self.conn.commit()
        return "updated"

    def get_current(self, natural_key_value: Any) -> Optional[Dict]:
        if self.scd_type == 2:
            self.cursor.execute(
                f"SELECT * FROM {self.name} WHERE {self.natural_key} = ? AND is_current = 1",
                (natural_key_value,),
            )
        else:
            self.cursor.execute(
                f"SELECT * FROM {self.name} WHERE {self.natural_key} = ?",
                (natural_key_value,),
            )
        row = self.cursor.fetchone()
        if row is None:
            return None
        cols = [d[0] for d in self.cursor.description]
        return dict(zip(cols, row))

    def get_all(self) -> List[Dict]:
        self.cursor.execute(f"SELECT * FROM {self.name}")
        cols = [d[0] for d in self.cursor.description]
        return [dict(zip(cols, r)) for r in self.cursor.fetchall()]


class FactTable:
    """Manages a fact table linked to dimension tables."""

    def __init__(self, conn: sqlite3.Connection, name: str,
                 measures: Dict[str, str], dimension_keys: Dict[str, str]):
        self.conn = conn
        self.cursor = conn.cursor()
        self.name = name
        self.measures = measures
        self.dimension_keys = dimension_keys
        self._create_table()

    def _create_table(self):
        col_defs = ["fact_id INTEGER PRIMARY KEY AUTOINCREMENT"]
        for fk, ref in self.dimension_keys.items():
            col_defs.append(f"{fk} INTEGER REFERENCES {ref}(sk_id)")
        for m, dt in self.measures.items():
            col_defs.append(f"{m} {dt}")
        col_defs.append("loaded_at TEXT DEFAULT (datetime('now'))")
        self.cursor.execute(f"CREATE TABLE IF NOT EXISTS {self.name} ({', '.join(col_defs)})")
        self.conn.commit()

    def insert(self, row: Dict[str, Any]) -> int:
        cols = ", ".join(row.keys())
        phs = ", ".join(["?"] * len(row))
        self.cursor.execute(f"INSERT INTO {self.name} ({cols}) VALUES ({phs})", list(row.values()))
        self.conn.commit()
        return self.cursor.lastrowid

    def bulk_insert(self, rows: List[Dict[str, Any]]) -> int:
        if not rows:
            return 0
        cols = ", ".join(rows[0].keys())
        phs = ", ".join(["?"] * len(rows[0]))
        self.cursor.executemany(
            f"INSERT INTO {self.name} ({cols}) VALUES ({phs})",
            [list(r.values()) for r in rows],
        )
        self.conn.commit()
        return len(rows)

    def aggregate(self, group_cols: List[str], measure: str, func: str = "SUM") -> List[Dict]:
        group = ", ".join(group_cols)
        sql = f"SELECT {group}, {func}({measure}) AS agg_value FROM {self.name} GROUP BY {group}"
        self.cursor.execute(sql)
        cols = [d[0] for d in self.cursor.description]
        return [dict(zip(cols, r)) for r in self.cursor.fetchall()]

    def row_count(self) -> int:
        self.cursor.execute(f"SELECT COUNT(*) FROM {self.name}")
        return self.cursor.fetchone()[0]


class SchemaGenerator:
    """Generates star or snowflake schemas from a specification dict."""

    def __init__(self, db_path: str = ":memory:"):
        self.conn = sqlite3.connect(db_path)
        self.dimensions: Dict[str, DimensionTable] = {}
        self.facts: Dict[str, FactTable] = {}

    def build_star_schema(self, spec: Dict[str, Any]) -> Dict[str, Any]:
        """Build a star schema from a specification.

        Args:
            spec: Dict with 'dimensions' and 'facts' keys.
        """
        for dim_spec in spec.get("dimensions", []):
            dim = DimensionTable(
                self.conn,
                dim_spec["name"],
                dim_spec["columns"],
                dim_spec["natural_key"],
                dim_spec.get("scd_type", 2),
            )
            self.dimensions[dim_spec["name"]] = dim

        for fact_spec in spec.get("facts", []):
            fact = FactTable(
                self.conn,
                fact_spec["name"],
                fact_spec["measures"],
                fact_spec["dimension_keys"],
            )
            self.facts[fact_spec["name"]] = fact

        return {
            "dimensions": list(self.dimensions.keys()),
            "facts": list(self.facts.keys()),
            "schema_type": "star",
        }

    def close(self):
        self.conn.close()
