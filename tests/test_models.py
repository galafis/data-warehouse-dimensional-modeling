"""
Tests for Data Warehouse Dimensional Modeling Toolkit.
"""

import sqlite3
import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from dimensional_modeling import (
    SurrogateKeyManager,
    DimensionTable,
    FactTable,
    SchemaGenerator,
)


class TestSurrogateKeyManager:
    def setup_method(self):
        self.conn = sqlite3.connect(":memory:")
        self.skm = SurrogateKeyManager(self.conn)

    def teardown_method(self):
        self.conn.close()

    def test_get_or_create_new(self):
        sk = self.skm.get_or_create("dim_product", "PROD-001")
        assert sk == 1

    def test_get_or_create_existing(self):
        sk1 = self.skm.get_or_create("dim_product", "PROD-001")
        sk2 = self.skm.get_or_create("dim_product", "PROD-001")
        assert sk1 == sk2

    def test_separate_tables_separate_keys(self):
        sk1 = self.skm.get_or_create("dim_a", "K1")
        sk2 = self.skm.get_or_create("dim_b", "K1")
        assert sk1 == 1
        assert sk2 == 1

    def test_lookup_missing(self):
        assert self.skm.lookup("dim_x", "NOPE") is None


class TestDimensionTableSCD1:
    def setup_method(self):
        self.conn = sqlite3.connect(":memory:")
        self.dim = DimensionTable(
            self.conn, "dim_product",
            {"product_id": "TEXT", "name": "TEXT", "category": "TEXT"},
            natural_key="product_id", scd_type=1,
        )

    def teardown_method(self):
        self.conn.close()

    def test_insert(self):
        result = self.dim.load({"product_id": "P1", "name": "Widget", "category": "Tools"})
        assert result == "inserted"

    def test_overwrite_on_update(self):
        self.dim.load({"product_id": "P1", "name": "Widget", "category": "Tools"})
        result = self.dim.load({"product_id": "P1", "name": "Super Widget", "category": "Tools"})
        assert result == "updated"
        current = self.dim.get_current("P1")
        assert current["name"] == "Super Widget"

    def test_get_all(self):
        self.dim.load({"product_id": "P1", "name": "A", "category": "C"})
        self.dim.load({"product_id": "P2", "name": "B", "category": "C"})
        assert len(self.dim.get_all()) == 2


class TestDimensionTableSCD2:
    def setup_method(self):
        self.conn = sqlite3.connect(":memory:")
        self.dim = DimensionTable(
            self.conn, "dim_customer",
            {"customer_id": "TEXT", "name": "TEXT", "city": "TEXT"},
            natural_key="customer_id", scd_type=2,
        )

    def teardown_method(self):
        self.conn.close()

    def test_insert_new(self):
        result = self.dim.load({"customer_id": "C1", "name": "Alice", "city": "SP"}, "2024-01-01")
        assert result == "inserted"

    def test_unchanged(self):
        self.dim.load({"customer_id": "C1", "name": "Alice", "city": "SP"}, "2024-01-01")
        result = self.dim.load({"customer_id": "C1", "name": "Alice", "city": "SP"}, "2024-06-01")
        assert result == "unchanged"

    def test_update_creates_version(self):
        self.dim.load({"customer_id": "C1", "name": "Alice", "city": "SP"}, "2024-01-01")
        self.dim.load({"customer_id": "C1", "name": "Alice", "city": "RJ"}, "2024-06-01")
        rows = self.dim.get_all()
        assert len(rows) == 2
        current = self.dim.get_current("C1")
        assert current["city"] == "RJ"
        assert current["is_current"] == 1


class TestDimensionTableSCD3:
    def setup_method(self):
        self.conn = sqlite3.connect(":memory:")
        self.dim = DimensionTable(
            self.conn, "dim_employee",
            {"emp_id": "TEXT", "department": "TEXT", "title": "TEXT"},
            natural_key="emp_id", scd_type=3,
        )

    def teardown_method(self):
        self.conn.close()

    def test_insert(self):
        result = self.dim.load({"emp_id": "E1", "department": "Eng", "title": "Dev"}, "2024-01-01")
        assert result == "inserted"

    def test_update_preserves_previous(self):
        self.dim.load({"emp_id": "E1", "department": "Eng", "title": "Dev"}, "2024-01-01")
        self.dim.load({"emp_id": "E1", "department": "Mgmt", "title": "Lead"}, "2024-06-01")
        current = self.dim.get_current("E1")
        assert current["department"] == "Mgmt"
        assert current["previous_department"] == "Eng"
        assert current["last_changed"] == "2024-06-01"


class TestFactTable:
    def setup_method(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.execute("CREATE TABLE dim_product (sk_id INTEGER PRIMARY KEY)")
        self.conn.execute("INSERT INTO dim_product VALUES (1)")
        self.fact = FactTable(
            self.conn, "fact_sales",
            measures={"amount": "REAL", "quantity": "INTEGER"},
            dimension_keys={"product_sk": "dim_product"},
        )

    def teardown_method(self):
        self.conn.close()

    def test_insert(self):
        fid = self.fact.insert({"product_sk": 1, "amount": 100.0, "quantity": 5})
        assert fid == 1

    def test_bulk_insert(self):
        rows = [{"product_sk": 1, "amount": i * 10.0, "quantity": i} for i in range(1, 6)]
        count = self.fact.bulk_insert(rows)
        assert count == 5
        assert self.fact.row_count() == 5

    def test_aggregate(self):
        self.fact.insert({"product_sk": 1, "amount": 100.0, "quantity": 5})
        self.fact.insert({"product_sk": 1, "amount": 200.0, "quantity": 3})
        result = self.fact.aggregate(["product_sk"], "amount", "SUM")
        assert result[0]["agg_value"] == 300.0


class TestSchemaGenerator:
    def test_build_star_schema(self):
        gen = SchemaGenerator()
        spec = {
            "dimensions": [
                {"name": "dim_product", "columns": {"product_id": "TEXT", "name": "TEXT"},
                 "natural_key": "product_id", "scd_type": 1},
            ],
            "facts": [
                {"name": "fact_sales", "measures": {"revenue": "REAL"},
                 "dimension_keys": {"product_sk": "dim_product"}},
            ],
        }
        result = gen.build_star_schema(spec)
        assert "dim_product" in result["dimensions"]
        assert "fact_sales" in result["facts"]
        assert result["schema_type"] == "star"
        gen.close()

    def test_invalid_scd_type(self):
        conn = sqlite3.connect(":memory:")
        with pytest.raises(ValueError):
            DimensionTable(conn, "bad", {"id": "TEXT"}, "id", scd_type=99)
        conn.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
