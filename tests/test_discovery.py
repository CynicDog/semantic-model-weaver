"""
Unit tests for weaver.discovery — no Snowflake connection required.

All Snowflake I/O (session.sql, session.table) is mocked. What is tested:

  _normalize_type          — Snowflake internal type name → DSL DataType string
  _type_family             — coarse type family for FK compatibility checks
  _infer_fk_candidates     — FK candidate inference logic across tables
  SchemaDiscovery._fetch_tables   — INFORMATION_SCHEMA.TABLES row mapping
  SchemaDiscovery._fetch_columns  — INFORMATION_SCHEMA.COLUMNS row mapping
  SchemaDiscovery._sample_table   — text/bool column sampling, dedup, failure fallback
  SchemaDiscovery.run             — end-to-end SchemaProfile assembly
"""

from unittest.mock import MagicMock


def _table_row(name, comment="", row_count=0):
    return {"TABLE_NAME": name, "COMMENT": comment, "ROW_COUNT": row_count}


def _column_row(table, name, data_type, nullable="YES", comment=""):
    return {
        "TABLE_NAME": table,
        "COLUMN_NAME": name,
        "DATA_TYPE": data_type,
        "IS_NULLABLE": nullable,
        "COMMENT": comment,
    }


def _col(name, type_, nullable=True, comment="", sample_values=None):
    """Build a minimal ColumnProfile dict for use in _infer_fk_candidates tests."""
    return {
        "name": name,
        "type": type_,
        "nullable": nullable,
        "comment": comment,
        "sample_values": sample_values or [],
    }


def _table_profile(name, columns):
    """Build a minimal TableProfile for _infer_fk_candidates tests."""
    return {
        "name": name,
        "comment": "",
        "row_count": 0,
        "columns": columns,
        "fk_candidates": [],
    }


def _mock_session(table_rows=None, column_rows=None, sample_rows=None):
    """
    Return a MagicMock session wired for a single SchemaDiscovery.run() call.

    session.sql() is called twice in order: first for tables, then for columns.
    session.table().select().sample().collect() returns sample_rows for every table.
    """
    session = MagicMock()

    tables_df = MagicMock()
    tables_df.collect.return_value = table_rows or []

    columns_df = MagicMock()
    columns_df.collect.return_value = column_rows or []

    session.sql.side_effect = [tables_df, columns_df]

    if sample_rows is not None:
        (
            session.table.return_value
            .select.return_value
            .sample.return_value
            .collect.return_value
        ) = sample_rows

    return session


class TestNormalizeType:
    def test_text_maps_to_varchar(self):
        from weaver.discovery import _normalize_type
        assert _normalize_type("TEXT") == "VARCHAR"

    def test_fixed_maps_to_number(self):
        from weaver.discovery import _normalize_type
        assert _normalize_type("FIXED") == "NUMBER"

    def test_real_maps_to_float(self):
        from weaver.discovery import _normalize_type
        assert _normalize_type("REAL") == "FLOAT"

    def test_date_maps_to_date(self):
        from weaver.discovery import _normalize_type
        assert _normalize_type("DATE") == "DATE"

    def test_timestamp_ntz_preserved(self):
        from weaver.discovery import _normalize_type
        assert _normalize_type("TIMESTAMP_NTZ") == "TIMESTAMP_NTZ"

    def test_timestamp_ltz_preserved(self):
        from weaver.discovery import _normalize_type
        assert _normalize_type("TIMESTAMP_LTZ") == "TIMESTAMP_LTZ"

    def test_boolean_preserved(self):
        from weaver.discovery import _normalize_type
        assert _normalize_type("BOOLEAN") == "BOOLEAN"

    def test_variant_preserved(self):
        from weaver.discovery import _normalize_type
        assert _normalize_type("VARIANT") == "VARIANT"

    def test_unknown_type_falls_back_to_varchar(self):
        from weaver.discovery import _normalize_type
        assert _normalize_type("EXOTIC_TYPE") == "VARCHAR"

    def test_case_insensitive(self):
        from weaver.discovery import _normalize_type
        assert _normalize_type("text") == "VARCHAR"
        assert _normalize_type("fixed") == "NUMBER"
        assert _normalize_type("real") == "FLOAT"

    def test_strips_precision_parens(self):
        from weaver.discovery import _normalize_type
        assert _normalize_type("NUMBER(38,0)") == "NUMBER"
        assert _normalize_type("VARCHAR(256)") == "VARCHAR"



class TestTypeFamily:
    def test_varchar_is_text_family(self):
        from weaver.discovery import _type_family
        assert _type_family("VARCHAR") == "text"

    def test_number_is_numeric_family(self):
        from weaver.discovery import _type_family
        assert _type_family("NUMBER") == "numeric"

    def test_float_is_numeric_family(self):
        from weaver.discovery import _type_family
        assert _type_family("FLOAT") == "numeric"

    def test_date_is_date_family(self):
        from weaver.discovery import _type_family
        assert _type_family("DATE") == "date"

    def test_timestamp_ntz_is_timestamp_family(self):
        from weaver.discovery import _type_family
        assert _type_family("TIMESTAMP_NTZ") == "timestamp"

    def test_timestamp_ltz_is_timestamp_family(self):
        from weaver.discovery import _type_family
        assert _type_family("TIMESTAMP_LTZ") == "timestamp"

    def test_accepts_raw_snowflake_type(self):
        """_type_family normalises internally, so FIXED (raw) → numeric."""
        from weaver.discovery import _type_family
        assert _type_family("FIXED") == "numeric"



class TestInferFkCandidates:
    def test_single_table_produces_no_candidates(self):
        from weaver.discovery import _infer_fk_candidates
        tables = [_table_profile("T1", [_col("ISU_CD", "VARCHAR")])]
        _infer_fk_candidates(tables)
        assert tables[0]["fk_candidates"] == []

    def test_matching_name_and_type_family_is_candidate(self):
        from weaver.discovery import _infer_fk_candidates
        tables = [
            _table_profile("TRADES", [_col("ISU_CD", "VARCHAR")]),
            _table_profile("STOCKS", [_col("ISU_CD", "VARCHAR")]),
        ]
        _infer_fk_candidates(tables)
        candidates = tables[0]["fk_candidates"]
        assert len(candidates) == 1
        assert candidates[0]["column"] == "ISU_CD"

    def test_incompatible_type_family_not_candidate(self):
        from weaver.discovery import _infer_fk_candidates
        tables = [
            _table_profile("A", [_col("ID", "VARCHAR")]),
            _table_profile("B", [_col("ID", "NUMBER")]),
        ]
        _infer_fk_candidates(tables)
        assert tables[0]["fk_candidates"] == []
        assert tables[1]["fk_candidates"] == []

    def test_column_unique_to_one_table_not_candidate(self):
        from weaver.discovery import _infer_fk_candidates
        tables = [
            _table_profile("T1", [_col("ONLY_HERE", "VARCHAR")]),
            _table_profile("T2", [_col("OTHER_COL", "VARCHAR")]),
        ]
        _infer_fk_candidates(tables)
        assert tables[0]["fk_candidates"] == []
        assert tables[1]["fk_candidates"] == []

    def test_match_format_is_table_dot_column(self):
        from weaver.discovery import _infer_fk_candidates
        tables = [
            _table_profile("TRADES", [_col("ISU_CD", "VARCHAR")]),
            _table_profile("STOCKS", [_col("ISU_CD", "VARCHAR")]),
        ]
        _infer_fk_candidates(tables)
        match = tables[0]["fk_candidates"][0]["matches"][0]
        assert match == "STOCKS.ISU_CD"

    def test_does_not_include_self_in_matches(self):
        from weaver.discovery import _infer_fk_candidates
        tables = [
            _table_profile("T1", [_col("ID", "VARCHAR")]),
            _table_profile("T2", [_col("ID", "VARCHAR")]),
        ]
        _infer_fk_candidates(tables)
        for table in tables:
            for cand in table["fk_candidates"]:
                for match in cand["matches"]:
                    assert not match.startswith(table["name"])

    def test_three_tables_sharing_column_has_two_matches(self):
        from weaver.discovery import _infer_fk_candidates
        tables = [
            _table_profile("A", [_col("ISU_CD", "VARCHAR")]),
            _table_profile("B", [_col("ISU_CD", "VARCHAR")]),
            _table_profile("C", [_col("ISU_CD", "VARCHAR")]),
        ]
        _infer_fk_candidates(tables)
        assert len(tables[0]["fk_candidates"][0]["matches"]) == 2

    def test_multiple_fk_columns_per_table(self):
        from weaver.discovery import _infer_fk_candidates
        tables = [
            _table_profile("T1", [_col("ISU_CD", "VARCHAR"), _col("MKT_ID", "VARCHAR")]),
            _table_profile("T2", [_col("ISU_CD", "VARCHAR"), _col("MKT_ID", "VARCHAR")]),
        ]
        _infer_fk_candidates(tables)
        assert len(tables[0]["fk_candidates"]) == 2



class TestFetchTables:
    def test_returns_correct_structure(self):
        from weaver.discovery import SchemaDiscovery

        session = MagicMock()
        session.sql.return_value.collect.return_value = [
            _table_row("NX_HT_BAT_REFER_A0", "Stock reference", 72165)
        ]
        result = SchemaDiscovery(session)._fetch_tables("NEXTRADE", "FIN")

        assert len(result) == 1
        row = result[0]
        assert row["name"] == "NX_HT_BAT_REFER_A0"
        assert row["comment"] == "Stock reference"
        assert row["row_count"] == 72165

    def test_null_comment_becomes_empty_string(self):
        from weaver.discovery import SchemaDiscovery

        session = MagicMock()
        session.sql.return_value.collect.return_value = [
            _table_row("T1", comment=None, row_count=0)
        ]
        result = SchemaDiscovery(session)._fetch_tables("DB", "SCH")
        assert result[0]["comment"] == ""

    def test_null_row_count_becomes_zero(self):
        from weaver.discovery import SchemaDiscovery

        session = MagicMock()
        session.sql.return_value.collect.return_value = [
            _table_row("T1", row_count=None)
        ]
        result = SchemaDiscovery(session)._fetch_tables("DB", "SCH")
        assert result[0]["row_count"] == 0

    def test_sql_targets_correct_database_and_schema(self):
        from weaver.discovery import SchemaDiscovery

        session = MagicMock()
        session.sql.return_value.collect.return_value = []
        SchemaDiscovery(session)._fetch_tables("MYDB", "MYSCHEMA")

        sql_text = session.sql.call_args[0][0]
        assert "MYDB" in sql_text
        assert "MYSCHEMA" in sql_text



class TestFetchColumns:
    def test_returns_columns_with_correct_fields(self):
        from weaver.discovery import SchemaDiscovery

        session = MagicMock()
        session.sql.return_value.collect.return_value = [
            _column_row("T1", "ISU_CD", "TEXT", nullable="NO", comment="Issue code")
        ]
        result = SchemaDiscovery(session)._fetch_columns("DB", "SCH")

        assert len(result) == 1
        col = result[0]
        assert col["table_name"] == "T1"
        assert col["name"] == "ISU_CD"
        assert col["raw_type"] == "TEXT"
        assert col["nullable"] is False
        assert col["comment"] == "Issue code"

    def test_is_nullable_yes_maps_to_true(self):
        from weaver.discovery import SchemaDiscovery

        session = MagicMock()
        session.sql.return_value.collect.return_value = [
            _column_row("T1", "COL", "TEXT", nullable="YES")
        ]
        result = SchemaDiscovery(session)._fetch_columns("DB", "SCH")
        assert result[0]["nullable"] is True

    def test_is_nullable_no_maps_to_false(self):
        from weaver.discovery import SchemaDiscovery

        session = MagicMock()
        session.sql.return_value.collect.return_value = [
            _column_row("T1", "COL", "TEXT", nullable="NO")
        ]
        result = SchemaDiscovery(session)._fetch_columns("DB", "SCH")
        assert result[0]["nullable"] is False

    def test_null_comment_becomes_empty_string(self):
        from weaver.discovery import SchemaDiscovery

        session = MagicMock()
        session.sql.return_value.collect.return_value = [
            _column_row("T1", "COL", "TEXT", comment=None)
        ]
        result = SchemaDiscovery(session)._fetch_columns("DB", "SCH")
        assert result[0]["comment"] == ""



class TestSampleTable:
    def _make_session(self, sample_rows):
        session = MagicMock()
        (
            session.table.return_value
            .select.return_value
            .sample.return_value
            .collect.return_value
        ) = sample_rows
        return session

    def test_text_columns_appear_in_result(self):
        from weaver.discovery import SchemaDiscovery

        cols = [{"name": "MKT_ID", "raw_type": "TEXT"}]
        session = self._make_session([{"MKT_ID": "STK"}, {"MKT_ID": "KSQ"}])

        result = SchemaDiscovery(session)._sample_table("DB", "SCH", "T", cols)
        assert "MKT_ID" in result
        assert set(result["MKT_ID"]) == {"STK", "KSQ"}

    def test_numeric_columns_are_not_sampled(self):
        from weaver.discovery import SchemaDiscovery

        cols = [{"name": "PRICE", "raw_type": "FIXED"}]
        session = self._make_session([])

        result = SchemaDiscovery(session)._sample_table("DB", "SCH", "T", cols)
        assert result == {}
        session.table.assert_not_called()

    def test_date_columns_are_not_sampled(self):
        from weaver.discovery import SchemaDiscovery

        cols = [{"name": "TRD_DT", "raw_type": "DATE"}]
        session = self._make_session([])

        result = SchemaDiscovery(session)._sample_table("DB", "SCH", "T", cols)
        assert result == {}
        session.table.assert_not_called()

    def test_returns_at_most_five_distinct_values(self):
        from weaver.discovery import SchemaDiscovery

        cols = [{"name": "SECTOR", "raw_type": "TEXT"}]
        rows = [{"SECTOR": str(i)} for i in range(10)]
        session = self._make_session(rows)

        result = SchemaDiscovery(session)._sample_table("DB", "SCH", "T", cols)
        assert len(result["SECTOR"]) == 5

    def test_deduplicates_values(self):
        from weaver.discovery import SchemaDiscovery

        cols = [{"name": "MKT_ID", "raw_type": "TEXT"}]
        rows = [{"MKT_ID": "STK"}, {"MKT_ID": "STK"}, {"MKT_ID": "STK"}]
        session = self._make_session(rows)

        result = SchemaDiscovery(session)._sample_table("DB", "SCH", "T", cols)
        assert result["MKT_ID"] == ["STK"]

    def test_none_values_are_skipped(self):
        from weaver.discovery import SchemaDiscovery

        cols = [{"name": "SECTOR", "raw_type": "TEXT"}]
        rows = [{"SECTOR": None}, {"SECTOR": "IT"}, {"SECTOR": None}]
        session = self._make_session(rows)

        result = SchemaDiscovery(session)._sample_table("DB", "SCH", "T", cols)
        assert result["SECTOR"] == ["IT"]

    def test_returns_empty_dict_on_snowpark_exception(self):
        from weaver.discovery import SchemaDiscovery

        cols = [{"name": "MKT_ID", "raw_type": "TEXT"}]
        session = MagicMock()
        session.table.side_effect = RuntimeError("connection lost")

        result = SchemaDiscovery(session)._sample_table("DB", "SCH", "T", cols)
        assert result == {}



class TestSchemaDiscoveryRun:
    def test_returns_schema_profile_with_correct_keys(self):
        from weaver.discovery import SchemaDiscovery

        session = _mock_session(
            table_rows=[_table_row("T1")],
            column_rows=[_column_row("T1", "ID", "FIXED")],
            sample_rows=[],
        )
        profile = SchemaDiscovery(session).run("MYDB", "MYSCH")

        assert profile["database"] == "MYDB"
        assert profile["schema"] == "MYSCH"
        assert "tables" in profile

    def test_table_count_matches_fetched_metadata(self):
        from weaver.discovery import SchemaDiscovery

        session = _mock_session(
            table_rows=[_table_row("T1"), _table_row("T2")],
            column_rows=[
                _column_row("T1", "ID", "FIXED"),
                _column_row("T2", "ID", "FIXED"),
            ],
            sample_rows=[],
        )
        profile = SchemaDiscovery(session).run("DB", "SCH")
        assert len(profile["tables"]) == 2

    def test_column_types_are_normalised(self):
        """Raw Snowflake type names must be converted to DSL DataType strings."""
        from weaver.discovery import SchemaDiscovery

        session = _mock_session(
            table_rows=[_table_row("T1")],
            column_rows=[
                _column_row("T1", "PRICE", "FIXED"),
                _column_row("T1", "NAME", "TEXT"),
            ],
            sample_rows=[{"NAME": "foo"}],
        )
        profile = SchemaDiscovery(session).run("DB", "SCH")
        types = {col["name"]: col["type"] for col in profile["tables"][0]["columns"]}
        assert types["PRICE"] == "NUMBER"
        assert types["NAME"] == "VARCHAR"

    def test_fk_candidates_populated_for_shared_columns(self):
        """Columns shared across tables must appear in fk_candidates after run()."""
        from weaver.discovery import SchemaDiscovery

        session = _mock_session(
            table_rows=[_table_row("TRADES"), _table_row("STOCKS")],
            column_rows=[
                _column_row("TRADES", "ISU_CD", "TEXT"),
                _column_row("STOCKS", "ISU_CD", "TEXT"),
            ],
            sample_rows=[{"ISU_CD": "005930"}],
        )
        profile = SchemaDiscovery(session).run("DB", "SCH")

        all_candidates = {
            tbl["name"]: tbl["fk_candidates"] for tbl in profile["tables"]
        }
        assert len(all_candidates["TRADES"]) > 0
        assert all_candidates["TRADES"][0]["column"] == "ISU_CD"

    def test_sample_called_for_each_table(self):
        """session.table() must be called once per table (for column sampling)."""
        from weaver.discovery import SchemaDiscovery

        session = _mock_session(
            table_rows=[_table_row("T1"), _table_row("T2")],
            column_rows=[
                _column_row("T1", "NAME", "TEXT"),
                _column_row("T2", "NAME", "TEXT"),
            ],
            sample_rows=[{"NAME": "x"}],
        )
        SchemaDiscovery(session).run("DB", "SCH")
        assert session.table.call_count == 2
