"""
Unit tests for weaver.dsl — no Snowflake connection required.

Covers:
  - Parsing a real-world Cortex Analyst YAML (nti_model.yaml)
  - Round-trip serialisation fidelity
  - YAML output structure (correct field names, no spurious fields)
  - All Pydantic validators
  - Edge cases (empty lists stripped, schema alias, verified_queries)
"""

from pathlib import Path

import pytest
import yaml

from weaver.dsl import (
    BaseTable,
    DataType,
    Dimension,
    Filter,
    FilterType,
    JoinType,
    Measure,
    Relationship,
    RelationshipColumn,
    RelationshipType,
    SemanticModel,
    SemanticTable,
    TimeDimension,
    VerifiedQuery,
)

NTI_YAML = Path(__file__).parent.parent / "examples/streamlit-on-snowflake/manifest/nti_model.yaml"


@pytest.fixture
def base_table():
    return BaseTable(**{"database": "DB", "schema": "SCH", "table": "FACT"})


@pytest.fixture
def minimal_model(base_table):
    return SemanticModel(
        name="test_model",
        tables=[
            SemanticTable(
                name="TRADES",
                base_table=base_table,
                time_dimensions=[
                    TimeDimension(name="TRADE_DATE", expr="TRADE_DT", data_type=DataType.DATE)
                ],
                dimensions=[
                    Dimension(
                        name="STOCK_CODE",
                        expr="ISU_CD",
                        data_type=DataType.VARCHAR,
                        synonyms=["ticker", "symbol"],
                        sample_values=["005930", "000660"],
                    )
                ],
                measures=[Measure(name="VOLUME", expr="SUM(QTY)", data_type=DataType.NUMBER)],
            )
        ],
    )


@pytest.fixture
def two_table_model(base_table):
    stocks_table = SemanticTable(
        name="STOCKS",
        base_table=BaseTable(**{"database": "DB", "schema": "SCH", "table": "STOCKS"}),
        dimensions=[Dimension(name="STOCK_CODE", expr="ISU_CD", data_type=DataType.VARCHAR)],
    )
    trades_table = SemanticTable(
        name="TRADES",
        base_table=base_table,
        measures=[Measure(name="VOLUME", expr="SUM(QTY)", data_type=DataType.NUMBER)],
    )
    return SemanticModel(
        name="joined_model",
        tables=[trades_table, stocks_table],
        relationships=[
            Relationship(
                name="trades_to_stocks",
                left_table="TRADES",
                right_table="STOCKS",
                relationship_type=RelationshipType.MANY_TO_ONE,
                join_type=JoinType.LEFT_OUTER,
                relationship_columns=[
                    RelationshipColumn(left_column="STOCK_CODE", right_column="STOCK_CODE")
                ],
            )
        ],
    )


def test_parse_nti_model():
    """nti_model.yaml is a real Cortex Analyst YAML — it must parse without errors."""
    model = SemanticModel.from_yaml_file(str(NTI_YAML))
    assert model.name == "next_trading_intelligence"
    assert len(model.tables) > 0


def test_nti_model_tables_have_base_table():
    model = SemanticModel.from_yaml_file(str(NTI_YAML))
    for table in model.tables:
        assert table.base_table.database
        assert table.base_table.schema_
        assert table.base_table.table


def test_nti_model_has_measures_and_dimensions():
    model = SemanticModel.from_yaml_file(str(NTI_YAML))
    tables_with_measures = [t for t in model.tables if t.measures]
    tables_with_dims = [t for t in model.tables if t.dimensions or t.time_dimensions]
    assert tables_with_measures, "Expected at least one table with measures"
    assert tables_with_dims, "Expected at least one table with dimensions"


def test_nti_model_roundtrip():
    """Parsed → serialised → re-parsed model must be equivalent."""
    original = SemanticModel.from_yaml_file(str(NTI_YAML))
    reparsed = SemanticModel.from_yaml(original.to_yaml())
    assert reparsed.name == original.name
    assert len(reparsed.tables) == len(original.tables)
    for orig_t, new_t in zip(original.tables, reparsed.tables):
        assert orig_t.name == new_t.name
        assert len(orig_t.measures) == len(new_t.measures)
        assert len(orig_t.dimensions) == len(new_t.dimensions)
        assert len(orig_t.time_dimensions) == len(new_t.time_dimensions)


def test_schema_alias_in_yaml(minimal_model):
    """The Python field is schema_, but YAML must use 'schema'."""
    raw = yaml.safe_load(minimal_model.to_yaml())
    base = raw["tables"][0]["base_table"]
    assert "schema" in base, "Expected 'schema' key in base_table YAML"
    assert "schema_" not in base, "schema_ must not appear in serialised YAML"


def test_empty_lists_not_in_yaml(minimal_model):
    """Fields with empty lists (e.g. relationships, filters) must be omitted."""
    raw = yaml.safe_load(minimal_model.to_yaml())
    assert "relationships" not in raw
    assert "verified_queries" not in raw
    table = raw["tables"][0]
    assert "filters" not in table


def test_empty_strings_not_in_yaml():
    """Empty description strings must be omitted from YAML output."""
    model = SemanticModel(
        name="bare",
        tables=[
            SemanticTable(
                name="T",
                base_table=BaseTable(**{"database": "D", "schema": "S", "table": "T"}),
                measures=[Measure(name="M", expr="SUM(x)", data_type=DataType.NUMBER)],
            )
        ],
    )
    raw = yaml.safe_load(model.to_yaml())
    assert "description" not in raw
    assert "description" not in raw["tables"][0]


def test_measure_has_no_default_aggregation(minimal_model):
    """
    Cortex Analyst embeds aggregation in expr — there is no default_aggregation field.
    Ensure we never accidentally emit it.
    """
    raw = yaml.safe_load(minimal_model.to_yaml())
    for table in raw["tables"]:
        for measure in table.get("measures", []):
            assert "default_aggregation" not in measure


def test_synonyms_and_sample_values_preserved(minimal_model):
    raw = yaml.safe_load(minimal_model.to_yaml())
    dim = raw["tables"][0]["dimensions"][0]
    assert dim["synonyms"] == ["ticker", "symbol"]
    assert dim["sample_values"] == ["005930", "000660"]


def test_relationship_serialisation(two_table_model):
    raw = yaml.safe_load(two_table_model.to_yaml())
    rels = raw["relationships"]
    assert len(rels) == 1
    rel = rels[0]
    assert rel["left_table"] == "TRADES"
    assert rel["right_table"] == "STOCKS"
    assert rel["relationship_type"] == "many_to_one"
    assert rel["join_type"] == "left_outer"
    assert rel["relationship_columns"][0] == {
        "left_column": "STOCK_CODE",
        "right_column": "STOCK_CODE",
    }


def test_verified_query_roundtrip():
    model = SemanticModel(
        name="vq_model",
        tables=[
            SemanticTable(
                name="T",
                base_table=BaseTable(**{"database": "D", "schema": "S", "table": "T"}),
                measures=[Measure(name="M", expr="SUM(x)", data_type=DataType.NUMBER)],
            )
        ],
        verified_queries=[
            VerifiedQuery(
                name="total_volume",
                question="What is the total volume?",
                sql="SELECT SUM(x) FROM T",
                verified_by="weaver",
            )
        ],
    )
    reparsed = SemanticModel.from_yaml(model.to_yaml())
    assert len(reparsed.verified_queries) == 1
    vq = reparsed.verified_queries[0]
    assert vq.question == "What is the total volume?"
    assert vq.sql == "SELECT SUM(x) FROM T"


def test_filter_serialisation():
    model = SemanticModel(
        name="filter_model",
        tables=[
            SemanticTable(
                name="T",
                base_table=BaseTable(**{"database": "D", "schema": "S", "table": "T"}),
                dimensions=[Dimension(name="MARKET", expr="MKT_ID", data_type=DataType.VARCHAR)],
                filters=[
                    Filter(
                        name="KOSPI_ONLY",
                        synonyms=["코스피", "KOSPI"],
                        filter_type=FilterType.CATEGORICAL,
                        expr="MKT_ID = 'STK'",
                    )
                ],
            )
        ],
    )
    raw = yaml.safe_load(model.to_yaml())
    f = raw["tables"][0]["filters"][0]
    assert f["name"] == "KOSPI_ONLY"
    assert f["expr"] == "MKT_ID = 'STK'"
    assert f["filter_type"] == "categorical"


def test_to_dict_is_json_safe(two_table_model):
    """to_dict() must produce plain Python types (no Enum instances)."""
    import json

    d = two_table_model.to_dict()
    serialised = json.dumps(d)
    assert "many_to_one" in serialised
    assert "left_outer" in serialised


def test_time_dimension_rejects_varchar():
    with pytest.raises(ValueError, match="date/timestamp"):
        TimeDimension(name="X", expr="col", data_type=DataType.VARCHAR)


def test_time_dimension_rejects_number():
    with pytest.raises(ValueError, match="date/timestamp"):
        TimeDimension(name="X", expr="col", data_type=DataType.NUMBER)


@pytest.mark.parametrize(
    "dtype",
    [DataType.DATE, DataType.TIMESTAMP_NTZ, DataType.TIMESTAMP_LTZ, DataType.TIMESTAMP_TZ],
)
def test_time_dimension_accepts_all_temporal_types(dtype):
    td = TimeDimension(name="DT", expr="col", data_type=dtype)
    assert td.data_type == dtype


def test_relationship_rejects_missing_left_table():
    with pytest.raises(ValueError, match="left_table 'MISSING'"):
        SemanticModel(
            name="bad",
            tables=[
                SemanticTable(
                    name="STOCKS",
                    base_table=BaseTable(**{"database": "D", "schema": "S", "table": "T"}),
                )
            ],
            relationships=[
                Relationship(
                    name="r",
                    left_table="MISSING",
                    right_table="STOCKS",
                    relationship_columns=[RelationshipColumn(left_column="a", right_column="b")],
                )
            ],
        )


def test_relationship_rejects_missing_right_table():
    with pytest.raises(ValueError, match="right_table 'GHOST'"):
        SemanticModel(
            name="bad",
            tables=[
                SemanticTable(
                    name="TRADES",
                    base_table=BaseTable(**{"database": "D", "schema": "S", "table": "T"}),
                )
            ],
            relationships=[
                Relationship(
                    name="r",
                    left_table="TRADES",
                    right_table="GHOST",
                    relationship_columns=[RelationshipColumn(left_column="a", right_column="b")],
                )
            ],
        )


def test_valid_relationship_passes(two_table_model):
    assert len(two_table_model.relationships) == 1
