import polars as pl

from hygge.core.polish import (
    ColumnRules,
    ConstantRule,
    HashIdRule,
    PolishConfig,
    Polisher,
    TimestampRule,
)


def test_polisher_column_normalization_pascal_and_remove_special():
    df = pl.DataFrame(
        {
            "Employee Number": [1, 2],
            "Effective-Date (UTC)": ["2024-01-01", "2024-01-02"],
        }
    )

    cfg = PolishConfig(
        columns=ColumnRules(
            remove_special=True,
            case="pascal",
            remove_spaces=False,
        )
    )

    polisher = Polisher(cfg)
    result = polisher.apply(df)

    assert "EmployeeNumber" in result.columns
    assert "EffectiveDateUtc" in result.columns


def test_polisher_column_normalization_camel_case():
    df = pl.DataFrame(
        {
            "Employee Number": [1, 2],
            "Effective-Date": ["2024-01-01", "2024-01-02"],
        }
    )

    cfg = PolishConfig(
        columns=ColumnRules(
            remove_special=True,
            case="camel",
            remove_spaces=False,
        )
    )

    polisher = Polisher(cfg)
    result = polisher.apply(df)

    assert "employeeNumber" in result.columns
    assert "effectiveDate" in result.columns


def test_polisher_column_normalization_snake_case():
    df = pl.DataFrame(
        {
            "Employee Number": [1, 2],
            "Effective-Date": ["2024-01-01", "2024-01-02"],
        }
    )

    cfg = PolishConfig(
        columns=ColumnRules(
            remove_special=True,
            case="snake",
            remove_spaces=False,
        )
    )

    polisher = Polisher(cfg)
    result = polisher.apply(df)

    assert "employee_number" in result.columns
    assert "effective_date" in result.columns


def test_polisher_pascal_case_boundaries():
    """Test that PascalCase boundaries are handled correctly."""
    df = pl.DataFrame(
        {
            "XMLParser": [1, 2],
            "HTTPSConnection": ["a", "b"],
            "EmployeeID": [10, 20],
            "employeeNumber": [100, 200],
            "XMLHTTPRequest": ["x", "y"],  # Multiple acronyms
        }
    )

    cfg = PolishConfig(
        columns=ColumnRules(
            case="snake",  # Use snake_case to verify word splitting
        )
    )

    polisher = Polisher(cfg)
    result = polisher.apply(df)

    # PascalCase boundaries should split correctly
    assert "xml_parser" in result.columns  # XMLParser -> XML + Parser
    assert "https_connection" in result.columns  # HTTPSConnection -> HTTPS + Connection
    assert "employee_id" in result.columns  # EmployeeID -> Employee + ID
    assert "employee_number" in result.columns  # employeeNumber -> employee + Number
    # Edge case: consecutive all-caps (XMLHTTPRequest) becomes xmlhttp_request
    # This is acceptable - the common cases (XMLParser, HTTPSConnection) work correctly
    assert "xmlhttp_request" in result.columns


def test_polisher_hash_id_deterministic():
    df = pl.DataFrame(
        {
            "EmployeeNumber": [1, 2],
            "EffectiveDate": ["2024-01-01", "2024-01-02"],
        }
    )

    rule = HashIdRule(
        name="UserIdHash",
        from_columns=["EmployeeNumber", "EffectiveDate"],
        algo="sha256",
        hex=True,
    )
    cfg = PolishConfig(hash_ids=[rule])
    polisher = Polisher(cfg)

    result1 = polisher.apply(df)
    result2 = polisher.apply(df)

    assert "UserIdHash" in result1.columns
    assert result1["UserIdHash"].dtype == pl.Utf8
    # Deterministic across applications
    assert result1["UserIdHash"].to_list() == result2["UserIdHash"].to_list()


def test_polisher_constants_and_timestamps():
    df = pl.DataFrame({"value": [1, 2]})

    constants = [ConstantRule(name="__rowMarker__", value=4)]
    timestamps = [TimestampRule(name="__LastLoadedAt__")]

    cfg = PolishConfig(constants=constants, timestamps=timestamps)
    polisher = Polisher(cfg)

    result = polisher.apply(df)

    assert "__rowMarker__" in result.columns
    assert "__LastLoadedAt__" in result.columns
    # Constant column is indeed constant
    assert set(result["__rowMarker__"].to_list()) == {4}
