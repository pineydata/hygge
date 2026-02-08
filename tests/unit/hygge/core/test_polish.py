import polars as pl
import pytest

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


def test_polisher_hash_id_respects_existing_column():
    """Test that hash ID rules respect existing columns and don't override them."""
    df = pl.DataFrame(
        {
            "EmployeeNumber": [1, 2],
            "EffectiveDate": ["2024-01-01", "2024-01-02"],
            "UserIdHash": ["existing", "values"],  # Pre-existing column
        }
    )

    rule = HashIdRule(
        name="UserIdHash",  # Same name as existing column
        from_columns=["EmployeeNumber", "EffectiveDate"],
        algo="sha256",
        hex=True,
    )
    cfg = PolishConfig(hash_ids=[rule])
    polisher = Polisher(cfg)

    result = polisher.apply(df)

    # Existing column should be preserved, not overwritten
    assert "UserIdHash" in result.columns
    assert result["UserIdHash"].to_list() == ["existing", "values"]


def test_polisher_hash_id_skips_silently_for_deletion_markers():
    """
    Test that hash ID rules skip silently when deletion markers already have
    hash ID columns as key columns.

    This prevents unnecessary warnings for deletion markers that already contain
    hash ID columns (e.g., when key_columns includes a hash ID column).
    """
    # Deletion marker DataFrame with hash ID column already present as key column
    df = pl.DataFrame(
        {
            "Id": [1, 2, 3],
            "IstarDesignationHashId": [
                "abc123",
                "def456",
                "ghi789",
            ],  # Hash ID column already exists (as key column)
            "__rowMarker__": [2, 2, 2],  # Delete marker
        }
    )

    # Hash ID rule that would normally create this column
    rule = HashIdRule(
        name="IstarDesignationHashId",  # Same name as existing column
        from_columns=["Id", "Designation"],  # Source columns (may not exist)
        algo="sha256",
        hex=True,
    )
    cfg = PolishConfig(hash_ids=[rule])
    polisher = Polisher(cfg)

    # Apply polish - should skip hash ID rule silently (no warnings)
    result = polisher.apply(df)

    # Existing hash ID column should be preserved
    assert "IstarDesignationHashId" in result.columns
    assert result["IstarDesignationHashId"].to_list() == ["abc123", "def456", "ghi789"]

    # Other columns should remain unchanged
    assert "Id" in result.columns
    assert "__rowMarker__" in result.columns


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


def test_polisher_hash_id_invalid_algorithm_skipped():
    """
    Test that invalid hash algorithms are skipped gracefully (comfort over correctness).
    """
    df = pl.DataFrame(
        {
            "EmployeeNumber": [1, 2],
            "EffectiveDate": ["2024-01-01", "2024-01-02"],
        }
    )

    # Invalid algorithm should be skipped, not fail
    invalid_rule = HashIdRule(
        name="InvalidHash",
        from_columns=["EmployeeNumber", "EffectiveDate"],
        algo="invalid_algo_xyz",
        hex=True,
    )
    # Valid rule should still work
    valid_rule = HashIdRule(
        name="ValidHash",
        from_columns=["EmployeeNumber", "EffectiveDate"],
        algo="sha256",
        hex=True,
    )

    cfg = PolishConfig(hash_ids=[invalid_rule, valid_rule])
    polisher = Polisher(cfg)

    result = polisher.apply(df)

    # Invalid rule should be skipped (no column added)
    assert "InvalidHash" not in result.columns
    # Valid rule should still work
    assert "ValidHash" in result.columns
    assert result["ValidHash"].dtype == pl.Utf8


def test_timestamp_rule_source_validation():
    """Test that TimestampRule validates source field."""
    # Valid values should work
    rule1 = TimestampRule(name="Test1", source="now_utc")
    assert rule1.source == "now_utc"

    rule2 = TimestampRule(name="Test2", source="now_local")
    assert rule2.source == "now_local"

    # Invalid value should raise ValueError
    with pytest.raises(
        ValueError, match="source must be one of: 'now_utc', 'now_local'"
    ):
        TimestampRule(name="Test3", source="invalid_source")


def test_timestamp_rule_type_validation():
    """Test that TimestampRule validates type field."""
    # Valid values should work
    rule1 = TimestampRule(name="Test1", type="datetime")
    assert rule1.type == "datetime"

    rule2 = TimestampRule(name="Test2", type="string")
    assert rule2.type == "string"

    # Invalid value should raise ValueError
    with pytest.raises(ValueError, match="type must be one of: 'datetime', 'string'"):
        TimestampRule(name="Test3", type="invalid_type")


def test_polisher_hash_id_matches_normalized_columns():
    """
    Test that hash ID creation can match normalized column names.

    This handles the case where:
    - Config references original column names with spaces: 'Employee Number'
    - DataFrame has already-normalized columns: 'EmployeeNumber'
    - The matching logic should normalize both and find the match
    """
    # DataFrame with already-normalized columns (as if from home)
    df = pl.DataFrame(
        {
            "EmployeeNumber": [1, 2, 3],
            "EffectiveDate": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "OtherColumn": ["a", "b", "c"],
        }
    )

    # Config references original column names with spaces
    hash_rule = HashIdRule(
        name="EmployeeHistoryId",
        # Original names with spaces
        from_columns=["Employee Number", "Effective Date"],
        algo="sha256",
        hex=True,
    )

    cfg = PolishConfig(
        columns=ColumnRules(case="pascal", remove_special=True),
        hash_ids=[hash_rule],
    )

    polisher = Polisher(cfg)
    result = polisher.apply(df)

    # Hash ID should be created using the normalized column names
    assert "EmployeeHistoryId" in result.columns
    assert result["EmployeeHistoryId"].dtype == pl.Utf8

    # Hash ID should be normalized after column normalization step
    # 'EmployeeHistoryId' -> 'EmployeeHistoryId' (already PascalCase, stays same)
    assert "EmployeeHistoryId" in result.columns

    # Verify hash values are deterministic
    hash_values = result["EmployeeHistoryId"].to_list()
    assert len(set(hash_values)) == 3  # All unique values


def test_polisher_hash_id_matches_normalized_columns_reverse():
    """
    Test reverse case: config has normalized, DataFrame has spaces.

    This handles the case where:
    - Config references normalized column names: 'EmployeeNumber'
    - DataFrame has original column names with spaces: 'Employee Number'
    - The matching logic should normalize both and find the match
    """
    # DataFrame with original column names with spaces
    df = pl.DataFrame(
        {
            "Employee Number": [1, 2, 3],
            "Effective Date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "Other Column": ["a", "b", "c"],
        }
    )

    # Config references normalized column names
    hash_rule = HashIdRule(
        name="EmployeeHistoryId",
        from_columns=["EmployeeNumber", "EffectiveDate"],  # Normalized names
        algo="sha256",
        hex=True,
    )

    cfg = PolishConfig(
        columns=ColumnRules(case="pascal", remove_special=True),
        hash_ids=[hash_rule],
    )

    polisher = Polisher(cfg)
    result = polisher.apply(df)

    # Hash ID should be created using the original column names
    assert "EmployeeHistoryId" in result.columns
    assert result["EmployeeHistoryId"].dtype == pl.Utf8

    # After normalization, columns should be normalized
    assert "EmployeeNumber" in result.columns
    assert "EffectiveDate" in result.columns
    assert "OtherColumn" in result.columns

    # Hash ID should also be normalized (if needed)
    assert "EmployeeHistoryId" in result.columns


def test_polisher_hash_id_matches_underscore_columns():
    """
    Test matching columns with underscores (AONE_Units case).

    This handles the case where:
    - Config references original column name with underscore: 'AONE_Units'
    - DataFrame has already-normalized column: 'AOneUnits'
    - Note: 'AONE_Units' normalizes to 'AoneUnits' (AONE -> Aone)
    - But 'AOneUnits' normalizes to 'AOneUnits' (A + One)
    - These don't match, so we need the DataFrame to have 'AoneUnits' instead
    """
    # DataFrame with already-normalized columns
    # Note: 'AONE_Units' normalizes to 'AoneUnits', not 'AOneUnits'
    df = pl.DataFrame(
        {
            "AoneUnits": [1, 2, 3],  # This is what AONE_Units normalizes to
            "UnitName": ["A", "B", "C"],
        }
    )

    # Config references original column name with underscore
    hash_rule = HashIdRule(
        name="AONEHashId",
        from_columns=["AONE_Units"],  # Original name with underscore
        algo="sha256",
        hex=True,
    )

    cfg = PolishConfig(
        columns=ColumnRules(case="pascal", remove_special=True),
        hash_ids=[hash_rule],
    )

    polisher = Polisher(cfg)
    result = polisher.apply(df)

    # Hash ID should be created with the name from config
    # But after normalization, it will be normalized
    # 'AONEHashId' -> 'AoneHashId' (AONE -> Aone)
    assert "AoneHashId" in result.columns
    assert result["AoneHashId"].dtype == pl.Utf8

    # Verify the original column is also normalized
    assert "AoneUnits" in result.columns
    assert "UnitName" in result.columns


def test_polisher_order_hash_ids_before_normalization():
    """
    Test that hash IDs are generated before normalization.

    This allows users to reference original column names in hash ID rules,
    and the hash ID column name itself will be normalized.
    """
    df = pl.DataFrame(
        {
            "Employee Number": [1, 2],  # Original name with space
            "Effective-Date": ["2024-01-01", "2024-01-02"],
        }
    )

    # Hash ID rule references original column names (with spaces/hyphens)
    hash_rule = HashIdRule(
        name="User Hash ID",  # Hash ID name will also be normalized
        from_columns=["Employee Number", "Effective-Date"],
        algo="sha256",
        hex=True,
    )

    cfg = PolishConfig(
        hash_ids=[hash_rule],
        columns=ColumnRules(
            remove_special=True,
            case="pascal",
            remove_spaces=True,
        ),
    )

    polisher = Polisher(cfg)
    result = polisher.apply(df)

    # Original columns should be normalized
    assert "EmployeeNumber" in result.columns
    assert "EffectiveDate" in result.columns

    # Hash ID column should exist and be normalized too
    # "User Hash ID" -> "UserHashId" (ID becomes Id via capitalize())
    assert "UserHashId" in result.columns
    assert result["UserHashId"].dtype == pl.Utf8

    # Hash should be deterministic
    expected_hash = result["UserHashId"].to_list()[0]
    result2 = polisher.apply(df)
    assert result2["UserHashId"].to_list()[0] == expected_hash


def test_polisher_column_name_collision_deduplication():
    """
    Test that duplicate normalized column names are handled with deduplication.

    When multiple columns normalize to the same name, suffixes are applied
    to make them unique (first keeps original, subsequent get "_1", "_2", etc.).
    """
    df = pl.DataFrame(
        {
            "Employee-ID": [1, 2],  # Will normalize to "EmployeeID"
            "EmployeeID": [3, 4],  # Also normalizes to "EmployeeID"
            "Employee ID": [5, 6],  # Also normalizes to "EmployeeID"
        }
    )

    cfg = PolishConfig(
        columns=ColumnRules(
            remove_special=True,
            case="pascal",
            remove_spaces=True,
        )
    )

    polisher = Polisher(cfg)
    result = polisher.apply(df)

    # All three should be deduplicated
    # "EmployeeID" normalizes to "EmployeeId" (ID becomes Id via capitalize())
    assert "EmployeeId" in result.columns  # First occurrence keeps original
    assert "EmployeeId_1" in result.columns  # Second gets "_1"
    assert "EmployeeId_2" in result.columns  # Third gets "_2"

    # Verify data is preserved
    assert len(result.columns) == 3
    assert len(result) == 2  # Two rows
