from typing import Iterable, Sequence

from pyspark.sql import DataFrame
from pyspark.sql.functions import col


class DQError(ValueError):
    """Raised when a data quality check fails."""
    pass


def assert_no_nulls(df: DataFrame, columns: Sequence[str], label: str) -> None:
    """Verify specified columns contain no null values.
    
    Args:
        df: DataFrame to validate.
        columns: Column names to check for nulls.
        label: Context label for error messages (e.g., 'bronze_customers').
        
    Raises:
        DQError: If any column contains null values.
    """
    for col_name in columns:
        if df.filter(col(col_name).isNull()).count() > 0:
            raise DQError(f"{label}: column '{col_name}' has null rows")


def assert_unique(df: DataFrame, keys: Sequence[str], label: str) -> None:
    """Verify uniqueness constraint on composite keys.
    
    Args:
        df: DataFrame to validate.
        keys: Column names forming the unique key.
        label: Context label for error messages.
        
    Raises:
        DQError: If duplicate key groups exist.
    """
    if df.groupBy(*keys).count().filter(col("count") > 1).count() > 0:
        raise DQError(f"{label}: found duplicate key groups on {list(keys)}")


def assert_positive(df: DataFrame, columns: Sequence[str], label: str) -> None:
    """Verify all values in columns are strictly positive (> 0).
    
    Args:
        df: DataFrame to validate.
        columns: Column names to check.
        label: Context label for error messages.
        
    Raises:
        DQError: If any column contains zero or negative values.
    """
    for col_name in columns:
        if df.filter(col(col_name) <= 0).count() > 0:
            raise DQError(f"{label}: column '{col_name}' has non-positive rows")


def assert_non_negative(df: DataFrame, columns: Sequence[str], label: str) -> None:
    """Verify all values in columns are non-negative (>= 0).
    
    Args:
        df: DataFrame to validate.
        columns: Column names to check.
        label: Context label for error messages.
        
    Raises:
        DQError: If any column contains negative values.
    """
    for col_name in columns:
        if df.filter(col(col_name) < 0).count() > 0:
            raise DQError(f"{label}: column '{col_name}' has negative rows")


def assert_allowed_values(df: DataFrame, column_name: str, allowed: Iterable[str], label: str) -> None:
    """Verify column values belong to a set of allowed values (enum check).
    
    Args:
        df: DataFrame to validate.
        column_name: Column to validate.
        allowed: Iterable of valid values.
        label: Context label for error messages.
        
    Raises:
        DQError: If column contains values outside the allowed set.
    """
    if df.filter(~col(column_name).isin(list(allowed))).count() > 0:
        raise DQError(f"{label}: column '{column_name}' has rows outside allowed values")


def assert_approx_equal(actual: float, expected: float, label: str, tolerance: float = 1e-6) -> None:
    """Verify two numeric values are approximately equal within tolerance.
    
    Used for reconciliation checks (e.g., sum of parts equals total).
    
    Args:
        actual: Actual value.
        expected: Expected value.
        label: Context label for error messages.
        tolerance: Allowed difference (default 1e-6).
        
    Raises:
        DQError: If absolute difference exceeds tolerance.
    """
    if abs(float(actual) - float(expected)) > tolerance:
        raise DQError(f"{label}: expected {expected}, got {actual}")