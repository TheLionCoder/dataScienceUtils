# -*- coding: utf-8 -*-

"""
utils
-----
This module contains utility functions.
"""
import hashlib
import logging
import zipfile
from pathlib import Path
from typing import Any, Dict, Union, IO, Callable, Generator

import colorlog
import pandas as pd
from sqlalchemy.dialects.oracle import NUMBER, FLOAT
from sqlalchemy.types import String, DATE


# Define empty data class error
class EmptyDataFrameError(Exception):
    pass


# Set up logger
def setup_logger() -> logging.Logger:
    """
    Set up logger configuration.
    :return: Logger instance.
    """
    formatter = colorlog.ColoredFormatter(
        "%(log_color)s %(levelname)-8s %(reset)s %(message)s",
        datefmt=None,
        reset=True,
        log_colors={
            "DEBUG": "cyan",
            "INFO": "green",
            "WARNING": "yellow",
            "ERROR": "red",
            "CRITICAL": "red,bg_white",
        },
    )
    logger = logging.getLogger(__name__)
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    return logger


def _load_dataframe_from_file(
    read_func: Callable,
    filepath: Union[IO, Path],
    **kwargs,
) -> pd.DataFrame:
    """
    Extracts a Pandas DataFrame from a given input file using a
    specified read dataset function.

    Args:
        read_func: A callable object that reads the dataset
        from the input file.
        filepath: The input file from which the dataset is extracted.
        Can be a file object or a path.
        **kwargs: Additional keyword arguments to be passed to the read_dataset_func.

    Returns:
        A Pandas DataFrame representing the dataset extracted from the input file.

    Raises:
        ValueError: If an error occurs during the dataset extraction process.

    """
    try:
        df: pd.DataFrame = read_func(filepath, **kwargs)
        return df
    except ValueError as value_error:
        raise value_error
    except AttributeError as attribute_error:
        raise attribute_error


def extract_dataframe_from_zip(
    source_zip_path: Path,
    member_name: str,
    read_func: Callable,
    **kwargs: object,
) -> pd.DataFrame:
    """
    Unzip a file from a zip archive.

    Args:
        -----
        read_func: Function to be used to read the dataset. e.g. pd.read_csv.
        source_zip_path: Local file path where the zip archive is located.
        member_name: Name of the file to be extracted from the
                zip archive and converted to a Pandas DataFrame.
        Extension of the file to be extracted from the zip archive.
                e.g., csv, xlsx.
        **kwargs: Additional arguments to be passed to the
        pd.read_csv or pd.read_excel functions.
        for more information see:
        pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html
    Returns:
        -------
        Pandas DataFrame.

    Raises:
        ------
        FileNotFoundError: If the file to be extracted is not
        found in the zip archive.
    """
    with zipfile.ZipFile(source_zip_path) as zip_folder:
        try:
            with zip_folder.open(member_name) as file:
                return _load_dataframe_from_file(read_func, file, **kwargs)
        except KeyError as key_error:
            raise FileNotFoundError(
                f"File {member_name} not found in zip archive {source_zip_path}."
            ) from key_error
        except zipfile.BadZipFile as bad_zip_file_error:
            raise bad_zip_file_error


def infer_sql_types(df: pd.DataFrame) -> Dict[Any, type]:
    """
    Get the SQL data type for each column in a Pandas DataFrame.

    Args:
        df: Pandas DataFrame.

    Returns:
        Dict[str, type]

    """
    sql_type_mapping: Dict[str, type] = {
        "object": String,
        "category": String,
        "string[pyarrow]": String,
        "double[pyarrow]": FLOAT,
        "datetime64[ns]": DATE,
        "timestamp[us][pyarrow]": DATE,
        "float64": FLOAT,
        "int64": NUMBER,
    }
    if df.empty:
        raise EmptyDataFrameError("DataFrame is empty.")

    return {
        col: sql_type_mapping.get(str(df[col].dtypes), String) for col in df.columns
    }


def get_top_n(ser: pd.Series, n: int, default: int | float | str = 0) -> pd.Series:
    """
    Args:
        ser: A pandas Series representing the data.
        n: An integer representing the number of top values to be returned.
        default: An optional parameter that specifies the default value to
        be used for elements that are not in the top n values.

    Returns:
        A pandas Series with the same length as the input Series where
        elements not in the top n values are replaced with the default value.

    Example:
        >>> "ser = pd.Series([1, 2, 3, 4, 5, 1, 2, 1, 2])"
        >>> "get_top_n(ser, 2, default=0)"
        0    1
        1    2
        2    0
        3    0
        4    0
        5    1
        6    2
        7    0
        8    0
        dtype: int64
    """
    top_n_values: pd.Index = ser.value_counts().nlargest(n).index
    return ser.where(ser.isin(top_n_values), default)


def standardize_columns(df_: pd.DataFrame) -> pd.DataFrame:
    """
    Rename columns in a given DataFrame.
    Args:
        df_: DataFrame to be processed.

    Returns:
        DataFrame with renamed columns.
    """
    renamed_cols: Dict[Any, str] = {
        col: _format_column_name(col) for col in df_.columns
    }
    return df_.rename(columns=renamed_cols)


def _format_column_name(col: Any) -> str:
    """Rename a column by removing spaces from its name.
    :param col: Column name.
    :return: Column name without spaces."""
    if isinstance(col, str):
        return col.strip().replace(" ", "_").lower()
    else:
        return str(col)


# add hash to files
def read_file_chunks(file_path: Path) -> Generator[bytes, None, None]:
    """
    Read a file and yield its content in chunks
    of 4086 bytes.
    Args:
        file_path: file path-

    Returns:
           Bytes
    """
    with open(file_path, "rb") as file:
        while chunk := file.read(4096):
            yield chunk


def compute_hash(file_content: Generator[bytes, None, None]) -> str:
    """
    Compute SHA-256 hash.
    Args:
        file_content: Bytes of file content.

    Returns:
        The computed SHA-256 hash.
    """
    hasher = hashlib.sha256()
    for chunk in file_content:
        hasher.update(chunk)
    return hasher.hexdigest()


def create_hash_file(directory_path: str, file_pattern: str) -> None:
    """
    Create a hash file that contains a list of files and hashes.
    Args:
        directory_path: Path to the directory where the files are located.
        file_pattern: Pattern of the files to be hashed.
    """
    src_path: Path = Path(directory_path)
    hash_file_name: str = "hashes.txt"
    hash_file_path = src_path / hash_file_name

    # Create a hash file that contains a list of files and hashes
    with open(hash_file_path, "w") as hash_output:
        for file in src_path.glob(file_pattern):
            if file.name == hash_file_name:
                continue
            file_content_gen = read_file_chunks(file)
            computed_hash = compute_hash(file_content_gen)
            hash_output.write(f"{file.name} {computed_hash}\n")
