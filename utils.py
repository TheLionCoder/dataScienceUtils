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
from typing import Any, Dict, List, Union, IO, Callable, Generator, Literal, Optional

import colorlog
import pandas as pd
import polars as pl
from sqlalchemy.dialects.oracle import NUMBER, FLOAT
from sqlalchemy.types import String, DATE


# Define empty data class error
class EmptyDataFrameError(Exception):
    pass


def list_files(
    dir_path: Path,
    *,
    ignore_substring: Optional[str] = None,
    file_extension: Optional[str] = None,
) -> Generator[Path, None, None]:
    """
    List files in a directory
    :param dir_path: Path to the directory.
    :param file_extension: File extension of the files to list.
    :param ignore_substring: Substring to ignore in the file name.
    :return: List of files in the directory.
    """
    assert dir_path.is_dir(), f"{dir_path} is not a directory"
    pattern: str = f"*.{file_extension}" if file_extension else "*"
    files: Generator[Path, None, None] = dir_path.glob(pattern)
    return (
        file
        for file in files
        if not ignore_substring or ignore_substring not in file.name
    )


# Reader
def read_data(
    config: Any,
    file_path: Path,
    file_format: Literal["csv", "txt", "xlsx"],
    **kwargs,
) -> pd.DataFrame:
    """
    Read data from a file.
    :param config: Config object with read functions.
    :param file_path: File path to the dataset.
    :param file_format: File format of the dataset.
    :param kwargs: Additional arguments to pass to the read function

    .e.g.:
    >>> 'read_data(/some_path/some_file.csv, file_format="csv", sep="|")'
    >>> 'read_data(/some_path/some_file.txt, file_format="txt", delimiter="|")'
    >>> 'read_data(/some_path/some_file.xlsx, file_format="xlsx")'
    """
    try:
        read_function = config.read_functions[file_format]
        return read_function(file_path, **kwargs)
    except KeyError:
        raise ValueError(f"File format {file_format} not supported.")
    except FileNotFoundError:
        raise FileNotFoundError(f"File {file_path} not found.")
    except Exception as e:
        raise ValueError(f"Error reading file {file_path}: {e}")


def to_path(path: str | Path) -> Path:
    """
    Convert a string path to a Path object.
    :param path: String path to convert.
    :return: Path object.
    """
    return Path(path).expanduser() if isinstance(path, str) else path.expanduser()


# Set up logger
def setup_logger() -> logging.Logger:
    """
    Set up logger configuration.
    :return: Logger instance.
    """
    formatter = colorlog.ColoredFormatter(
        "%(asctime)s - %(log_color)s %(levelname)-8s %(reset)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
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
        It Can be a file object or a path.
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
        read_func: Function to be used to read the dataset.
        E.g., pd.read_csv.
        source_zip_path: Local file path where the zip archive is located.
        member_name: Name of the file to be extracted from the
                zip archive and converted to a Pandas DataFrame.
        Extension of the file to be extracted from the zip archive.
                E.g., csv, xlsx.
        **kwargs: Additional arguments to be passed to the
        pd.read_csv or pd.read_excel functions.
        For more information see:
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


def get_top_n(
    series: pd.Series, top_n: int, default: int | float | str = "other"
) -> pd.Series:
    """
    Args:
        series: A pandas Series representing the data.
        top_n: An integer representing the number of top values to be returned.
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
    top_n_values: pd.Series = series.value_counts()
    return series.where(series.isin(top_n_values.index[:top_n]), default)


def encode_categorical_features_by_frequency(
    dataframe: pl.DataFrame, *, input_col: List[str] | str, normalized: bool = True
) -> pl.DataFrame:
    """Encode Categorical Features by frequency.
    :param dataframe: polars.DataFrame
    :param input_col: List[str]
    :param normalized: bool
    :return: polars.DataFrame
    :raises: ValueError if input_col is not a list
    e.g.
      >>> "encode_categorical_features(dataframe=dataframe,input_col=['column_name', 'other_column_name'])"
    """
    encoded_column_name: str = f"{input_col}_ce"
    total_rows: int = dataframe.height if normalized else 1
    return dataframe.group_by(input_col).agg(
        pl.len().truediv(pl.lit(total_rows)).alias(encoded_column_name)
    )


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


def compute_hash(file_content: Generator[bytes, None, None], hash_name: str) -> str:
    """
    Compute Blake2b hash.
    Args:
        file_content: Bytes of file content.
        hash_name: Hashing algorithm to use.

    Returns:
        The computed SHA-256 hash.
    """
    hasher = hashlib.new(hash_name)
    for chunk in file_content:
        hasher.update(chunk)
    return hasher.hexdigest()
