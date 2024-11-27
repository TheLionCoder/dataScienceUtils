# -*- coding: utf-8 -*-

"""
utils
-----
This module contains utility functions.
"""

import hashlib
from pathlib import Path
from typing import List, Generator

import polars as pl


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
