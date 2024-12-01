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
