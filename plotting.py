# -*- coding: utf-8 -*-
"""
plotting utils
--------------
This module contains plotting utility functions.
"""


def format_axis(x: float, pos=None) -> str:
    """Format currency
    The two args are the value and tick position, pos is not used,
    but it is required by matplotlib.
    :param x: value to format.
    :param pos: tick position.
    :return: formatted value.
    """
    threshold = [(1e12, "b"), (1e9, "mm"), (1e6, "m"), (1e3, "k"), (1, "na")]
    for scale, suffix in threshold:
        if x >= scale:
            return f"${x / scale:,.2f} {suffix}"
    return f"${x:,.2f}"



