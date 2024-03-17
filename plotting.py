# -*- coding: utf-8 -*-
"""
plotting utils
--------------
This module contains plotting utility functions.
"""
import matplotlib.pyplot as plt
from typing import Any, Callable


def format_axis(value: float, tick_position=None) -> str:
    """format currency
    The two args are the value and tick position, pos is not used,
    but it is required by matplotlib.
    :param value: value to format.
    :param tick_position: tick position.
    :return: formatted value.
    """
    scales_and_suffixes = [(1e12, "b"), (1e9, "mm"), (1e6, "m"), (1e3, "k"), (1, "na")]
    for scale, suffix in scales_and_suffixes:
        if value >= scale:
            return f"{value / scale:,.2f}{suffix}"
    return f"{value:,.2f}"


def label_bar_chart(
    axes: plt.Axes,
    fmt: str | Callable,
    **kwargs,
) -> None:
    """annotate a bar chart
    :param axes: axes object:
    :param fmt: format string or callable.
    see: https://matplotlib.org/stable/api/_as_gen/matplotlib.axes.Axes.bar_label.html
    :param kwargs: keyword arguments passed to axes.Annotate
    see: https://matplotlib.org/stable/api/_as_gen/matplotlib.axes.Axes.annotate.html"""
    for container_object in axes.containers:
        axes.bar_label(container=container_object, fmt=fmt, **kwargs)


def label_chart_line(
    axes: plt.Axes,
    line_object: plt.Line2D,
    xy_text_coords: tuple[int, int] = (0, 5),
    color: str | None = None,
    **kwargs,
) -> None:
    """Annotate a line chart
    :param axes: axes object.
    :param line_object: Line2D object.
    :param xy_text_coords: Xy text coordinates.Default to (0, 5).
    :param color: Color of data values. Defaults to None.
    :param kwargs: Keyword arguments passed to axes. Annotate
    see: https://matplotlib.org/stable/api/_as_gen/matplotlib.axes.Axes.annotate.html
    """
    x_data, y_data = line_object.get_xdata(), line_object.get_ydata()
    text_color: str = color if color else line_object.get_color()
    for x_value, y_value in zip(x_data, y_data):
        text: Any = scale_number_values(y_value)
        axes.annotate(
            text=text,
            xy=(x_value, y_value),
            xytext=xy_text_coords,
            color=text_color,
            textcoords="offset points",
            **kwargs,
        )


def scale_number_values(value: str | int | float) -> str | int | float:
    """Format number scale:
    :param value: value to format.
    :return: formatted value.
    """
    try:
        numeric_value: float = float(value)
    except ValueError:
        return value

    scale_factors: list = [1e12, 1e9, 1e6, 1e3, 1]
    for scale in scale_factors:
        if numeric_value >= scale:
            return f"{numeric_value / scale:,.1f}"
