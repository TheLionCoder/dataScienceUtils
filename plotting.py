# -*- coding: utf-8 -*-
"""
plotting utils
--------------
This module contains plotting utility functions.
"""
from numbers import Real
import matplotlib.pyplot as plt
from typing import Any, Dict, List, Tuple


def format_axis(x: float, pos=None) -> str:
    """format currency
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


def label_bar_chart(
    axes: plt.Axes,
    **kwargs,
) -> None:
    """annotate a bar chart
    :param axes: axes object:
    :param kwargs: keyword arguments passed to axes.annotate
    see: https://matplotlib.org/stable/api/_as_gen/matplotlib.axes.Axes.annotate.html"""
    for container in axes.containers:
        labels = [format_number(value) for value in container.datavalues]
        axes.bar_label(
            container,
            labels=labels,
            **kwargs,
        )


def label_chart_line(
    axes: plt.Axes,
    text_positions: List[Tuple[float, float]],
    annotation_spacing: float = 0.1,
    **kwargs,
) -> None:
    """annotate a line chart
    :param axes: axes object.
    :param text_positions: list of tuples with x a y coordinates for text.
    :param kwargs: keyword arguments passed to axes.annotate
    :param annotation_spacing: spacing between annotations.Default to 0.1.
    see: https://matplotlib.org/stable/api/_as_gen/matplotlib.axes.Axes.annotate.html
    """

    lines: list = axes.get_lines()

    for line_object, text_pos in zip(lines, text_positions):
        _annotate_line_points(
            line_object=line_object,
            axes=axes,
            xytext=text_pos,
            annotation_spacing=annotation_spacing,
            **kwargs,
        )


def format_number(value: Real) -> str:
    """Format number scale:
    :param value: value to format.
    :return: formatted value.
    """
    scale_factors: list = [1e12, 1e9, 1e6, 1e3]
    for scale in scale_factors:
        if value < 1:
            return f"{value * 100: 4.1f}%"
        if value >= scale:
            return f"{value / scale:,.2f}"
    return f"{value:,.2f}"


def _annotate_line_points(
    line_object: plt.Line2D,
    axes: plt.Axes,
    xytext: tuple[float, float],
    annotation_spacing: float,
    **kwargs,
) -> None:
    """annotate line points
    :param line_object: plt.Line2D object.
    :param axes: axes object.
    :param xytext: tuple of x an y coordinates for text.
    :param annotation_spacing: spacing between annotations.
    :param kwargs: keyword arguments passed to axes.annotate
    see: https://matplotlib.org/stable/api/_as_gen/matplotlib.axes.Axes.annotate.html
    """
    # check annotation for each line
    annotated_points: dict[any, any] = {}
    x_data, y_data = (line_object.get_xdata(), line_object.get_ydata())
    line_color = line_object.get_color()
    for x, y in zip(x_data, y_data):
        # Calculate a unique key for the current point
        # based on the x and y values
        point_key = f"{x}_{y}"
        close_to_previous_point: bool = _check_for_overlap(
            x=x,
            y=y,
            annotation_spacing=annotation_spacing,
            annotated_points=annotated_points,
        )
        # Check if this point is too close to a previous annotated point
        new_xytext: tuple = (
            (xytext[0] + annotation_spacing, xytext[1])
            if close_to_previous_point
            else xytext
        )
        text = format_number(y)
        _annotate_point(
            axes=axes,
            x=x,
            y=y,
            text=text,
            line_color=line_color,
            xytext=new_xytext,
            **kwargs,
        )
        # Store the coordinates of the annotated point
        annotated_points[point_key] = (x, y)


def _annotate_point(
    axes: plt.Axes,
    x: float,
    y: float,
    text: str,
    line_color: str,
    xytext: tuple[float, float],
    **kwargs,
) -> None:
    """annotate a point
    :param axes: axes object.
    :param x: x coordinate.
    :param y: y coordinate.
    :param text: text to annotate.
    :param line_color: color of line.
    :param xytext: tuple of x an y coordinates for text.
    :param kwargs: keyword arguments passed to axes.annotate
    see: https://matplotlib.org/stable/api/_as_gen/matplotlib.axes.Axes.annotate.html
    """
    axes.annotate(
        text,
        xy=(x, y),
        xytext=xytext,
        textcoords="offset points",
        ha="center",
        color=line_color,
        **kwargs,
    )


def _check_for_overlap(
    x: float, y: float, annotation_spacing: float, annotated_points: Dict[Any, Any]
) -> bool:
    """check for overlap
    :param x: x coordinate.
    :param y: y coordinate.
    :param annotation_spacing: spacing between annotations.
    :param annotated_points: dict of annotated points.
    """
    return any(
        abs(x - prev_x) < annotation_spacing < abs(y - prev_y)
        for prev_x, prev_y in annotated_points.values()
    )
