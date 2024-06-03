# -*- encoding: utf-8 -*-
from typing import Any

import numpy as np
import plotly.express as px
import polars as pl
import polars.selectors as cs

from sklearn.decomposition import PCA
from sklearn import set_config

set_config(transform_output="polars")


class PcaTransformer:
    """
    - The PcaTransformer class is a wrapper for the PCA class from the sklearn library.
    - It allows fitting and transform a dataset using PCA and provides some useful methods
    to analyze the results of the PCA.
    -The class requires the number of components to use in the pca model.
    - The class has the following methods:
        - fit: Fit the PCA model to the dataset.
        - transform: Transform the dataset using the fitted PCA model.
        - filter_components: Filter the components of the PCA model based on a threshold.
        - plot_pca_variance: Plot the variance explained by each principal component.
        - plot_pca_components: Plot the components of the PCA model.
        - plot_pca_3d: Plot the dataset in 3D using the principal components as the x, y, and z axes.

    """

    def __init__(self, n_components: int = 3) -> None:
        """
        Initialize the PcaTransformer class.
        :param n_components: The number of components to use in the PCA model.
        """
        self.columns = None
        self._n_components = n_components
        self._pca: PCA = PCA(n_components=self._n_components)

    def __repr__(self) -> str:
        return f"Pca(n_components={self._n_components})"

    @property
    def n_components(self) -> int:
        return self._n_components

    @property
    def make_pca_components_dataframe(self):
        """
        Create a DataFrame with the components of the PCA model.
        :return: pl.DataFrame: A DataFrame with the components of the PCA model.
        """
        return (
            pl.DataFrame(self._pca.components_, orient="col")
            .rename(
                mapping={f"column_{i}": f"PC_{i + 1}" for i in range(self.n_components)}
            )
            .with_columns(pl.Series(pl.Series(self.columns)).alias("feature"))
        )

    @property
    def explained_variance_ratio(self) -> np.ndarray:
        """
        Get the variance explained by each principal component.
        :return: np.ndarray: The variance explained by each principal component.
        """
        return self._pca.explained_variance_ratio_

    def fit(self, x_train: pl.DataFrame) -> "PcaTransformer":
        """
        Fit the PCA model to the dataset.
        :param x_train: The dataset to fit the PCA model to.
        :return: PcaTransformer: The PcaTransformer object.
        """
        self._pca.fit(x_train)
        self.columns = x_train.columns
        return self

    def transform(self, x_test: pl.DataFrame) -> pl.DataFrame:
        """
        Transform the dataset using the fitted PCA model.
        :param x_test: The dataset to transform.
        :return: pl.DataFrame: The transformed dataset.
        """
        return pl.DataFrame(self._pca.transform(x_test)).rename(
            mapping={f"pca{i}": f"PC{i + 1}" for i in range(self._n_components)}
        )

    def filter_components(
        self, limit_components: int = 2, threshold: float = 0.1
    ) -> pl.DataFrame:
        """
        Filter the components of the PCA model based on a threshold.
        :param limit_components: The number of components to filter.
        :param threshold: The threshold to filter the components.
        :return: pl.DataFrame: The filtered components of the PCA model.
        """
        pca_components = self.make_pca_components_dataframe
        columns = pca_components.columns[:limit_components]
        return pca_components.select(pl.col(columns), pl.col("feature")).filter(
            pl.any_horizontal(cs.numeric().abs().gt(threshold))
        )

    def plot_pca_variance(self, **kwargs) -> Any:
        """
        Plot the variance explained by each principal component.
        :param kwargs: Additional keyword arguments to pass to the plot method.
        :return: hv: The plot of the variance explained by each principal component.
        """
        return pl.DataFrame(
            {
                "PC": [f"PC_{i + 1}" for i in range(self.n_components)],
                "var": self.explained_variance_ratio,
            }
        ).plot(x="PC", **kwargs)

    def plot_pca_components(
        self, limit_components: int, threshold: float = 0.1, **kwargs
    ) -> Any:
        """
        Plot the components of the PCA model.
        :param limit_components: The number of components to plot.
        :param threshold: The threshold to filter the components.
        :param kwargs: Additional keyword arguments to pass to the plot method.
        :return: hv: The plot of the components of the PCA model.

        """
        components = self.filter_components(limit_components, threshold=threshold)
        return (
            components.select(cs.numeric())
            .transpose()
            .rename(
                mapping={
                    f"column_{i}": str(col)
                    for i, col in enumerate(
                        components.select(pl.col("feature")).to_series()
                    )
                }
            )
            .with_columns(
                PC=pl.Series([f"PC_{i + 1}" for i in range(limit_components)])
            )
            .plot.bar(x="PC", **kwargs)
        )

    def plot_pca_3d(
        self,
        dataset: pl.DataFrame,
        x: str = "PC1",
        y: str = "PC2",
        z: str | None = "PC3",
        color_col: pl.Series | None = None,
        cmap: str = "viridis",
        biplot: bool = True,
        biplot_scale: int = 20,
        alpha: float = 1,
        width: int = 1000,
        height: int = 600,
        symbol_col: pl.Series | None = None,
        size_col: pl.Series | None = None,
        color_bar_title: str = "Color",
        **kwargs,
    ) -> Any:
        """
        Plot the dataset in 3D using the principal components as the x, y, and z axes.
        :param dataset: The dataset to plot.
        :param x: The column to use as the x-axis.
        :param y: The column to use as the y-axis.
        :param z: The column to use as the z-axis.
        :param color_col: The column to use as the color.
        :param cmap: The colormap to use.
        :param biplot: Whether to plot the biplot.
        :param biplot_scale: The scale of the biplot.
        :param alpha: The opacity of the points.
        :param width: The width of the plot.
        :param height: The height of the plot.
        :param symbol_col: The column to use as the symbol.
        :param size_col: The column to use as the size.
        :param kwargs: Additional keyword arguments to pass to the plot method.
        :return: px: The plot of the dataset in 3D.
        """
        data = self.transform(dataset)
        if color_col is not None:
            data = data.with_columns(color_col)

        fig = px.scatter_3d(
            data,
            x=x,
            y=y,
            z=z,
            color=color_col,
            color_continuous_scale=cmap,
            hover_data=data.columns[:3],
            symbol=symbol_col,
            size=size_col,
            opacity=alpha,
            width=width,
            height=height,
            **kwargs,
        )
        fig.update_coloraxes(colorbar_title=color_bar_title)

        if size_col is None:
            fig.update_traces(marker_size=3)

        if biplot:
            scale = biplot_scale
            annots = [
                {
                    "showarrow": False,
                    "x": loadings["PC1"],
                    "y": loadings["PC2"],
                    "z": loadings["PC3"],
                    "text": column,
                    "xanchor": "left",
                    "xshift": 1,
                    "opacity": 0.7,
                }
                for column in dataset.columns
                for loadings in [
                    {
                        f"PC{i}": val * scale
                        for i, val in enumerate(
                            dataset.select(pl.col(column)).to_series(), 1
                        )
                    }
                ]
                for new_fig in [
                    px.line_3d(
                        x=[0, loadings["PC1"]],
                        y=[0, loadings["PC2"]],
                        z=[0, loadings["PC3"]],
                        width=20,
                    )
                ]
                if not any(
                    fig.add_trace(trace, row=1, col=1) for trace in new_fig["data"]
                )
            ]
            fig.update_layout(scene={"annotations": annots})
        return fig
