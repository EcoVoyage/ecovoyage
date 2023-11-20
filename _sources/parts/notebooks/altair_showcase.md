---
jupytext:
  formats: md:myst
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.11.4
kernelspec:
  display_name: Python 3
  language: python
  name: python3
---

# Altair Showcase

>  Can you generate my a version of the altair notebook that showcases the different functionalities of Altair?


Sure! Here's an extended version of the MyST Markdown Jupyter notebook that demonstrates various Altair functionalities, including different chart types, data transformations, and interactivity.


This Jupyter notebook demonstrates various functionalities of Altair, including different chart types, data transformations, and interactivity.

## Import Libraries

```{code-cell} ipython3
import altair as alt
import pandas as pd
import numpy as np
```

## Bar Chart

Create a simple bar chart using sample data.

```{code-cell} ipython3
data = pd.DataFrame({
    'category': ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I'],
    'value': [28, 55, 43, 91, 81, 53, 19, 87, 52]
})

bar_chart = alt.Chart(data).mark_bar().encode(
    x='category',
    y='value'
)

bar_chart
```

## Line Chart

Create a line chart using a sine wave.

```{code-cell} ipython3
x = np.linspace(0, 4 * np.pi, 100)
y = np.sin(x)

data = pd.DataFrame({'x': x, 'y': y})

line_chart = alt.Chart(data).mark_line().encode(
    x='x',
    y='y'
)

line_chart
```

## Scatter Plot

Create a scatter plot using random data.

```{code-cell} ipython3
data = pd.DataFrame({
    'x': np.random.rand(50),
    'y': np.random.rand(50),
    'size': np.random.rand(50) * 100
})

scatter_plot = alt.Chart(data).mark_circle().encode(
    x='x',
    y='y',
    size='size'
)

scatter_plot
```

## Data Transformations

Create a histogram using the `transform_bin` function in Altair.

```{code-cell} ipython3
data = pd.DataFrame({
    'x': np.random.normal(0, 1, 1000)
})

histogram = alt.Chart(data).mark_bar().encode(
    alt.X("x_bin:Q", bin=alt.Bin(maxbins=50)),
    alt.Y("count():Q")
).transform_bin(
    "x_bin", "x"
)

histogram
```

## Interactivity

Add interactivity to a scatter plot by enabling tooltips and zooming/panning.

```{code-cell} ipython3
data = pd.DataFrame({
    'x': np.random.rand(100),
    'y': np.random.rand(100),
    'label': np.arange(100)
})

interactive_scatter_plot = alt.Chart(data).mark_circle().encode(
    x='x',
    y='y',
    tooltip=['label']
).interactive()

interactive_scatter_plot
```

This notebook showcases a few of Altair's functionalities. You can explore the [official Altair documentation](https://altair-viz.github.io/) to learn more about the library and discover more advanced features. Save this template as a `.md` file and open it in your Jupyter environment to create a MyST Markdown Jupyter notebook.