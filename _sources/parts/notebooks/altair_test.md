---
jupytext:
  formats: md:myst
  text_representation:
    extension: .md
    format_name: myst
    format_veersion: 1.11.4
kernelspec:
  display_name: Python 3
  language: python
  name: python3
---

# Altair Test

> Can you generate me a jupyter notebook in MyST Markdown format. That includes at least one Altair Diagramm and can be used to test the integration of JupyterLite and Jupyter Book?

This is a Jupyter notebook in MyST Markdown format, including an Altair diagram. You can use this notebook to test the integration of JupyterLite and Jupyter Book.

First, import the necessary libraries:

```{code-cell} ipython3
import altair as alt
import pandas as pd
```

Next, create a sample dataset:

```{code-cell} ipython3
data = pd.DataFrame({
    'a': ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I'],
    'b': [28, 55, 43, 91, 81, 53, 19, 87, 52]
})
```

Now, create an Altair chart using the sample dataset:

```{code-cell} ipython3
chart = alt.Chart(data).mark_bar().encode(
    x='a',
    y='b'
)
```

Finally, display the chart:

```{code-cell} ipython3
chart
```

You should see a bar chart with letters on the x-axis and numbers on the y-axis. Save this template as a `.md` file and open it in your Jupyter environment to create a MyST Markdown Jupyter notebook. You can then test the integration with JupyterLite and Jupyter Book.