 # When Notebooks Go Lite
 ## A JupyterLite Integration Guide for Jupyter Books

 > Can you write me a configuration example how to integrate JupyterLite into a Jupyter Book?

To integrate JupyterLite into a Jupyter Book, you need to follow these steps:

1. Install Jupyter Book and JupyterLite:

   ```
   pip install jupyter-book jupyterlite
   ```

2. Create a new Jupyter Book project:

   ```
   jupyter-book create my-jupyter-book
   cd my-jupyter-book
   ```

3. Add JupyterLite to your Jupyter Book:

   a. Download the JupyterLite distribution:

   ```
   wget https://github.com/jupyterlite/jupyterlite/releases/download/v0.1.0-alpha.11/jupyterlite-0.1.0-alpha.11.tar.gz
   tar -xzf jupyterlite-0.1.0-alpha.11.tar.gz -C ./_static/
   mv _static/jupyterlite-0.1.0-alpha.11 _static/jupyterlite
   ```

```{note}
The latest release of jupyterlite is v0.1.0b22 and the currect commands are:
wget https://github.com/jupyterlite/jupyterlite/releases/download/v0.1.0b22/jupyterlite-0.1.0b22.tar.gz
tar -xzf jupyterlite-0.1.0b22.tar.gz -C ./_static/
mv _static/jupyterlite-0.1.0-alpha.11 _static/jupyterlite
```

   b. In the `_static` folder, create a new file named `jupyterlite.js` with the following content:

   ```javascript
   document.addEventListener('DOMContentLoaded', async function () {
       const notebookLinks = document.querySelectorAll('a[href$=".ipynb"]');

       for (const notebookLink of notebookLinks) {
           notebookLink.href = '/_static/jupyterlite/lab/tree' + notebookLink.pathname;
       }
   });
   ```

   This script replaces the default links to `.ipynb` files with links that load the notebooks in JupyterLite.

4. Include the `jupyterlite.js` script in your Jupyter Book. Open the `_config.yml` file and add the following lines under the `sphinx:` section:

   ```yaml
   extra_javascript:
     - jupyterlite.js
   ```

   Your `_config.yml` should look like this:

   ```yaml
   title: My Jupyter Book
   logo: logo.png
   author: Your Name
   copyright: Copyright 2021

   sphinx:
     extra_javascript:
       - jupyterlite.js

   repository:
     url: https://github.com/yourusername/yourrepository
     path_to_book: docs
     branch: main

   launch_buttons:
     binderhub_url: https://mybinder.org
     thebe: true
     colab_url: https://colab.research.google.com
   ```

5. Build your Jupyter Book:

   ```
   jupyter-book build .
   ```

Now, when you open your Jupyter Book, any links to `.ipynb` files will open in JupyterLite, allowing users to interact with the notebooks right in the browser.

Remember to replace the placeholders in the `_config.yml` file with your actual repository information if you plan to host your Jupyter Book on GitHub Pages or another platform.

Note: JupyterLite is still in the early stages of development, and some features may not work as expected. Keep an eye on the JupyterLite repository for updates and improvements.