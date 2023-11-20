# Creating a GitHub Page with the Executable Book Project

> Write me a template for a github page using the Executable Book Project.

The Executable Book Project (EBP) is a set of tools for generating beautiful, publication-quality books and documents from computational material. In this template, we'll create a simple GitHub page for a book using EBP. You can customize the content, layout, and design elements to fit your project.

To get started, follow these steps:

1. Create a new repository on your GitHub account.

2. Clone the repository to your local machine using the command `git clone <repository_url>`.

3. Install the required tools for EBP by running `pip install jupyter-book`.

4. Navigate to the repository folder and run `jupyter-book create mybookname` to create a new book template.

5. Change into the newly created book directory using `cd mybookname`.

Now, you have the basic structure of an EBP book. The main components include:

- `_config.yml`: This file contains the configuration settings for your book, such as the title, author, and theme.
- `toc.yml`: This file defines the table of contents for your book, listing the files and sections to be included.
- `content/`: This directory contains the Markdown, Jupyter Notebook, and other files that make up the content of your book.

To customize your book, follow these steps:

1. Modify the `_config.yml` file to set the `title`, `author`, and any other desired settings. For example:

```yaml
title: "My Awesome Book"
author: "Your Name"
logo: "images/logo.png"
```

2. Add your content files (Markdown or Jupyter Notebooks) to the `content/` directory. For example, create a file named `introduction.md` with your book's introduction.

3. Update the `toc.yml` file to include the new content files in the desired order. For example:

```yaml
- file: content/introduction
  title: Introduction
- file: content/chapter1
  title: Chapter 1
```

4. Optionally, add any images or other media files to the `content/images/` directory, and reference them in your content files using relative paths.

5. To preview your book locally, run `jupyter-book build .` in the book directory. This will create a `_build/` directory with the HTML files.

6. To publish your book to GitHub Pages, follow these steps:

   a. Install the `ghp-import` tool by running `pip install ghp-import`.

   b. Run `ghp-import -n -p -f _build/html` from the book directory. This command pushes the contents of `_build/html` to the `gh-pages` branch of your repository.

   c. Go to your repository's settings on GitHub, scroll down to the "GitHub Pages" section, and make sure the "Source" is set to "gh-pages branch".

7. Your book is now published on GitHub Pages! You can view it at the URL provided in the "GitHub Pages" section of your repository settings (e.g., `https://<username>.github.io/<repository>`).

Remember to commit your changes to the repository using git and push them to GitHub. This ensures that your book's source files are stored and version-controlled.

```bash
git add .
git commit -m "Initial book setup"
git push origin main
```

Feel free to customize this template further by exploring the extensive [documentation](https://jupyterbook.org/intro.html) provided by the Executable Book Project.

> That's excellent advice! What changes do I need if I want to publish the page on `atrawog.org` instead of `https://<username>.github.io/<repository>`?


To publish your Jupyter Book on a custom domain like `atrawog.org`, follow these steps:

1. Purchase the domain (e.g., `atrawog.org`) from a domain registrar if you haven't already.

2. Update your domain's DNS settings with your registrar to point to GitHub Pages. You need to create two records:

   a. An `A` record that points to the following IP addresses:

   ```
   185.199.108.153
   185.199.109.153
   185.199.110.153
   185.199.111.153
   ```

   b. A `CNAME` record that points to your GitHub Pages URL:

   ```
   yourusername.github.io.
   ```

   Note that the exact steps to configure DNS records will vary depending on yourdomain registrar. Consult their documentation for specific instructions.

3. In your Jupyter Book repository, create a file named `CNAME` (without any extension) in the root directory. Add a single line to this file containing your custom domain, for example:

   ```
   atrawog.org
   ```

   Commit and push this file to your repository.

4. In your repository's settings on GitHub, scroll down to the "GitHub Pages" section. In the "Custom domain" field, enter your custom domain (e.g., `atrawog.org`) and save the changes.

5. Make sure that your custom domain is configured to use HTTPS. In the "GitHub Pages" section, check the "Enforce HTTPS" option. Note that it might take a few minutes to hours for the SSL/TLS certificate to be issued by GitHub.

6. After the DNS settings have propagated (which can take up to 24-48 hours), your Jupyter Book should be accessible at your custom domain (e.g., `https://atrawog.org`).

Please note that the steps above assume that you want to host your Jupyter Book on the root of your custom domain. If you want to host it under a subdomain or subdirectory, you'll need to adjust the DNS settings and `CNAME` file accordingly. You can find more information in the [GitHub Pages documentation](https://docs.github.com/en/pages/configuring-a-custom-domain-for-your-github-pages-site).
