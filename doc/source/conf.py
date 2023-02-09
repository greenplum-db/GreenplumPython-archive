# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys

sys.path.insert(0, os.path.abspath("../../greenplumpython"))
with open("../../greenplumpython/VERSION") as f:
    release_version = list(f.readlines())[0]

# -- Project information -----------------------------------------------------

project = "GreenplumPython"
copyright = "2022, VMware"
author = "VMware"


# The full version, including alpha/beta/rc tags
release = release_version


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx_rtd_theme",
    "nbsphinx",
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = []


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "sphinx_rtd_theme"

html_context = {
    "display_github": True,
    "github_user": "greenplum-db",
    "github_repo": "GreenplumPython",
    "github_version": "main/doc/source/",
}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
# html_static_path = ["_static"]

suppress_warnings = [
    # FIXME: *THIS IS TERRIBLE.* Generally speaking, we do want Sphinx to inform
    # us about cross-referencing failures. Remove this hack entirely after Sphinx
    # resolves this open issue:
    #    https://github.com/sphinx-doc/sphinx/issues/4961
    # Squelch mostly ignorable warnings resembling:
    #     WARNING: more than one target found for cross-reference 'DataFrame':
    #     dataframe.DataFrame, pandas.dataframe.DataFrame
    #
    # Sphinx currently emits *HUNDREDS* of these warnings against our
    # documentation. All of these warnings appear to be ignorable. Although we
    # could explicitly squelch *SOME* of these warnings by canonicalizing
    # relative to absolute references in docstrings, Sphinx emits still others
    # of these warnings when parsing PEP-compliant type hints via static
    # analysis. Since those hints are actual hints that *CANNOT* by definition
    # by canonicalized, our only recourse is to squelch warnings altogether.
    # In consequence, some DataFrame references in our doc are not correct.
    "ref.python",
]
