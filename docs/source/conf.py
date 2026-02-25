# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# Adapted from Apache Arrow
# https://github.com/apache/arrow/blob/main/docs/source/conf.py

# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

import os

project = "C++ Paimon"
copyright = "2024-present Alibaba Inc."
author = "Alibaba Inc."

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "breathe",
    "myst_parser",
    "sphinx_design",
    "sphinx_copybutton",
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.doctest",
    "sphinx.ext.ifconfig",
    "sphinx.ext.intersphinx",
    "sphinx.ext.mathjax",
    "sphinx.ext.viewcode",
    "sphinxcontrib.mermaid",
]

# Show members for classes in .. autosummary
autodoc_default_options = {
    "members": None,
    "special-members": "__dataframe__",
    "undoc-members": None,
    "show-inheritance": None,
    "inherited-members": None,
}

# Breathe configuration
breathe_projects = {
    "paimon_cpp": os.environ.get("PAIMON_CPP_DOXYGEN_XML", "../../apidoc/xml"),
}
breathe_default_project = "paimon_cpp"

# Overridden conditionally below
autodoc_mock_imports = []

# copybutton configuration
copybutton_prompt_text = r">>> |\.\.\. |\$ |In \[\d*\]: | {2,5}\.\.\.: "
copybutton_prompt_is_regexp = True
copybutton_line_continuation_character = "\\"

# MyST-Parser configuration
myst_enable_extensions = [
    "amsmath",
    "attrs_inline",
    "deflist",
    "dollarmath",
    "fieldlist",
    "html_admonition",
    "html_image",
    "linkify",
    "strikethrough",
    "substitution",
    "tasklist",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "pydata_sphinx_theme"
html_static_path = ["_static"]

# Custom fixes to the RTD theme
html_css_files = ["theme_overrides.css"]

# Hide the primary sidebar (section navigation) for these pages
html_sidebars = {
    "implementations": [],
    "status": [],
}

# The master toctree document.
master_doc = "index"

version = "0.1.0"

html_theme_options = {
    "show_toc_level": 2,
    "show_nav_level": 2,
    "use_edit_page_button": True,
    "header_links_before_dropdown": 4,
    "navbar_align": "left",
    "navbar_end": ["theme-switcher", "navbar-icon-links"],
    "icon_links": [
        {
            "name": "GitHub",
            "url": "https://github.com/alibaba/paimon-cpp",
            "icon": "fa-brands fa-square-github",
        },
    ],
    "logo": {
        "text": "Paimon C++",
    },
    "show_version_warning_banner": True,
}

html_context = {
    "github_user": "alibaba",
    "github_repo": "paimon-cpp",
    "github_version": "main",
    "doc_path": "docs/source",
}

html_title = f"C++ Paimon"

html_show_sourcelink = False
