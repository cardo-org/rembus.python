# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'Rembus'
copyright = '2025, Attilio Donà'
author = 'Attilio Donà'
release = '0.2.0'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.autodoc",
    'sphinx.ext.napoleon',
    'sphinx.ext.autosummary',
    'sphinx_inline_tabs',
]

#'sphinx_tabs.tabs',

autodoc_default_options = {
    'members': True,
    'undoc-members': True,
    'imported-members': True,  # this is important
}

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']



# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'furo'

html_theme_options = {
    "light_css_variables": {
        "color-brand-primary": "#306998",  # blue from logo
        "color-brand-content": "#0b487a",  # blue more saturated and less dark
    },
    "dark_css_variables": {
        "color-brand-primary": "#ffd43bcc",  # yellow from logo, more muted than content
        "color-brand-content": "#ffd43bd9",  # yellow from logo, transparent like text
    },
    "sidebar_hide_name": False,
}

html_static_path = ['_static']
