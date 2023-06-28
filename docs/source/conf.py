import sphinx_rtd_theme

# Add the theme to the extensions list
extensions = [
    "sphinx_rtd_theme",
    "sphinx.ext.autodoc"
]

project = 'DESC data management'
copyright = '2023, DESC'
author = 'DESC CI Working Group'

# Set the theme
html_theme = "sphinx_rtd_theme"
#html_theme_path = [sphinx_rtd_theme.get_html_theme_path()]

# Optionally, you can customize the theme further with theme-specific options
html_theme_options = {
    'logo_only': False,
    'display_version': True,
    'prev_next_buttons_location': 'bottom',
    'style_external_links': False,
    'vcs_pageview_mode': '',
    'style_nav_header_background': 'white',
    # Toc options
    'collapse_navigation': False,
    'sticky_navigation': True,
    'navigation_depth': 2,
    'includehidden': True,
    'titles_only': False,
}

html_static_path = ['_static']
html_logo = '_static/desc_logo.png'
