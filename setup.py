from setuptools import setup

APP = ['YT_earnings_parse.py']
DATA_FILES = []
OPTIONS = {
    'argv_emulation': True,
    'packages': ['pandas', 'pydrive'],  # Only include third-party packages you really need.
    'excludes': ['packaging'],          # Exclude packaging to prevent conflicts.
}

setup(
    app=APP,
    data_files=DATA_FILES,
    options={'py2app': OPTIONS},
    setup_requires=['py2app'],
)