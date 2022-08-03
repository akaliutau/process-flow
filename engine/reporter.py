import sys

FORMATTER = 'simple' if sys.stdout.isatty() else 'tsv'
HEADERS = ['no', 'step']


class Reporter:

    def __init__(self):
        pass
