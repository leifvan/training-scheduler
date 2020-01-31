from docstr_coverage import get_docstring_coverage
import os

files = [entry.path for entry in os.scandir() if entry.path.endswith('.py') and not os.path.samefile(entry.path, __file__)]
file_specific, total = get_docstring_coverage(files, verbose=3)