from docstr_coverage import get_docstring_coverage
import os

# collect files
files = []
for dirpath, _, filenames in os.walk('training_scheduler'):
    files.extend(os.path.join(dirpath, name) for name in filenames if name.endswith('.py'))

get_docstring_coverage(files, verbose=3)
