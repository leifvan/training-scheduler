from docstr_coverage import get_docstring_coverage
from typing import List, Union
import os

if __name__ == "__main__":
    # collect files
    files: List[Union[os.PathLike, str]] = []
    for dirpath, _, filenames in os.walk('training_scheduler'):
        files.extend(os.path.join(dirpath, name) for name in filenames if name.endswith('.py'))

    get_docstring_coverage(files, verbose=3)
