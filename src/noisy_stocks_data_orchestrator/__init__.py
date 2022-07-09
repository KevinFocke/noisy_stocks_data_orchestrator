__version__ = "0.1.0"
# Pytest shenanigans workaround.
# Explanation: Pytest does not find nodule while python does
# Elaboration: https://stackoverflow.com/a/69691436
import pathlib
import sys

sys.path.append(str(pathlib.Path(__file__).parent))
