__version__ = "0.1.0"
# Fix pytest path discovery
import pathlib
import sys

sys.path.append(str(pathlib.Path(__file__).parent))
