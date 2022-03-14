import sys
from pathlib import Path
import pandas as pd
import numpy as np
import pytest
# adding the path of the package to load modules
# this makes all functions available
adp_path = str(Path(sys.path[0]).parent.absolute() / "aeriusdatapipeline")
sys.path.append(adp_path)
import export as exp
