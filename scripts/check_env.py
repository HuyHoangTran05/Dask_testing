import sys
import dask
import pandas as pd
import pyarrow as pa

print("Python executable:", sys.executable)
print("Python version:", sys.version)
print("Dask version:", dask.__version__)
print("Pandas version:", pd.__version__)
print("PyArrow version:", pa.__version__)