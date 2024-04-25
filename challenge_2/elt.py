import os
import yaml
import pandas as pd
import numpy as np
from snowflake.snowpark.session import Session

class ETL:
    def __init__(self):
        