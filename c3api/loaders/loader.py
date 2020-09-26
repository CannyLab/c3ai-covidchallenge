
import typing
from pandas import DataFrame

class Loader:

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
    
    def fetch(self,) -> DataFrame:
        raise NotImplementedError("subclasses of Loader must implement fetch using the C3 AI Datalake")