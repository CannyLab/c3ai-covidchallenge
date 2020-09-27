import c3api.c3aidatalake as datalake
import numpy as np
import pandas as pd
from typing import List

from c3api.loaders.loader import Loader

class GeneralDataLoader(Loader):

    def __init__(self,):
        super(GeneralDataLoader, self).__init__(None, None)


    def fetch(self, location: str, include_fields: List, limit: int = None) -> pd.DataFrame:
        """
        Fetch General Data about various locations supported by the C3 API

        Args:
            location (str): Location ID (i.e. "UnitedStates" or "Alameda_California_UnitedStates")
            include_fields (List): List of fields to return (reference API) parameter is a powerful way to fetch data from multiple C3.ai Types
            limit (int, optional): [description]. Max Limit of rows

        Returns:
            pd.DataFrame: [description]
        """

        assert len(location) > 0, "location must be specified"
        
        spec = {
            "filter": "id == '{}'".format(location)
        }

        if len(include_fields) > 0:
            spec["include"] = include_fields
        
        if limit:
            spec["limit"] = limit

        return datalake.fetch("outbreaklocation", { "spec": spec })


        