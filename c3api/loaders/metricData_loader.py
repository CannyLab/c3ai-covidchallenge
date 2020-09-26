import c3api.c3aidatalake as datalake
import numpy as np
import pandas as pd
from typing import List

from c3api.loaders.loader import Loader

class MetricDataLoader(Loader):
    def __init__(self, start_date, end_date, interval):
        super(MetricDataLoader, self).__init__(start_date, end_date)

    def fetch(self, locations: List, stat_groups: List, interval: str = "DAY"):
        assert len(locations) > 0 and len(stat_groups) > 0, "Locations and Statistic Groups must not be empty"

        spec = {
            "ids": locations,
            "expressions": stat_groups,
            "start": self.start_date,
            "end": self.end_date,
            "interval": interval
        }

        return datalake.evalmetrics("outbreaklocation", {"spec": spec})

class MetricGroups:
    JHU_GROUPS = [
        "JHU_ConfirmedCases",
        "JHU_ConfirmedDeaths",
        "JHU_ConfirmedRecoveries",
        "JHU_ConfirmedCasesInterpolated",
        "JHU_ConfirmedDeathsInterpolated",
        "JHU_ConfirmedRecoveriesInterpolated",
    ]

    