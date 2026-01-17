
from dataclasses import dataclass

from google.cloud import bigquery as bq
import polars as pl



class SerializeParams:
    ...


def _get_param(
    param_name: str,
    param_value: Any,
) -> bq.ScalarQueryParameter | bq.ArrayQueryParameter:
    """Convert a python object to BigQuery query parameter.

    @param param_name: Parameter name
    @type param_name: str
    @param param_value: Parameter value
    @type param_value: Any
    @return: BigQuery query parameter
    @rtype: bq.ScalarQueryParameter | bq.ArrayQueryParameter
    """
    if isinstance(param_value, pl.Series):
        param_value = param_value.to_list()
    if isinstance(param_value, list):
        assert len(param_value) > 0
        head_value = param_value[0]
        data_type_str = _ParamTypeMap[type(head_value)]
        return bq.ArrayQueryParameter(param_name, data_type_str, param_value)  # type: ignore
    data_type_str = _ParamTypeMap[type(param_value)]  # type: ignore
    return bq.ScalarQueryParameter(param_name, data_type_str, param_value)  # type: ignore



@dataclass
class BQPolarsHandler:
    client: bq.Client

    def insert_df(self, df: pl.DataFrame, table_id: str) -> None:
        raise NotImplementedError()


    def fetch_df(self, query: str) -> pl.DataFrame:
        raise NotImplementedError()
        ...
    