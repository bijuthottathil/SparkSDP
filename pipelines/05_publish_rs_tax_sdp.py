import os
from pyspark.sql import SparkSession
from pyspark import pipelines as dp

GOLD_TABLES = [
    "gold_tax_annual_summary",
    "gold_tax_income_bands",
]

spark = SparkSession.active()

_local_mode = os.environ.get("LOCAL_MODE", "false").lower() == "true"
_script_dir = os.environ.get("SCRIPT_DIR", os.getcwd())

if _local_mode:
    _src_root = f"file:{os.path.join(_script_dir, 'spark-warehouse')}"
    _dst_root = f"file:{os.path.join(_script_dir, 'rs-tax-export')}"
else:
    _adls_account = os.environ.get("ADLS_ACCOUNT_NAME", "")
    if not _adls_account:
        raise ValueError("ADLS_ACCOUNT_NAME must be set for cloud mode.")
    _container = "medallion"
    _src_root = f"abfs://{_container}@{_adls_account}.dfs.core.windows.net/ldp-tax/warehouse"
    _dst_root = f"abfs://{_container}@{_adls_account}.dfs.core.windows.net/published-tax-data"

for _t in GOLD_TABLES:
    dp.create_sink(
        name=f"rs_tax_sink__{_t}",
        format="parquet",
        options={
            "path": f"{_dst_root}/{_t}",
            "checkpointLocation": f"{_dst_root}/_checkpoints/{_t}",
        },
    )

    @dp.append_flow(name=f"publish__{_t}", target=f"rs_tax_sink__{_t}")
    def _publish_table(_t=_t):
        return spark.readStream.table(_t)
