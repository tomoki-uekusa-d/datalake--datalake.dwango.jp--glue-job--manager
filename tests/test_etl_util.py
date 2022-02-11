from tkinter import W
import findspark
findspark.init()
findspark.find()

import pytest
import logging
from contextlib import ExitStack
from unittest.mock import Mock, patch

from pyspark.sql import SparkSession

from chispa.dataframe_comparer import assert_df_equality

from src.lib import etl_util 


@pytest.fixture(scope="session")
def spark(request):
    spark = SparkSession.builder.master("local").appName("etl-unit-test").getOrCreate()
    request.addfinalizer(lambda: spark.stop())

    # Quite py4j log
    # logger = logging.getLogger("py4j")
    # logger.setLevel(logging.WARN)

    return spark


@pytest.mark.parametrize(
    "testcase, expected",
    [
        ("HOGEHOGE", True),
        ("FUGAFUGA", False),
        (None, False)
    ],
)
def test_hogehogehoge(testcase, expected):
    assert etl_util.hogehogehoge(testcase) == expected


def test_convert_dataframe(spark):
    source_data = [
        ("jo&&se",),
        ("**li**",),
        ("#::luisa",),
        (None,)
    ]
    source_schema = ["name"]
    source_df = spark.createDataFrame(source_data, source_schema)
    actual_df = etl_util.convert_dataframe(source_df)

    expected_data = [
        ("jo&&se", "jose"),
        ("**li**", "li"),
        ("#::luisa", "luisa"),
        (None, None)
    ]
    expected_schema = ["name", "clean_name"]
    expected_df = spark.createDataFrame(expected_data, expected_schema)

    assert_df_equality(actual_df, actual_df)
