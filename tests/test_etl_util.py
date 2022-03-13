from tkinter import W
import findspark
findspark.init()
findspark.find()

import pytest
import logging
from contextlib import ExitStack
from unittest.mock import Mock, patch

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from chispa.dataframe_comparer import assert_df_equality

from src.lib import etl_util 


@pytest.fixture(scope="session")
def spark(request):
    spark = SparkSession.builder.master("local").appName("etl-unit-test").getOrCreate()
    request.addfinalizer(lambda: spark.stop())
    return spark


def test_join_material(spark):
    test_dim_hub_item = spark.createDataFrame([
        ("111111", "dim_hub_item_1"),
        ("222222", "dim_hub_item_2"),
        ("333333", "dim_hub_item_3"),
    ], ["id", "column_A"])
    test_dim_hub_item_and_artist = spark.createDataFrame([
        ("111111", "1111", "dim_hub_item_and_artist_1"),
        ("222222", "2222", "dim_hub_item_and_artist_2"),
        ("333333", "3333", "dim_hub_item_and_artist_3"),
    ], ["item_id", "artist_id", "column_B"])
    test_dim_hub_music_and_item = spark.createDataFrame([
        ("111111", "111", "dim_hub_music_and_item_1"),
        ("222222", "222", "dim_hub_music_and_item_2"),
        ("333333", "333", "dim_hub_music_and_item_3"),
    ], ["item_id", "music_id", "column_C"])
    test_dim_hub_music = spark.createDataFrame([
        ("111", "dim_hub_music_1"),
        ("222", "dim_hub_music_2"),
        ("333", "dim_hub_music_3"),
    ], ["id", "column_D"])
    test_dim_hub_artist = spark.createDataFrame([
        ("1111", "dim_hub_artist_1"),
        ("2222", "dim_hub_artist_2"),
        ("3333", "dim_hub_artist_3"),
    ], ["id", "column_E"])
    actual_df = etl_util.join_dim_material(
        test_dim_hub_item,
        test_dim_hub_item_and_artist,
        test_dim_hub_music_and_item,
        test_dim_hub_music,
        test_dim_hub_artist,
    )

    expected_df = spark.createDataFrame([
        ("111111", "dim_hub_item_1", "1111", "dim_hub_item_and_artist_1", "111", "dim_hub_music_and_item_1", "dim_hub_music_1", "dim_hub_artist_1"),
        ("222222", "dim_hub_item_2", "2222", "dim_hub_item_and_artist_2", "222", "dim_hub_music_and_item_2", "dim_hub_music_2", "dim_hub_artist_2"),
        ("333333", "dim_hub_item_3", "3333", "dim_hub_item_and_artist_3", "333", "dim_hub_music_and_item_3", "dim_hub_music_3", "dim_hub_artist_3"),
    ], ["id", "column_A", "artist_id", "column_B", "music_id", "column_C", "column_D", "column_E"] )

    assert_df_equality(actual_df, expected_df, ignore_row_order=True)


def test_explode_tracks(spark):
    test_df = spark.createDataFrame([
        ('album_music_id_1', 'album_music_name_1', 'album_artist_id_1', 'album_artist_name_1', 'album_release_date_1', '{"Items": [{"AssetId": 100000, "Description": null, "ResourceType": "SecondaryResource", "SequenceNumber": 2}], "Groups": [{"Items": [{"AssetId": 11111, "Description": null, "ResourceType": "PrimaryResource", "SequenceNumber": 1}, {"AssetId": 22222, "Description": null, "ResourceType": "PrimaryResource", "SequenceNumber": 2}, {"AssetId": 33333, "Description": null, "ResourceType": "PrimaryResource", "SequenceNumber": 3}, {"AssetId": 44444, "Description": null, "ResourceType": "PrimaryResource", "SequenceNumber": 4}], "Groups": [], "Titles": [{"Title": "Disc 1", "SubTitle": null, "TitleType": "GroupingTitle", "SubTitleType": null, "TitleLanguage": null, "SubTitleLanguage": null}], "SequenceNumber": 1}]}'),
    ], ["album_music_id", "album_music_name", "album_artist_id", "album_artist_name", "album_release_date", "asset_structure"])
    actual_df = etl_util.explode_tracks(test_df)
    excepted_schema = StructType([
        StructField("album_music_id", StringType(), True),
        StructField("album_music_name", StringType(), True),
        StructField("album_artist_id", StringType(), True),
        StructField("album_artist_name", StringType(), True),
        StructField("album_release_date", StringType(), True),
        StructField("album_track_asset_id", IntegerType(), True),
    ])
    expected_df = spark.createDataFrame([
        ("album_music_id_1", "album_music_name_1", "album_artist_id_1", "album_artist_name_1", "album_release_date_1", 11111),
        ("album_music_id_1", "album_music_name_1", "album_artist_id_1", "album_artist_name_1", "album_release_date_1", 22222),
        ("album_music_id_1", "album_music_name_1", "album_artist_id_1", "album_artist_name_1", "album_release_date_1", 33333),
        ("album_music_id_1", "album_music_name_1", "album_artist_id_1", "album_artist_name_1", "album_release_date_1", 44444),
    ], excepted_schema)
    
    assert_df_equality(actual_df, expected_df)


def test_aggregate_musics_to_album(spark):
    test_schema = StructType([
        StructField("material_id", StringType(), True),
        StructField("material_name", StringType(), True),
        StructField("music_id", StringType(), True),
        StructField("music_name", StringType(), True),
        StructField("score", IntegerType(), True),
        StructField("artist_id", StringType(), True),
        StructField("artist_name", StringType(), True),
        StructField("release_date", StringType(), True),
        StructField("pattern_id", StringType(), True),
        StructField("tieup_detail_genre_id", StringType(), True),
        StructField("tieup_name", StringType(), True),
        StructField("tieup_id", StringType(), True),
        StructField("johnnys", StringType(), True),
        StructField("album_music_id", StringType(), True),
        StructField("album_music_name", StringType(), True),
        StructField("album_artist_id", StringType(), True),
        StructField("album_artist_name", StringType(), True),
        StructField("album_release_date", StringType(), True),
    ])
    expected_schema = StructType([
        StructField("music_id", StringType(), True),
        StructField("music_name", StringType(), True),
        StructField("material_id", StringType(), True),
        StructField("material_name", StringType(), True),
        StructField("artist_id", StringType(), True),
        StructField("artist_name", StringType(), True),
        StructField("release_date", StringType(), True),
        StructField("pattern_id", StringType(), True),
        StructField("tieup_detail_genre_id", StringType(), True),
        StructField("tieup_name", StringType(), True),
        StructField("tieup_id", StringType(), True),
        StructField("johnnys", StringType(), True),
        StructField("transition_type", StringType(), False),
        StructField("score", IntegerType(), True),
    ])
    test_df = spark.createDataFrame([
        ("material_id_1", "material_name_1", "music_id_1", "music_name_1", 600, "artist_id_1", "artist_name_1", "release_date_1", "pattern_id_1", "tieup_detail_genre_id_1", "tieup_name_1", "tieup_id_1", "johnnys_1", "album_music_id_1", "album_music_name_1", "album_artist_id_1", "album_artist_name_1", "album_release_date_1"),
        ("material_id_2", "material_name_2", "music_id_2", "music_name_2", 500, "artist_id_2", "artist_name_2", "release_date_2", "pattern_id_2", "tieup_detail_genre_id_2", "tieup_name_2", "tieup_id_2", "johnnys_2", "album_music_id_2", "album_music_name_2", "album_artist_id_2", "album_artist_name_2", "album_release_date_2"),
        ("material_id_3", "material_name_3", "music_id_3", "music_name_3", 400, "artist_id_3", "artist_name_3", "release_date_3", "pattern_id_3", "tieup_detail_genre_id_3", "tieup_name_3", "tieup_id_3", "johnnys_3", "album_music_id_2", "album_music_name_2", "album_artist_id_2", "album_artist_name_2", "album_release_date_2"),
        ("material_id_4", "material_name_4", "music_id_4", "music_name_4", 300, "artist_id_4", "artist_name_4", "release_date_4", "pattern_id_4", "tieup_detail_genre_id_4", "tieup_name_4", "tieup_id_4", "johnnys_4", "album_music_id_4", "album_music_name_4", "album_artist_id_4", "album_artist_name_4", "album_release_date_4"),
        ("material_id_5", "material_name_5", "music_id_5", "music_name_5", 200, "artist_id_5", "artist_name_5", "release_date_5", "pattern_id_5", "tieup_detail_genre_id_5", "tieup_name_5", "tieup_id_5", "johnnys_5", "album_music_id_2", "album_music_name_2", "album_artist_id_2", "album_artist_name_2", "album_release_date_2"),
        ("material_id_6", "material_name_6", "music_id_6", "music_name_6", 100, "artist_id_6", "artist_name_6", "release_date_6", "pattern_id_6", "tieup_detail_genre_id_6", "tieup_name_6", "tieup_id_6", "johnnys_6", "album_music_id_6", "album_music_name_6", "album_artist_id_6", "album_artist_name_6", "album_release_date_6"),
    ], test_schema)

    actual_df = etl_util.aggregate_musics_to_album(
        test_df,
        top_k=4,
        threshold_num=2,
        order_column="score",
        ascending=False,
        group_column="album_music_id",
    )
    expected_df = spark.createDataFrame([
        ("music_id_1", "music_name_1", "material_id_1", "material_name_1", "artist_id_1", "artist_name_1", "release_date_1", "pattern_id_1", "tieup_detail_genre_id_1", "tieup_name_1", "tieup_id_1", "johnnys_1", "music", 600),
        ("album_music_id_2", "album_music_name_2", None, None, "album_artist_id_2", "album_artist_name_2", "album_release_date_2", None, None, None, None, None, "album", 0),
        ("music_id_4", "music_name_4", "material_id_4", "material_name_4", "artist_id_4", "artist_name_4", "release_date_4", "pattern_id_4", "tieup_detail_genre_id_4", "tieup_name_4", "tieup_id_4", "johnnys_4", "music", 300),
    ], expected_schema)
    assert_df_equality(actual_df, expected_df, ignore_row_order=False)

    actual_df = etl_util.aggregate_musics_to_album(
        test_df,
        top_k=None,
        threshold_num=2,
        order_column="score",
        ascending=False,
        group_column="album_music_id",
    )
    expected_df = spark.createDataFrame([
        ("music_id_1", "music_name_1", "material_id_1", "material_name_1", "artist_id_1", "artist_name_1", "release_date_1", "pattern_id_1", "tieup_detail_genre_id_1", "tieup_name_1", "tieup_id_1", "johnnys_1", "music", 600),
        ("album_music_id_2", "album_music_name_2", None, None, "album_artist_id_2", "album_artist_name_2", "album_release_date_2", None, None, None, None, None, "album", 0),
        ("music_id_4", "music_name_4", "material_id_4", "material_name_4", "artist_id_4", "artist_name_4", "release_date_4", "pattern_id_4", "tieup_detail_genre_id_4", "tieup_name_4", "tieup_id_4", "johnnys_4", "music", 300),
        ("music_id_6", "music_name_6", "material_id_6", "material_name_6", "artist_id_6", "artist_name_6", "release_date_6", "pattern_id_6", "tieup_detail_genre_id_6", "tieup_name_6", "tieup_id_6", "johnnys_6", "music", 100),
    ], expected_schema)

    assert_df_equality(actual_df, expected_df, ignore_row_order=False)
