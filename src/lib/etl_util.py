from struct import Struct
from tokenize import String
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, MapType
from pyspark.sql.window import Window


def join_dim_material(dim_hub_item, dim_hub_item_and_artist, dim_hub_music_and_item, dim_hub_music, dim_hub_artist):
    df_dim_material = (
        dim_hub_item.join(
            dim_hub_item_and_artist,
            dim_hub_item["id"] == dim_hub_item_and_artist["item_id"],
            "left",
        )
        .drop(dim_hub_item_and_artist.item_id)
        .join(
            dim_hub_music_and_item,
            dim_hub_item["id"] == dim_hub_music_and_item["item_id"],
            "left",
        )
        .drop(dim_hub_music_and_item.item_id)
        .join(dim_hub_music, dim_hub_music_and_item["music_id"] == dim_hub_music["id"], "left")
        .drop(dim_hub_music.id)
        .join(
            dim_hub_artist,
            dim_hub_item_and_artist["artist_id"] == dim_hub_artist["id"],
            "left",
        )
        .drop(dim_hub_artist.id)
    )
    print("df_dim_material:" + str(df_dim_material.count()))
    return df_dim_material


def explode_tracks(df, structure_column="asset_structure"):
    asset_structure_json_schema = StructType([
        StructField("Items", ArrayType(
                StructType([
                    StructField("AssetId", IntegerType(), True),
                    StructField("ResourceType", StringType(), True),
                    StructField("SequenceNumber", IntegerType(), True),
                ])
        ), True),
        StructField("Groups", ArrayType(
            StructType([
                StructField("Items", ArrayType(
                    StructType([
                        StructField("AssetId", IntegerType(), True),
                        StructField("ResourceType", StringType(), True),
                        StructField("SequenceNumber", IntegerType(), True),
                    ])
                ), True),
                StructField("Groups", ArrayType(
                    StructType([
                        StructField("AssetId", IntegerType(), True),
                        StructField("ResourceType", StringType(), True),
                        StructField("SequenceNumber", IntegerType(), True),
                    ])
                ), True),
                StructField("Titles", ArrayType(
                    StructType([
                        StructField("Title", StringType(), True),
                        StructField("SubTitle", StringType(), True),
                        StructField("TitleType", StringType(), True),
                        StructField("SubTitleType", StringType(), True),
                        StructField("TitleLanguage", StringType(), True),
                        StructField("SubTitleLanguage", StringType(), True),
                    ])
                ), True),
                StructField("SequenceNumber", IntegerType(), True),
            ]),
        ), True),
    ])

    _df = df.withColumn("asset_structure_map", F.from_json(F.col(f"{structure_column}"), asset_structure_json_schema)).select(
        F.col("*"),
        F.explode_outer(F.col("asset_structure_map.Groups.Items").getItem(0).AssetId).alias("album_track_asset_id"),
    ).drop(structure_column, "asset_structure_map")
    return _df


def aggregate_musics_to_album(df, top_k=100, threshold_num=5, order_column="count", group_column="album_material_id"):
    df_with_rank = df.withColumn("rank", F.row_number().over(Window.orderBy(F.col(order_column).desc())))

    # df_with_rank.show(truncate=False) # NOTE: DEBUG

    df_target = df_with_rank.filter((F.col("rank") <= top_k))
    df_filtered_top_k_album_count = df_target.select(group_column).groupBy(F.col(group_column)).agg(F.count(group_column).alias("count_album")).filter(
        (F.col("count_album") >= threshold_num)
    ).withColumn("is_album_aggregation", F.lit(True)).select(
        group_column, "is_album_aggregation"
    )
    df_join_album_info = df_with_rank.join(
        df_filtered_top_k_album_count,
        df[group_column] == df_filtered_top_k_album_count[group_column],
        "left",
    ).drop(df_filtered_top_k_album_count[group_column])

    # df_join_album_info.show(truncate=False) # NOTE: DEBUG

    # NOTE: Separate with ranking
    # df_others = df_with_rank.filter((F.col("rank") > top_k)).withColumn(
    #     "is_album_aggregation", F.lit(None)
    # )
    # df_others.show(truncate=False) # NOTE: DEBUG

    df_target_selected = df_join_album_info.select(
        F.when(F.col("is_album_aggregation"), F.col("album_material_id")).otherwise(F.col("material_id")).alias("material_id"),
        F.when(F.col("is_album_aggregation"), F.col("album_material_name")).otherwise(F.col("material_name")).alias("material_name"),
        F.when(F.col("is_album_aggregation"), F.col("album_music_id")).otherwise(F.col("music_id")).alias("music_id"),
        F.when(F.col("is_album_aggregation"), F.col("album_music_name")).otherwise(F.col("music_name")).alias("music_name"),
        F.when(F.col("is_album_aggregation"), F.lit(None)).otherwise(F.col("count")).alias("count"),
        F.when(F.col("is_album_aggregation"), F.col("album_artist_id")).otherwise(F.col("artist_id")).alias("artist_id"),
        F.when(F.col("is_album_aggregation"), F.col("album_artist_name")).otherwise(F.col("artist_name")).alias("artist_name"),
        F.when(F.col("is_album_aggregation"), F.col("album_release_date")).otherwise(F.col("release_date")).alias("release_date"),
        F.when(F.col("is_album_aggregation"), F.lit(None)).otherwise(F.col("pattern_id")).alias("pattern_id"),
        F.when(F.col("is_album_aggregation"), F.lit(None)).otherwise(F.col("tieup_detail_genre_id")).alias("tieup_detail_genre_id"),
        F.when(F.col("is_album_aggregation"), F.lit(None)).otherwise(F.col("tieup_name")).alias("tieup_name"),
        F.when(F.col("is_album_aggregation"), F.lit(None)).otherwise(F.col("tieup_id")).alias("tieup_id"),
        F.when(F.col("is_album_aggregation"), F.lit(None)).otherwise(F.col("johnnys")).alias("johnnys"),
        F.when(F.col("is_album_aggregation"), F.lit("album")).otherwise(F.lit("music")).alias("transition_type"),
    ).distinct().sort(F.col("transition_type"), F.col(order_column).desc())

    # NOTE: Separate with ranking
    # df_others_selected = df_others.select(
    #     F.col("material_id"),
    #     F.col("material_name"),
    #     F.col("music_id"),
    #     F.col("music_name"),
    #     F.col("count"),
    #     F.col("artist_id"),
    #     F.col("artist_name"),
    #     F.col("release_date"),
    #     F.col("pattern_id"),
    #     F.col("tieup_detail_genre_id"),
    #     F.col("tieup_name"),
    #     F.col("tieup_id"),
    #     F.col("johnnys"),
    #     F.lit("music").alias("transition_type"),
    # )
    # df_unioned = df_target_selected.union(df_others_selected)
    # df_target_selected.show(truncate=False) # NOTE: DEBUG
    # return df_unioned

    return df_target_selected

