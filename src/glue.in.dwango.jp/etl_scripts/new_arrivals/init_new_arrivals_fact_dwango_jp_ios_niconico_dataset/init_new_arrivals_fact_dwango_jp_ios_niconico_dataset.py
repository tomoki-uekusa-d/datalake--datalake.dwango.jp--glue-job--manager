import sys, boto3, hashlib, re
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime, date, timedelta
from pytz import timezone

from etl_util import join_dim_material, explode_tracks, aggregate_musics_to_album, filter_duplicate_tieup_item


def get_today_month_string():
    d = datetime.now(timezone("Asia/Tokyo"))
    date_string = d.strftime("%Y-%m")
    return date_string

this_month = get_today_month_string()

## Assets
datacatalog_database = "datacatalog"
fact_purchase_database = "new_arrivals"
s3_bucket_name = "etl-datadomain-new-arrivals"
site = "dwango_jp_ios"
corner = "niconico"
target_month = this_month.replace('-', '')
s3_base_path = f"fact_new_arrivals/site={site}/corner={corner}/target_date={target_month}01"
s3_base_path_csv_filename = f"{target_month}01_{site}_{corner}.csv"

# @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

datasource31 = glueContext.create_dynamic_frame.from_catalog(
    database=f"{datacatalog_database}",
    table_name="dim_hub_item",
    transformation_ctx="datasource31",
)
dim_hub_item = (
    datasource31.toDF().filter(col("catalog_end_date").isNull())
    .withColumn(
        "delivery_start_date",
        from_utc_timestamp(col("delivery_start_date"), "Asia/Tokyo"),
    )
    .withColumn("delivery_end_date", from_utc_timestamp(col("delivery_end_date"), "Asia/Tokyo"))
    .filter(col("delivery_start_date").like(f"{this_month}%"))
)
datasource32 = glueContext.create_dynamic_frame.from_catalog(
    database=f"{datacatalog_database}",
    table_name="dim_hub_item_and_artist",
    transformation_ctx="datasource32",
)
dim_hub_item_and_artist = datasource32.toDF().filter(col("catalog_end_date").isNull()).select(col("item_id"), col("artist_id"))
datasource33 = glueContext.create_dynamic_frame.from_catalog(
    database=f"{datacatalog_database}",
    table_name="dim_hub_music",
    transformation_ctx="datasource33",
)
dim_hub_music = (
    datasource33.toDF().filter(col("catalog_end_date").isNull()).select(col("id"), col("name").alias("music_name"), col("tieup").alias("music_tieup"))
)
datasource34 = glueContext.create_dynamic_frame.from_catalog(
    database=f"{datacatalog_database}",
    table_name="dim_hub_artist",
    transformation_ctx="datasource34",
)
dim_hub_artist = datasource34.toDF().filter(col("catalog_end_date").isNull()).select(col("id"), col("name").alias("artist_name"))
datasource35 = glueContext.create_dynamic_frame.from_catalog(
    database=f"{datacatalog_database}",
    table_name="dim_hub_music_and_item",
    transformation_ctx="datasource35",
)
dim_hub_music_and_item = datasource35.toDF().filter(col("catalog_end_date").isNull()).select(col("item_id"), col("music_id"))
datasource36 = glueContext.create_dynamic_frame.from_catalog(
    database = f"{datacatalog_database}",
    table_name = "dim_hub_item_and_site_contract",
    transformation_ctx = "datasource36"
)
dim_hub_item_and_site_contract = datasource36.toDF().filter((col('catalog_end_date').isNull())).select(col("site_name"), col("item_id"))
datasource37 = glueContext.create_dynamic_frame.from_catalog(
    database = f"{datacatalog_database}",
    table_name = "dim_hub_item_and_ftdt",
    transformation_ctx = "datasource37"
)
dim_hub_item_and_ftdt = datasource37.toDF().filter(col('catalog_end_date').isNull()).select(col("item_id"), col("filetype_id"))

df_dim_material_all = join_dim_material(
    dim_hub_item,
    dim_hub_item_and_artist,
    dim_hub_item_and_ftdt,
    dim_hub_music_and_item,
    dim_hub_item_and_site_contract,
    dim_hub_music,
    dim_hub_artist,
)

# ファイルタイプIDについては以下を確認
# https://paper.dropbox.com/doc/--BdnYYRzQgQgGHX6inFPKEfHkAQ-jIilKzv0c0lANGtyprzAI
site_name = "dwango.jp(iPhone)"
list_filetype_id = [
    "281300", "281400", "070040", "070041", "070060", "070061", "070140", "070141", "070240", "070340", "070440", "070540",
    "070640", "070740", "070840", "070940", "071040", "071140", "071240", "071340", "071400", "071540", "071600", "071700",
]
df_dim_material_target = df_dim_material_all.filter(
    (col("site_name") == site_name) &
    (col("filetype_id").isin(list_filetype_id))
)

datasource2 = glueContext.create_dynamic_frame.from_catalog(
    database=f"{datacatalog_database}",
    table_name="dim_id_provider_zocalo_item_provider",
    transformation_ctx="datasource2",
)
dim_id_provider_zocalo_item_provider = (
    datasource2.toDF()
    .filter(col("catalog_end_date").isNull())
    .select(
        col("dam2018_collection_id").alias("collection_id"),
        col("dam2018_asset_id").alias("asset_id"),
        col("zocalo_item_id").alias("material_id"),
    )
    .distinct()
)
datasource5 = glueContext.create_dynamic_frame.from_catalog(
    database=f"{datacatalog_database}",
    table_name="dim_hub_music_and_music_genre",
    transformation_ctx="datasource5",
)
df_dim_music_and_music_genre = datasource5.toDF().filter(col("catalog_end_date").isNull())
datasource6 = glueContext.create_dynamic_frame.from_catalog(
    database=f"{fact_purchase_database}", table_name="johnnys_csv", transformation_ctx="datasource6"
)
df_johnnys = datasource6.toDF()
datasource7 = glueContext.create_dynamic_frame.from_catalog(
    database=f"{datacatalog_database}",
    table_name="dim_hub_music_and_tieup",
    transformation_ctx="datasource7",
)
df_dim_music_and_tieup = datasource7.toDF().filter(col("catalog_end_date").isNull())
datasource8 = glueContext.create_dynamic_frame.from_catalog(
    database=f"{datacatalog_database}",
    table_name="dim_hub_tieup",
    transformation_ctx="datasource8",
)
df_dim_tieup = datasource8.toDF().filter(col("catalog_end_date").isNull())
datasource10 = glueContext.create_dynamic_frame.from_catalog(
    database=f"{datacatalog_database}",
    table_name=f"dim_collection_detail",
    transformation_ctx="datasource10",
)
dim_collection_detail_albums = datasource10.toDF().filter(
    (col('release_type') == 'Album') &
    (col('title_titletype') == 'AlbumName') &
    (col('title_titlelanguage') == 'ja-Hani') &
    (col('artist_names_language') == 'ja-Hani') &
    (col('catalog_end_date').isNull())
)
datasource11 = glueContext.create_dynamic_frame.from_catalog(
    database=f"{datacatalog_database}",
    table_name=f"dim_collection_collection_detail",
    transformation_ctx="datasource11",
)
dim_collection_collection_detail = datasource11.toDF()

dim_albums = dim_collection_detail_albums.join(
    dim_collection_collection_detail,
    dim_collection_collection_detail["collection_detail_id"] == dim_collection_detail_albums["id"],
    "left"
).join(
    dim_id_provider_zocalo_item_provider,
    dim_id_provider_zocalo_item_provider["collection_id"] == dim_collection_collection_detail["collection_id"],
    "left"
).join(
    df_dim_material_all,
    dim_id_provider_zocalo_item_provider["material_id"] == df_dim_material_all["id"],
    "left"
).select(
    dim_collection_detail_albums["id"].alias("album_music_id"),
    dim_collection_detail_albums["title_title"].alias("album_music_name"),
    df_dim_material_all["artist_id"].alias("album_artist_id"),
    dim_collection_detail_albums["artist_names_text"].alias("album_artist_name"),
    df_dim_material_all["delivery_start_date"].cast("int").alias("album_release_date"),
    dim_collection_detail_albums["asset_structure"]
)
df_album_tracks = explode_tracks(dim_albums)

df_new_arrivals_origin = (
    df_dim_material_target
    .join(
        dim_id_provider_zocalo_item_provider,
        dim_id_provider_zocalo_item_provider["material_id"] == df_dim_material_target["id"],
        "left",
    )
    .join(
        df_dim_music_and_tieup,
        df_dim_material_target["music_id"] == df_dim_music_and_tieup["music_id"],
        "left",
    )
    .join(df_dim_tieup, df_dim_music_and_tieup["tieup_id"] == df_dim_tieup["id"], "left")
    .join(
        df_dim_music_and_music_genre,
        df_dim_material_target["id"] == df_dim_music_and_music_genre["music_id"],
        "left",
    )
    .join(df_johnnys, df_dim_material_target["artist_id"] == df_johnnys["artist_id"], "left")
    .join(
        df_album_tracks,
        dim_id_provider_zocalo_item_provider["asset_id"] == df_album_tracks["album_track_asset_id"],
        "left",
    )
)

df_new_arrivals = (
    df_new_arrivals_origin.filter(df_dim_tieup["searchable"] != 0)
    .select(
        lit(1).alias("pickup"),
        df_dim_material_target["id"].alias("material_id"),
        df_dim_material_target["name"].alias("material_name"),
        df_dim_material_target["music_id"],
        df_dim_material_target["music_name"],
        df_dim_material_target["artist_id"],
        df_dim_material_target["artist_name"],
        df_dim_material_target["delivery_start_date"].cast("int").alias("release_date"),
        when(df_dim_music_and_tieup["pattern_id"].isNull(), 0).otherwise(df_dim_music_and_tieup["pattern_id"]).alias("pattern_id"),
        when(df_dim_tieup["detailgenre_id"].isNull(), 0).otherwise(df_dim_tieup["detailgenre_id"]).alias("tieup_detail_genre_id"),
        when(df_dim_tieup["name"].isNull(), "null").otherwise(df_dim_tieup["name"]).cast("string").alias("tieup_name"),
        when(df_dim_tieup["id"].isNull(), 0).otherwise(df_dim_tieup["id"]).cast("int").alias("tieup_id"),
        when(df_johnnys["is_johnnys"].isNull(), 0).otherwise(1).alias("johnnys"),
        lit(0).alias("score"),
        df_album_tracks["album_music_id"],
        df_album_tracks["album_music_name"],
        df_album_tracks["album_artist_id"],
        df_album_tracks["album_artist_name"],
        df_album_tracks["album_release_date"],
    )
    .distinct()
    .orderBy(col("release_date").desc())
)

df_new_arrivals_aggregated = aggregate_musics_to_album(
    df_new_arrivals,
    top_k=None,
    threshold_num=5,
    order_column="release_date",
    ascending=False,
    group_column="album_music_id",
)

df_new_arrivals_filtered_duplicated_tieup = filter_duplicate_tieup_item(
    df_new_arrivals_aggregated,
    order_column="release_date",
    ascending=False,
)

output_path = f"s3://{s3_bucket_name}/{s3_base_path}/"
df_new_arrivals_filtered_duplicated_tieup.coalesce(1).write.mode("overwrite").csv(output_path, header=True)

# NOTE : Rename filename
URI = sc._gateway.jvm.java.net.URI
Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.s3.S3FileSystem
fs = FileSystem.get(URI(f"s3://{s3_bucket_name}"), glueContext._jsc.hadoopConfiguration())

s3_resource = boto3.resource('s3')
s3_object_list = s3_resource.Bucket(s3_bucket_name).objects.filter(Prefix=s3_base_path)
s3_object_names = [item.key for item in s3_object_list]
fs.rename(
    Path(f"s3://{s3_bucket_name}/{s3_object_names[0]}"),
    Path(f"s3://{s3_bucket_name}/{s3_base_path}/{s3_base_path_csv_filename}")
)

job.commit()
