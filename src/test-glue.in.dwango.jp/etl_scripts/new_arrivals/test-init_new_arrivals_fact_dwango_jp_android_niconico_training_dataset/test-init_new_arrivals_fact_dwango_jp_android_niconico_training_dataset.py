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

## Assets
datacatalog_database = "test_datacatalog"
fact_purchase_database = "test_new_arrivals"
fact_purchase_table = "fact_purchase_dwango_jp_android_niconico"
s3_bucket_name = "etl-datadomain-test-new-arrivals"
s3_base_path = "fact_new_arrivals_dwango_jp_android_niconico_training_dataset"
s3_base_path_csv = "fact_new_arrivals_dwango_jp_android_niconico_training_dataset"

# @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


def get_n_days_ago(datetime1, n):
    d = datetime.strptime(datetime1, "%Y-%m-%d")
    d -= timedelta(days=n)
    date_string = d.strftime("%Y-%m-%d")
    return date_string


def get_today_date_string():
    d = datetime.now(timezone("Asia/Tokyo"))
    date_string = d.strftime("%Y-%m-%d")
    return date_string


today = get_today_date_string()
target = get_n_days_ago(today, 365)
# target = get_n_days_ago(today,90)


def get_elapsed_days(day):
    d = int(datetime.strptime(today, "%Y-%m-%d").timestamp())
    diff = d - day
    l = [90, int((diff + 86399) / 86400)]
    l.sort()
    return l[0]


udf_get_elapsed_days = udf(get_elapsed_days, IntegerType())


# dim_material
# dim_hub_item/dim_hub_item_and_artist/dim_hub_music/dim_hub_artistで代用
# datasource3 = glueContext.create_dynamic_frame.from_catalog(database = f"{datacatalog_database}", table_name = "dim_material", transformation_ctx = "datasource3")
datasource31 = glueContext.create_dynamic_frame.from_catalog(
    database=f"{datacatalog_database}",
    table_name="dim_hub_item",
    transformation_ctx="datasource31",
)
datasource32 = glueContext.create_dynamic_frame.from_catalog(
    database=f"{datacatalog_database}",
    table_name="dim_hub_item_and_artist",
    transformation_ctx="datasource32",
)
datasource33 = glueContext.create_dynamic_frame.from_catalog(
    database=f"{datacatalog_database}",
    table_name="dim_hub_music",
    transformation_ctx="datasource33",
)
datasource34 = glueContext.create_dynamic_frame.from_catalog(
    database=f"{datacatalog_database}",
    table_name="dim_hub_artist",
    transformation_ctx="datasource34",
)
datasource35 = glueContext.create_dynamic_frame.from_catalog(
    database=f"{datacatalog_database}",
    table_name="dim_hub_music_and_item",
    transformation_ctx="datasource35",
)
# df_dim_material = datasource3.toDF()

# NOTE: 配信開始日が新着となる対象かどうかを判定して取ってくる
dim_hub_item = (
    datasource31.toDF()
    .filter(col("catalog_end_date").isNull())
    .withColumn(
        "delivery_start_date",
        from_utc_timestamp(col("delivery_start_date"), "Asia/Tokyo"),
    )
    .withColumn(
        "delivery_end_date", from_utc_timestamp(col("delivery_end_date"), "Asia/Tokyo")
    )
    .filter(col("delivery_start_date") < target)
)

# NOTE: アイテムIDに対応するとアーティストIDを取る
dim_hub_item_and_artist = (
    datasource32.toDF()
    .filter(col("catalog_end_date").isNull())
    .select(col("item_id"), col("artist_id"))
)

# NOTE: 楽曲情報色々をとる
dim_hub_music = (
    datasource33.toDF()
    .filter(col("catalog_end_date").isNull())
    .select(
        col("id"), col("name").alias("music_name"), col("tieup").alias("music_tieup")
    )
)

# NOTE: アーティスト名を取る
dim_hub_artist = (
    datasource34.toDF()
    .filter(col("catalog_end_date").isNull())
    .select(col("id"), col("name").alias("artist_name"))
)

# NOTE: itemidと楽曲idを対応させる
dim_hub_music_and_item = (
    datasource35.toDF()
    .filter(col("catalog_end_date").isNull())
    .select(col("item_id"), col("music_id"))
)

# NOTE : 諸々ジョインして整形する
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
    .join(
        dim_hub_music, dim_hub_music_and_item["music_id"] == dim_hub_music["id"], "left"
    )
    .drop(dim_hub_music.id)
    .join(
        dim_hub_artist,
        dim_hub_item_and_artist["artist_id"] == dim_hub_artist["id"],
        "left",
    )
    .drop(dim_hub_artist.id)
)

# NOTE: 数をチェック
print("df_dim_material:" + str(df_dim_material.count()))


# TODO: ここよくわからないので調査
# NOTE: zocaloとは？
# dim_id_provider_zocalo_item_provider
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
        col("zocalo_item_id").alias("material_id"),
    )
    .distinct()
)

# NOTE: ジャンル情報
# dim_music_and_music_genre
datasource5 = glueContext.create_dynamic_frame.from_catalog(
    database=f"{datacatalog_database}",
    table_name="dim_hub_music_and_music_genre",
    transformation_ctx="datasource5",
)
df_dim_music_and_music_genre = datasource5.toDF().filter(
    col("catalog_end_date").isNull()
)

# NOTE: ジャニーズの情報が入ってそう
# df_johnnys
datasource6 = glueContext.create_dynamic_frame.from_catalog(
    database=f"{fact_purchase_database}", table_name="johnnys_csv", transformation_ctx="datasource6"
)
df_johnnys = datasource6.toDF()

# NOTE: タイアップとの紐付け情報が入ってる
# dim_music_and_tieup
datasource7 = glueContext.create_dynamic_frame.from_catalog(
    database=f"{datacatalog_database}",
    table_name="dim_hub_music_and_tieup",
    transformation_ctx="datasource7",
)
df_dim_music_and_tieup = datasource7.toDF().filter(col("catalog_end_date").isNull())

# NOTE: タイアップ情報自体の情報が入ってる
# dim_tieup
datasource8 = glueContext.create_dynamic_frame.from_catalog(
    database=f"{datacatalog_database}",
    table_name="dim_hub_tieup",
    transformation_ctx="datasource8",
)
df_dim_tieup = datasource8.toDF().filter(col("catalog_end_date").isNull())

# NOTE: 購入情報
# fact_purchase
datasource9 = glueContext.create_dynamic_frame.from_catalog(
    database="new_arrivals", # test側に無いのでここだけ本番と一緒
    table_name=f"{fact_purchase_table}",
    transformation_ctx="datasource0",
)
df_fact_purchase = datasource9.toDF()

# fact
## NOTE: dim_materialから色々作る
df_new_arrivals = (
    df_dim_material
    ## NOTE: あとから取った情報を色々とJOIN
    .join(
        dim_id_provider_zocalo_item_provider,
        dim_id_provider_zocalo_item_provider["material_id"] == df_dim_material["id"],
        "left",
    )
    .join(
        df_dim_music_and_tieup,
        df_dim_material["music_id"] == df_dim_music_and_tieup["music_id"],
        "left",
    )
    .join(
        df_dim_tieup, df_dim_music_and_tieup["tieup_id"] == df_dim_tieup["id"], "left"
    )
    .join(
        df_dim_music_and_music_genre,
        df_dim_material["id"] == df_dim_music_and_music_genre["music_id"],
        "left",
    )
    .join(df_johnnys, df_dim_material["artist_id"] == df_johnnys["artist_id"], "left")
    .join(
        df_fact_purchase,
        df_dim_material["music_id"] == df_fact_purchase["product_id"],
        "left",
    )
    ## NOTE: scheduleが0のものはいらない
    .filter(df_dim_tieup["searchable"] != 0)
    ## NOTE: 必要なものを変換しつつselect
    .select(
        # df_dim_material["id"].alias("material_id"),
        # df_dim_material["name"].alias("material_name"),
        ## NOTE: このカウント数はいい感じに配信日とかで諸々考慮
        when(
            (df_fact_purchase["count"].isNull())
            | (
                udf_get_elapsed_days(df_dim_material["delivery_start_date"].cast("int"))
                == 0
            ),
            0,
        )
        .otherwise(
            lit(
                df_fact_purchase["count"]
                / udf_get_elapsed_days(
                    df_dim_material["delivery_start_date"].cast("int")
                )
            )
        )
        .alias("average_count"),
        df_dim_material["music_id"],
        df_dim_material["music_name"],
        when(df_fact_purchase["count"].isNull(), 0)
            .otherwise(df_fact_purchase["count"])
            .alias("count"),
        lit(
            udf_get_elapsed_days(df_dim_material["delivery_start_date"].cast("int"))
        ).alias("elapsed_days"),
        df_dim_material["artist_id"],
        df_dim_material["artist_name"],
        df_dim_material["delivery_start_date"].cast("int").alias("release_date"),
        when(df_dim_music_and_tieup["pattern_id"].isNull(), 0)
            .otherwise(df_dim_music_and_tieup["pattern_id"])
            .alias("pattern_id"),
        when(df_dim_tieup["detailgenre_id"].isNull(), 0)
            .otherwise(df_dim_tieup["detailgenre_id"])
            .alias("tieup_detail_genre_id"),
        when(df_dim_tieup["name"].isNull(), "null")
            .otherwise(df_dim_tieup["name"])
            .cast("string")
            .alias("tieup_name"),
        when(df_dim_tieup["id"].isNull(), 0)
            .otherwise(df_dim_tieup["id"])
            .cast("int")
            .alias("tieup_id"),
        # ジャニーズ判定
        when(df_johnnys["is_johnnys"].isNull(), 0).otherwise(1).alias("johnnys"),
    )
    ## NOTE: 重複を削除
    .distinct()
    ## NOTE: 購入カウントが無いものは除去
    .filter(col("count") != 0)
    ## NOTE: 購入数順にソート
    .orderBy(col("count").desc())
)

codec = "snappy"
output_path = f"s3://{s3_bucket_name}/{s3_base_path}/"
df_new_arrivals.write.mode("overwrite").parquet(output_path, compression=codec)

output_path = f"s3://{s3_bucket_name}/{s3_base_path_csv}/"
df_new_arrivals.coalesce(1).write.mode("overwrite").csv(output_path, header=False)
job.commit()
