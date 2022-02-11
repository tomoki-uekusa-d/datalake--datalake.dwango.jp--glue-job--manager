import pyspark.sql.functions as F


def hogehogehoge(word):
    if word == "HOGEHOGE":
        return True
    return False

def convert_dataframe(df):
    df.printSchema()
    return df