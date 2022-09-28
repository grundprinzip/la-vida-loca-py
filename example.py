from pyspark.sql.connect.client import RemoteSparkSession
from pyspark.sql.connect.pipeline import Pipeline, stages
from pyspark.sql.connect.functions import col


SAMPLE_CLUSTER="0915-191458-1q549rq3"
TOKEN="TOKEN_HERE"

spark = RemoteSparkSession(host="e2-dogfood.staging.cloud.databricks.com", port=443, cluster_id=SAMPLE_CLUSTER, token=TOKEN)

df = spark.read.table("hive_metastore.magrund.berlin_houses")
print(df.limit(10).collect())


# Configure the pipeline.
tokenizer = stages.Tokenizer()
tokenizer.inputCol = "text"
tokenizer.outputCol = "words"

hashingTF = stages.HashingTF()
hashingTF.inputCol = "words"
hashingTF.outputCol = "features"

lr = stages.LogisticRegression()
lr.maxIter = 10
lr.regParam = 0.001

# Create the pipeline.
p = Pipeline(spark, [tokenizer, hashingTF, lr])

# Fit the pipeline.
training = spark.read.table("hive_metastore.magrund.training")
model = p.fit(training)

print(f"Try loading this model from Ruby: {model.name}")

# Do the scoring
scoring = spark.read.table("hive_metastore.magrund.mltest")
df = model.transform(scoring)


# DF Name
for tpl in df.select(col("id"), col("text"), col("prediction"), col("probability")).collect().iterrows():
    row = tpl[1]
    rid, text, prob, prediction = row["id"], row["text"], row["probability"], row["prediction"]
    print(
        "(%d, %s) --> prob=%s, prediction=%f" % (
            rid, text, str(prob), prediction  # type: ignore
        )
    )