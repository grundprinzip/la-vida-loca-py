from pyspark.sql.connect.client import RemoteSparkSession


class SparkSession:
    def __init__(self) -> None:
        self._host = "e2-dogfood.staging.cloud.databricks.com"
        self._path = "/driver-proxy-api/o/6051921418418893/0619-200503-k3c3xyk2/15001"
        self._token = None

    @classmethod
    def builder(clz) -> "SparkSession":
        return SparkSession()

    def appName(self, name: str) -> "SparkSession":
        self._name = name
        return self

    def config(self, key: str, value: str) -> "SparkSession":
        if key == "spark.connect.host":
            self._host = value
        if key == "spark.connect.path":
            self._path = value
        if key == "spark.connect.token":
            self._token = value
        return self

    def getOrCreate(self) -> "RemoteSparkSession":
        return RemoteSparkSession(
            http_path=f"https://{self._host}{self._path}", token=f"{self._token}"
        )
