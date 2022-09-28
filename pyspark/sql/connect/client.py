#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


import io
import logging
import os
import typing
import uuid

import grpc
import pandas
import pandas as pd
import pyarrow as pa

import pyspark.sql.connect.proto as pb2
import pyspark.sql.connect.proto.base_pb2_grpc as grpc_lib
from pyspark import cloudpickle
from pyspark.sql.connect.data_frame import DataFrame
from pyspark.sql.connect.readwriter import DataFrameReader
from pyspark.sql.connect.plan import Sql


NumericType = typing.Union[int, float]

logging.basicConfig(level=logging.INFO)


class MetricValue:
    def __init__(self, name: str, value: NumericType, type: str):
        self._name = name
        self._type = type
        self._value = value

    def __repr__(self) -> str:
        return f"<{self._name}={self._value} ({self._type})>"

    @property
    def name(self) -> str:
        return self._name

    @property
    def value(self) -> NumericType:
        return self._value

    @property
    def metric_type(self) -> str:
        return self._type


class PlanMetrics:
    def __init__(self, name: str, id: str, parent: str, metrics: typing.List[MetricValue]):
        self._name = name
        self._id = id
        self._parent_id = parent
        self._metrics = metrics

    def __repr__(self) -> str:
        return f"Plan({self._name})={self._metrics}"

    @property
    def name(self) -> str:
        return self._name

    @property
    def plan_id(self) -> str:
        return self._id

    @property
    def parent_plan_id(self) -> str:
        return self._parent_id

    @property
    def metrics(self) -> typing.List[MetricValue]:
        return self._metrics


class AnalyzeResult:
    def __init__(self, cols: typing.List[str], types: typing.List[str], explain: str):
        self.cols = cols
        self.types = types
        self.explain_string = explain

    @classmethod
    def fromProto(cls, pb: typing.Any) -> "AnalyzeResult":
        return AnalyzeResult(pb.column_names, pb.column_types, pb.explain_string)


class RemoteSparkSession(object):
    """Conceptually the remote spark session that communicates with the server"""

    def __init__(
        self,
        host: str = None,
        port: int = 15001,
        user_id: str = "Martin",
        cluster_id=None,
        token=None,
    ):
        self._host = "localhost" if host is None else host
        self._port = port
        self._user_id = user_id
        self.token = os.getenv("DATABRICKS_TOKEN", token)
        self.cluster_id = cluster_id

        if self._host == "localhost":
            self._channel = grpc.insecure_channel(f"{self._host}:{self._port}")
        else:
            ssl_creds = grpc.ssl_channel_credentials()
            token_creds = grpc.access_token_call_credentials(self.token)
            comp_creds = grpc.composite_channel_credentials(ssl_creds, token_creds)
            self._channel = grpc.secure_channel(f"{self._host}:{self._port}", comp_creds)

        self._stub = grpc_lib.SparkConnectServiceStub(self._channel)

        # Create the reader
        self.read = DataFrameReader(self)
        # self.ml = MLManager(self)

    def register_udf(self, function, return_type) -> str:
        """Create a temporary UDF in the session catalog on the other side. We generate a
        temporary name for it."""
        name = f"fun_{uuid.uuid4().hex}"
        fun = pb2.CreateScalarFunction()
        fun.parts.append(name)
        fun.serialized_function = cloudpickle.dumps((function, return_type))

        req = pb2.Request()
        req.user_context.user_id = self._user_id
        req.plan.command.create_function.CopyFrom(fun)

        self._execute_and_fetch(req)
        return name

    def _metadata(self):
        return (
            ('x-databricks-cluster-id', self.cluster_id),
        )

    def _build_metrics(self, metrics: "pb2.Response.Metrics") -> typing.List[PlanMetrics]:
        return [
            PlanMetrics(
                x.name,
                x.plan_id,
                x.parent,
                [MetricValue(k, v.value, v.metric_type) for k, v in x.execution_metrics.items()],
            )
            for x in metrics.metrics
        ]

    def sql(self, sql_string: str) -> "DataFrame":
        return DataFrame.withPlan(Sql(sql_string), self)

    def collect(self, plan: pb2.Plan) -> pandas.DataFrame:
        req = pb2.Request()
        req.user_context.user_id = self._user_id
        req.plan.CopyFrom(plan)
        return self._execute_and_fetch(req)

    def analyze(self, plan: pb2.Plan) -> AnalyzeResult:
        req = pb2.Request()
        req.user_context.user_id = self._user_id
        req.plan.CopyFrom(plan)

        resp = self._stub.AnalyzePlan(req, metadata=self._metadata())
        return AnalyzeResult.fromProto(resp)

    def _process_batch(self, b) -> pandas.DataFrame:
        if b.batch is not None and len(b.batch.data) > 0:
            with pa.ipc.open_stream(b.data) as rd:
                return rd.read_pandas()
        elif b.csv_batch is not None and len(b.csv_batch.data) > 0:
            return pd.read_csv(io.StringIO(b.csv_batch.data), delimiter="|")

    def _execute_and_fetch_ml(self, req: pb2.Request):

        for b in self._stub.ExecutePlan(req, metadata=self._metadata()):
            if b.server_side:
                return b.server_side.uid

    def _execute_and_fetch(self, req: pb2.Request) -> typing.Optional[pandas.DataFrame]:
        m = None
        result_dfs = []

        for b in self._stub.ExecutePlan(req, metadata=self._metadata()):
            if b.metrics is not None:
                m = b.metrics
            result_dfs.append(self._process_batch(b))

        if len(result_dfs) > 0:
            df = pd.concat(result_dfs)
            # Attach the metrics to the DataFrame attributes.
            df.attrs["metrics"] = self._build_metrics(m)
            return df
        else:
            return None
