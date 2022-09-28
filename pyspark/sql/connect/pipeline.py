from pyspark.sql.connect.client import RemoteSparkSession
from pyspark.sql.connect.data_frame import DataFrame
from pyspark.sql.connect.column import LiteralExpression
from pyspark.sql.connect.plan import ServerSide

import pyspark.sql.connect.proto as pb2

from typing import List


class Stage(object):
    """Shallow class representing a stage."""

    def __init__(self, name) -> None:
        self.name = name
        self.params = {}

    def __setattr__(self, name, value):
        if name in ["name", "params"]:
            object.__setattr__(self, name, value)
        else:
            self.params[name] = value

    def __getattr__(self, item):
        return self.params[item]

    def to_proto(self):
        stage = pb2.Stage()
        stage.name = self.name
        for k in self.params:
            stage.parameters[k].CopyFrom(LiteralExpression(self.params[k]).to_plan(None).literal)
        return stage


class StageBuilder:
    """Helper class to create Stages by name as syntactic sugar."""

    def __getattr__(self, item):
        def _():
            return Stage(item)

        _.__doc__ = f"Create stage {item}"
        return _


stages = StageBuilder()


class PipelineModel:
    def __init__(self, client: RemoteSparkSession, name: str) -> None:
        self.client = client
        self.name = name

    def transform(self, df) -> DataFrame:
        req = pb2.Request()
        req.user_context.user_id = self.client._user_id
        req.plan.pipeline_model.name = self.name
        req.plan.pipeline_model.input.CopyFrom(df._plan.plan(self.client))
        uid = self.client._execute_and_fetch_ml(req)
        return DataFrame.withPlan(ServerSide(uid), self.client)


class Pipeline:
    def __init__(self, client: RemoteSparkSession, s: List[Stage]):
        self.client = client
        self.stages = s

    def fit(self, df: DataFrame) -> "PipelineModel":
        req = pb2.Request()
        req.user_context.user_id = self.client._user_id

        plan = pb2.Plan()
        for s in self.stages:
            plan.pipeline.stages.append(s.to_proto())

        plan.pipeline.input.CopyFrom(df._plan.plan(self.client))

        req.plan.CopyFrom(plan)
        uid = self.client._execute_and_fetch_ml(req)
        return PipelineModel(self.client, uid)
