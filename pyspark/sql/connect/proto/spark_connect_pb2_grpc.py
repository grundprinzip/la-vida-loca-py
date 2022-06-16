# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

import pyspark.sql.connect.proto.spark_connect_pb2 as spark__connect__pb2

class SparkConnectServiceStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.ExecutePlan = channel.unary_stream(
                '/SparkConnectService/ExecutePlan',
                request_serializer=spark__connect__pb2.Request.SerializeToString,
                response_deserializer=spark__connect__pb2.Response.FromString,
                )
        self.AnalyzePlan = channel.unary_unary(
                '/SparkConnectService/AnalyzePlan',
                request_serializer=spark__connect__pb2.Request.SerializeToString,
                response_deserializer=spark__connect__pb2.AnalyzeResponse.FromString,
                )


class SparkConnectServiceServicer(object):
    """Missing associated documentation comment in .proto file."""

    def ExecutePlan(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def AnalyzePlan(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_SparkConnectServiceServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'ExecutePlan': grpc.unary_stream_rpc_method_handler(
                    servicer.ExecutePlan,
                    request_deserializer=spark__connect__pb2.Request.FromString,
                    response_serializer=spark__connect__pb2.Response.SerializeToString,
            ),
            'AnalyzePlan': grpc.unary_unary_rpc_method_handler(
                    servicer.AnalyzePlan,
                    request_deserializer=spark__connect__pb2.Request.FromString,
                    response_serializer=spark__connect__pb2.AnalyzeResponse.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'SparkConnectService', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class SparkConnectService(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def ExecutePlan(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_stream(request, target, '/SparkConnectService/ExecutePlan',
            spark__connect__pb2.Request.SerializeToString,
            spark__connect__pb2.Response.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def AnalyzePlan(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/SparkConnectService/AnalyzePlan',
            spark__connect__pb2.Request.SerializeToString,
            spark__connect__pb2.AnalyzeResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)