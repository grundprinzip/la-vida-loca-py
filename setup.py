from setuptools import setup

setup(
    name='la-vida-local',
    version='1.5.0',
    install_requires = [
      'cloudpickle==1.6.0',
      'protobuf',
      'grpclib',
      'requests',
    ],
    packages=[
      'pyspark',
      'pyspark.cloudpickle',
      'pyspark.sql',
      'pyspark.sql.connect',
      'pyspark.sql.connect.proto'],
)