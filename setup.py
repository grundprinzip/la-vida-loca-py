from setuptools import setup

setup(
    name='la-vida-local',
    version='1.4.1',
    packages=['pyspark',
      'pyspark.cloudpickle',
      'pyspark.sql',
      'pyspark.sql.connect',
      'pyspark.sql.connect.proto'],
)