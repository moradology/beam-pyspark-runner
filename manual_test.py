#!/usr/bin/env python
from beam_pyspark_runner.pyspark_runner import PySparkRunner

import apache_beam as beam

def double_key(value):
    return (value * 2, value)

with beam.Pipeline(runner=PySparkRunner()) as p:
    p | beam.Create([1, 2, 3, 4, 5, 6, 10]) | beam.Map(double_key) | beam.GroupByKey() | "write test" >> beam.io.WriteToText("/tmp/test/")