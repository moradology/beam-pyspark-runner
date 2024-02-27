import dataclasses
from functools import reduce
import math
import typing as t

import apache_beam
from pyspark.rdd import RDD
from pyspark.sql import SparkSession
from apache_beam.pipeline import AppliedPTransform
from apache_beam.transforms.window import GlobalWindows

from .overrides import _Create, _Flatten, _GroupByKeyOnly


# OpInput for Spark would be an RDD or a list of RDDs
OpInput = t.Union[RDD, t.Sequence[RDD], None]


class SparkRDDOp:
    def __init__(self, applied: AppliedPTransform, spark: SparkSession):
        self.applied = applied
        self.spark = spark

    @property
    def transform(self):
        return self.applied.transform

    def apply(self, input_rdd: OpInput) -> RDD:
        raise NotImplementedError("This method should be implemented by subclasses.")


class NoOp(SparkRDDOp):
    def apply(self, input_rdd: OpInput) -> RDD:
        return input_rdd


class Create(SparkRDDOp):
    def apply(self, input_rdd: OpInput) -> RDD:
        assert input_rdd is None, 'Create expects no input!'
        original_transform = t.cast(_Create, self.transform)
        items = original_transform.values
        # print(f"CREATE ITEMS: {list(items)}")
        num_partitions = max(1, math.ceil(math.sqrt(len(items)) / math.sqrt(100)))
        rdd = self.spark.sparkContext.parallelize(items, num_partitions)
        return rdd


class ParDo(SparkRDDOp):
    def apply(self, input_rdd: RDD) -> RDD:
        transform = t.cast(apache_beam.ParDo, self.transform)
        # print("IN PARDO")
        # print(input_rdd.collect())
        # print(input_rdd.flatMap(lambda x: transform.fn.process(x, *transform.args, **transform.kwargs)).collect())
        # print("EXIT PARDO")
        return input_rdd.flatMap(lambda x: transform.fn.process(x, *transform.args, **transform.kwargs))


class Map(SparkRDDOp):
    def apply(self, input_rdd: RDD) -> RDD:
        transform = t.cast(apache_beam.Map, self.transform)
        return input_rdd.map(lambda x: transform.fn.process(x, *transform.args, **transform.kwargs))


class GroupByKey(SparkRDDOp):
    def apply(self, input_rdd: RDD) -> RDD:
        # print("IN GROUPBYKEY")
        # print(input_rdd)
        # print(type(input_rdd))
        # print("==========")
        # print(input_rdd.collect())
        # print("cooking...")
        # print(input_rdd.groupByKey().collect())
        # print("EXIT GROUPBYKEY")
        return input_rdd.groupByKey().mapValues(list)


class Flatten(SparkRDDOp):
    def apply(self, input_rdd: OpInput) -> RDD:
        assert isinstance(input_rdd, list), 'Must take a sequence of RDDs!'
        # print("IN FLATTEN")
        # print(reduce(lambda x, y: x.union(y), input_rdd))
        # print(reduce(lambda x, y: x.union(y), input_rdd).collect())
        # print("EXIT FLATTEN")
        return reduce(lambda x, y: x.union(y), input_rdd)


TRANSLATIONS = {
    _Create: Create,
    apache_beam.ParDo: ParDo,
    apache_beam.Map: Map,
    _GroupByKeyOnly: GroupByKey,
    _Flatten: Flatten,
}