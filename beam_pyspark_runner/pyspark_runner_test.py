import unittest
from typing import Any, Callable, TypeVar

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing import test_pipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

from .pyspark_runner import PySparkRunner
try:
    from pyspark.sql import SparkSession
except (ImportError, ModuleNotFoundError):
    raise unittest.SkipTest('Spark must be installed to run these tests.')

class PySparkRunnerRunPipelineTest(unittest.TestCase):

    Element = TypeVar("Element")
    Accumulator = TypeVar("Accumulator")
    def build_reduce_fn(
        self,
        accumulate_op: Callable[[Element, Element], Accumulator],
        merge_op: Callable[[Accumulator, Accumulator], Accumulator],
        initializer: Accumulator,
        extractor: Callable[[Accumulator], Any] = lambda x: x
    ) -> beam.CombineFn:
        
        """Factory to construct reducers without so much ceremony"""
        class AnonymousCombineFn(beam.CombineFn):
            def create_accumulator(self):
                return initializer

            def add_input(self, accumulator, input):
                return accumulate_op(accumulator, input)

            def merge_accumulators(self, accumulators):
                acc = accumulators[0]
                for accumulator in accumulators[1:]:
                    acc = merge_op(acc, accumulator)
                return acc

            def extract_output(self, accumulator):
                return extractor(accumulator)

        return AnonymousCombineFn

    """Test class used to introspect the spark runner via a debugger."""
    def setUp(self) -> None:
        self.pipeline = test_pipeline.TestPipeline(runner=PySparkRunner())

    def test_create(self):
        with self.pipeline as p:
            pcoll = p | beam.Create([1])
            pcoll | "first one" >> beam.Map(lambda x: x / 5)
            # pcoll | "big label" >> beam.Map(lambda x: x + 1) | "super label" >> beam.Map(lambda y: y * 2) | "yet another label" >> beam.Map(lambda z: z - 3)
            # pcoll | "more names" >> beam.Map(lambda x: x + 2)

    # def test_map(self):
    #     def double(x):
    #         return x * 2

    #     with self.pipeline as p:
    #         pcoll = p | beam.Create([1]) | beam.Map(double)
    #         assert_that(pcoll, equal_to([2]))

    # def test_combine(self):
    #     sum_combiner = self.build_reduce_fn(lambda x, y: x + y, lambda x,  y: x + y, 0)

    #     self.pipeline | beam.Create([1, 2, 3, 4]) | beam.CombineGlobally(sum)
    #     self.pipeline.run()

    # def test_create_map_and_groupby(self):
    #     def double(x):
    #         return x * 2, x

    #     with self.pipeline as p:
    #         pcoll = p | beam.Create([1]) | beam.Map(double) | beam.GroupByKey()
    #         assert_that(pcoll, equal_to([(2, [1])]))

    # def test_create_map_and_groupby_other_thing(self):
    #     def double(x):
    #         return x * 2, x

    #     self.pipeline | beam.Create([1]) | beam.Map(double) | beam.GroupByKey() | beam.Map(lambda x: print(x))
    #     self.pipeline.run()

if __name__ == '__main__':
    unittest.main()