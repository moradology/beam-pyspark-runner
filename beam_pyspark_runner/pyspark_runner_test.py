import glob
import os
import unittest
import warnings
from typing import Any, Callable, TypeVar

import apache_beam as beam
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
        initializer: Callable[[], Accumulator],
        extractor: Callable[[Accumulator], Any] = lambda x: x
    ) -> beam.CombineFn:
        
        """Factory to construct reducers without so much ceremony"""
        class AnonymousCombineFn(beam.CombineFn):
            def create_accumulator(self):
                return initializer()

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
        warnings.filterwarnings("ignore", category=ResourceWarning)
        self.pipeline = test_pipeline.TestPipeline(runner=PySparkRunner())

    def test_create(self):
        with self.pipeline as p:
            pcoll = p | beam.Create([1])
            assert_that(pcoll, equal_to([1]))

    def test_multiple_paths(self):
        with self.pipeline as p:
            pcoll = p | beam.Create([1])
            p1 = pcoll | "first branch" >> beam.Map(lambda x: x / 5)
            p2 = pcoll | "second branch" >> beam.Map(lambda x: x + 1) | "super label" >> beam.Map(lambda y: y * 2) | "yet another label" >> beam.Map(lambda z: z - 1)
            p3 = pcoll | "third branch" >> beam.Map(lambda x: x + 2)
            p4 = p2 | "sharing second" >> beam.Map(lambda x: x * 100)
            assert_that(p2, equal_to([3]))

    def test_flatmap(self):
        with self.pipeline as p:
            pcoll = p | beam.Create([[1], [3, 4]]) | "flatmap sum" >> beam.FlatMap(lambda x: [sum(x)])
            assert_that(pcoll, equal_to([1, 7]))

    def test_map(self):
        with self.pipeline as p:
            pcoll = p | beam.Create([[1], [3, 4]]) | beam.Map(sum)
            assert_that(pcoll, equal_to([1, 7]))

    def test_combine(self):
        sum_fn = self.build_reduce_fn(
            accumulate_op=lambda x, y: x + y,
            merge_op=lambda x, y: x + y,
            initializer=lambda: 0,
            extractor=lambda x: x
        )

        with self.pipeline as p:
            pcoll = p | beam.Create([1, 2, 3, 4]) | beam.CombineGlobally(sum_fn())
            pcoll
            assert_that(pcoll, equal_to([10]))

    def test_create_map_and_groupby(self):
        def double_key(x):
            return x * 2, x

        with self.pipeline as p:
            pcoll = p | beam.Create([1]) | beam.Map(double_key) | beam.GroupByKey()
            assert_that(pcoll, equal_to([(2, [1])]))

    def test_write(self):
        # Test constants
        test_output_dir = "/tmp/test/"
        input_list = [1, 2, 3, 4, 10]
        expected_values = [str((v * 2, [v])) for v in input_list]

        # Clean out the /tmp/test/ dir
        if os.path.exists(test_output_dir):
            for file in glob.glob(os.path.join(test_output_dir, '*')):
                os.remove(file)

        def double_key(x):
            return x * 2, x

        # Run pipeline
        with self.pipeline as p:
            p | beam.Create(input_list) | beam.Map(double_key) | beam.GroupByKey() | "write test" >> beam.io.WriteToText(test_output_dir)

        # Check results
        output_files = glob.glob(os.path.join(test_output_dir, '*'))
        all_contents = []
        for output_file in output_files:
            with open(output_file, 'r') as file:
                contents = file.readlines()
                all_contents.extend(contents)
        all_contents = [line.strip() for line in all_contents]

        for expected in expected_values:
            self.assertIn(expected, all_contents)

        


if __name__ == '__main__':
    unittest.main()