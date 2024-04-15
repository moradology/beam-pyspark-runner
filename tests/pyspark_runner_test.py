import glob
import os
import tempfile
import unittest
import warnings
from typing import Any, Callable, TypeVar

import apache_beam as beam
from apache_beam.testing import test_pipeline
from apache_beam.testing.util import assert_that, equal_to

from beam_pyspark_runner.pyspark_runner import PySparkRunner


class PySparkRunnerRunPipelineTest(unittest.TestCase):

    Element = TypeVar("Element")
    Accumulator = TypeVar("Accumulator")

    def build_reduce_fn(
        self,
        accumulate_op: Callable[[Element, Element], Accumulator],
        merge_op: Callable[[Accumulator, Accumulator], Accumulator],
        initializer: Callable[[], Accumulator],
        extractor: Callable[[Accumulator], Any] = lambda x: x,
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
            pcoll | "first branch" >> beam.Map(lambda x: x / 5)
            p2 = (
                pcoll
                | "second branch" >> beam.Map(lambda x: x + 1)
                | "super label" >> beam.Map(lambda y: y * 2)
                | "yet another label" >> beam.Map(lambda z: z - 1)
            )
            pcoll | "third branch" >> beam.Map(lambda x: x + 2)
            p2 | "sharing second" >> beam.Map(lambda x: x * 100)
            assert_that(p2, equal_to([3]))

    def test_flatmap(self):
        with self.pipeline as p:
            pcoll = (
                p | beam.Create([[1], [3, 4]]) | "flatmap sum" >> beam.FlatMap(lambda x: [sum(x)])
            )
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
            extractor=lambda x: x,
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
        input_list = [1, 2, 3, 4, 10]
        expected_values = [str((v * 2, [v])) for v in input_list]

        def double_key(x):
            return x * 2, x

        with tempfile.TemporaryDirectory() as test_output_dir:
            # beam expects trailing '/' to treat this as a directory!
            test_output_dir = test_output_dir + "/"

            # Run pipeline
            with self.pipeline as p:
                p | beam.Create(input_list) | beam.Map(
                    double_key
                ) | beam.GroupByKey() | "write test" >> beam.io.WriteToText(test_output_dir)

            # Check results
            output_files = glob.glob(os.path.join(test_output_dir, "*"))
            all_contents = []
            for output_file in output_files:
                with open(output_file, "r") as file:
                    contents = file.readlines()
                    all_contents.extend(contents)
            all_contents = [line.strip() for line in all_contents]

            for expected in expected_values:
                self.assertIn(expected, all_contents)

    def test_dict_side_input(self):
        class UseMultimap(beam.DoFn):
            def process(self, element, side_input_multimap):
                # 'element' is a key to look up in the multimap 'side_input_multimap'
                values_for_key = side_input_multimap.get(element, [])
                for value in values_for_key:
                    yield f"{element}: {value}"

        with self.pipeline as p:
            side_input_multimap = p | "Create Side Input" >> beam.Create(
                [("key1", ["value1a", "value1b"]), ("key2", ["value2a"])]
            )
            side_input_dict = beam.pvalue.AsDict(side_input_multimap)

            pcoll = (
                p
                | "Create Elements" >> beam.Create(["key1", "key2"])
                | "Use Multimap" >> beam.ParDo(UseMultimap(), side_input_multimap=side_input_dict)
            )
            assert_that(pcoll, equal_to(["key1: value1a", "key1: value1b", "key2: value2a"]))
