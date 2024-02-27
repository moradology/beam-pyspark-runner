import unittest

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
    """Test class used to introspect the spark runner via a debugger."""
    def setUp(self) -> None:
        self.pipeline = test_pipeline.TestPipeline(runner=PySparkRunner())

    def test_create(self):
        with self.pipeline as p:
            pcoll = p | beam.Create([1])
            assert_that(pcoll, equal_to([1]))

    def test_create_and_map(self):
        def double(x):
            return x * 2

        with self.pipeline as p:
            pcoll = p | beam.Create([1]) | beam.Map(double)
            assert_that(pcoll, equal_to([2]))

    def test_create_map_and_groupby(self):
        def double(x):
            return x * 2, x

        with self.pipeline as p:
            pcoll = p | beam.Create([1]) | beam.Map(double) | beam.GroupByKey()
            assert_that(pcoll, equal_to([(2, [1])]))

    def test_create_map_and_groupby_other_thing(self):
        def double(x):
            return x * 2, x

        self.pipeline | beam.Create([1]) | beam.Map(double) | beam.GroupByKey() | beam.Map(lambda x: print(x))
        self.pipeline.run()

if __name__ == '__main__':
    unittest.main()