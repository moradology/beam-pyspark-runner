import dataclasses
from typing import Dict, Sequence, Optional

from pyspark.rdd import RDD
from pyspark.sql import SparkSession

from apache_beam.pipeline import AppliedPTransform
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners.runner import PipelineResult, PipelineRunner, PipelineState
from apache_beam.pipeline import PipelineVisitor
from apache_beam.transforms.external import ExternalTransform

from .pyspark_visitors import ConsumerTrackingPipelineVisitor, VerifyNoCrossLanguageTransforms
from .evaluation_context import EvaluationContext
from .evaluator import Evaluator, TransformEvaluatorRegistry
from .transform_evaluator import NoOp, TRANSLATIONS
from .overrides import pyspark_overrides


class PySparkOptions(PipelineOptions):
    """TODO: Any spark-specific options we want to include"""
    @classmethod
    def _add_argparse_args(cls, parser):
        # Add an argument for specifying the application name
        parser.add_value_provider_argument('--application_name',
                                           type=str,
                                           help='Name of the PySpark application',
                                           default='BeamPySparkApp')


class PySparkRunner(PipelineRunner):
    """Executes a pipeline using Apache Spark."""


    def run_pipeline(self, pipeline, options):

        pyspark_options = options.view_as(PySparkOptions)
        # application_name = pyspark_options.application_name
        # spark = SparkSession.builder.appName(application_name).getOrCreate()
        
        pipeline.visit(VerifyNoCrossLanguageTransforms())
        self.consumer_tracking_visitor = ConsumerTrackingPipelineVisitor()
        pipeline.visit(self.consumer_tracking_visitor)
        # spark_visitor = self.to_spark_rdd_visitor(spark)
        # pipeline.replace_all(pyspark_overrides())
        # pipeline.visit(spark_visitor)
        evaluation_context = EvaluationContext(
            pyspark_options,
            self.consumer_tracking_visitor.root_transforms,
            self.consumer_tracking_visitor.value_to_consumers,
            self.consumer_tracking_visitor.step_names,
            self.consumer_tracking_visitor.views
        )
        eval = Evaluator(
            self.consumer_tracking_visitor.value_to_consumers,
            TransformEvaluatorRegistry(evaluation_context),
            evaluation_context
        )

        eval.evaluate(self.consumer_tracking_visitor.root_transforms)
        
        # spark_visitor.last_rdd.collect()
        # result_rdd = NotImplemented("this isnt a thing yet.")
        # result_rdd.collect()
        # spark.stop()
        
        return PipelineResult(PipelineState.DONE)