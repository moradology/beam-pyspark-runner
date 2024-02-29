import dataclasses
from typing import Dict, Sequence, Optional

from pyspark.rdd import RDD
from pyspark.sql import SparkSession

from apache_beam.pipeline import AppliedPTransform
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners.runner import PipelineResult, PipelineRunner, PipelineState
from apache_beam.pipeline import PipelineVisitor
from apache_beam.transforms.external import ExternalTransform

from .pyspark_visitors import EvalContextPipelineVisitor, VerifyNoCrossLanguageTransforms
from .evaluation_context import EvaluationContext
from .evaluator import Evaluator
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
        debugging = True

        pyspark_options = options.view_as(PySparkOptions)
        # application_name = pyspark_options.application_name
        # spark = SparkSession.builder.appName(application_name).getOrCreate()

        # Only accept Python transforms
        pipeline.visit(VerifyNoCrossLanguageTransforms())

        self.graph_view_visitor = EvalContextPipelineVisitor()
        pipeline.visit(self.graph_view_visitor)
        if debugging:
            self.graph_view_visitor.print_full_graph()
            self.graph_view_visitor.collect_all_paths()
            self.graph_view_visitor.print_all_paths()

        # eval_ctx = EvaluationContext()
        # eval = RddEvaluator(eval_ctx)
        # eval.evaluate_to_rdd(self.consumer_tracking_visitor.root_transforms)
        
        # spark_visitor.last_rdd.collect()
        # result_rdd = NotImplemented("this isnt a thing yet.")
        # result_rdd.collect()
        # spark.stop()
        
        return PipelineResult(PipelineState.DONE)