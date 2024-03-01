import dataclasses
from typing import Dict, Sequence, Optional

from pyspark.rdd import RDD
from pyspark.sql import SparkSession

from apache_beam.pipeline import AppliedPTransform
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners.runner import PipelineResult, PipelineRunner, PipelineState
from apache_beam.pipeline import PipelineVisitor
from apache_beam.transforms.external import ExternalTransform

from .evaluator import EvalContext, RDDEvaluator
from .overrides import pyspark_overrides
from .pyspark_visitors import EvalContextPipelineVisitor, VerifyNoCrossLanguageTransforms


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
        application_name = pyspark_options.application_name
        spark = SparkSession.builder.appName(application_name).getOrCreate()

        pipeline.replace_all(pyspark_overrides())

        # Only accept Python transforms
        pipeline.visit(VerifyNoCrossLanguageTransforms())

        context_visitor = EvalContextPipelineVisitor()
        pipeline.visit(context_visitor)

        eval_ctx = EvalContext(context_visitor)
        if debugging:
            eval_ctx.print_full_graph()
            eval_ctx.print_all_paths()
        evaluator = RDDEvaluator(spark.sparkContext, eval_ctx)
        path_rdds = evaluator.evaluate_pipeline(pipeline)
        for rdd in path_rdds:
            rdd.collect()
        
        # spark_visitor.last_rdd.collect()
        # result_rdd = NotImplemented("this isnt a thing yet.")
        # result_rdd.collect()
        # spark.stop()
        
        res = PipelineResult(PipelineState.DONE)
        return res