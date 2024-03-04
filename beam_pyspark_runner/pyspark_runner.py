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

        # Only accept Python transforms
        pipeline.visit(VerifyNoCrossLanguageTransforms())

        context_visitor = EvalContextPipelineVisitor()
        pipeline.visit(context_visitor)

        eval_ctx = EvalContext(context_visitor)
        if debugging:
            eval_ctx.print_full_graph()
        #    eval_ctx.print_all_paths()
        evaluator = RDDEvaluator()

        # Construct plan for execution
        # TODO: optimize pipeline
        # TODO: organize plan for application of `side_inputs`
        spark = SparkSession.builder.appName(application_name).getOrCreate()
        # Evaluate nodes into RDDs
        rdd_paths = []
        for leaf in eval_ctx.leaves:
            rdd_paths.append(evaluator.evaluate_node(leaf.applied_transform, spark.sparkContext))

        # Run pipelines
        results = []
        for rdd in rdd_paths:
            res = rdd.collect()
            results.append(res)

        print("RESULTS", results)
        
        res = PipelineResult(PipelineState.DONE)
        return res