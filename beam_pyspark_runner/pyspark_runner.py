import logging

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners.runner import PipelineResult, PipelineRunner, PipelineState
from pyspark.sql import SparkSession

from .eval_context import EvalContext
from .evaluator import RDDEvaluator
from .execution_plan import PysparkPlan, PysparkStage
from .overrides import pyspark_overrides
from .pyspark_visitors import (
    EvalContextPipelineVisitor,
    VerifyNoCrossLanguageTransforms,
)

logger = logging.getLogger(__name__)


class PysparkResult(PipelineResult):
    def __post_init__(self):
        super().__init__(PipelineState.RUNNING)

    def wait_until_finish(self, duration=None) -> str:
        return self._state

    def cancel(self) -> str:
        self._state = PipelineState.CANCELLED
        return self._state

    def metrics(self):
        raise NotImplementedError("metrics later")


class PySparkOptions(PipelineOptions):
    """TODO: Any spark-specific options we want to include"""

    @classmethod
    def _add_argparse_args(cls, parser):
        # Add an argument for specifying the application name
        parser.add_argument(
            "--application_name",
            type=str,
            help="Name of the PySpark application",
            default="BeamPySparkApp",
        )


class PySparkRunner(PipelineRunner):
    """Executes a pipeline using Apache Spark."""

    def run_pipeline(self, pipeline, options):
        pyspark_options = options.view_as(PySparkOptions)

        # Handle necessary AST overrides
        pipeline.replace_all(pyspark_overrides)

        # Verify that all nodes are pure python nodes: that's all we support
        pipeline.visit(VerifyNoCrossLanguageTransforms())

        # Build up required holistic AST context
        context_visitor = EvalContextPipelineVisitor()
        pipeline.visit(context_visitor)
        eval_ctx = EvalContext(context_visitor)

        leaves = eval_ctx.leaves
        side_input_producers = eval_ctx.side_input_producers
        stage_nodes = leaves | side_input_producers
        # Construct plan for execution
        execution_plan = PysparkPlan([PysparkStage.from_node(node) for node in stage_nodes])

        # log out a description of all nodes
        eval_ctx.log_node_contexts()

        # report on execution plan
        execution_plan.log_execution_plan()

        # Execute plan
        execution_record = {}
        dependencies = {}
        evaluator = RDDEvaluator(eval_ctx.nodes_to_cache)
        spark = SparkSession.builder.appName(pyspark_options.application_name).getOrCreate()
        for idx, stage in enumerate(execution_plan.topologically_ordered()):
            logger.debug(f"Beginning stage {idx + 1} of {len(execution_plan.stages)}")
            logger.debug(f"Beginning evaluation of terminal node {stage.terminal_node}")
            # If results later needed as side-inputs, save them. Else, avoid materializing results to driver
            if stage.terminal_node in execution_plan.all_dependencies:
                result = evaluator.evaluate_node(
                    stage.terminal_node, spark.sparkContext, dependencies
                ).collect()
                dependencies[stage.terminal_node] = result
            else:
                evaluator.evaluate_node(
                    stage.terminal_node, spark.sparkContext, dependencies
                ).foreach(lambda x: x)
                result = f"Side effecting result calculated for {stage.terminal_node}"
            execution_record[stage.terminal_node.full_label] = result
            logger.debug(f"Completed stage {idx + 1} of {len(execution_plan.stages)}")
            logger.debug(f"Completed evaluation of terminal node {stage.terminal_node}")

        # Report on execution record
        logger.debug("\n=============================")
        logger.debug(f"Execution Record for {pyspark_options.application_name}")
        logger.debug("=============================")
        for node_label, execution in execution_record.items():
            logger.debug(f"{node_label}: {execution}")
        logger.debug("=============================\n")

        return PysparkResult(state=PipelineState.DONE)
