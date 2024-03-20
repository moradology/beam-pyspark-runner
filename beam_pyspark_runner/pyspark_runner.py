import dataclasses
from typing import List

from pyspark.sql import SparkSession

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners.runner import PipelineResult, PipelineRunner, PipelineState

from .eval_context import EvalContext
from .evaluator import RDDEvaluator
from .execution_plan import PysparkPlan, PysparkStage
from .overrides import pyspark_overrides
from .pyspark_visitors import EvalContextPipelineVisitor, VerifyNoCrossLanguageTransforms


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
        parser.add_argument("--application_name",
                            type=str,
                            help="Name of the PySpark application",
                            default="BeamPySparkApp")
        parser.add_argument("--print_execution_plan",
                            action="store_false",
                            help="Flag to print a description of this pipeline's execution")
        parser.add_argument("--debug",
                            action="store_true",
                            help="Flag to print stuff for debugging")


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

        # Construct plan for execution
        execution_plan = PysparkPlan([PysparkStage.from_terminal_node(leaf) for leaf in eval_ctx.leaves])

        # Optionally print out a description of all nodes
        if pyspark_options.debug:
            eval_ctx.print_node_contexts()

        # Optionally report on execution plan
        if pyspark_options.print_execution_plan:
            print("\n=============================")
            print(f"Execution Plan for {pyspark_options.application_name}")
            print("=============================")
            for idx, stage in enumerate(execution_plan.topologically_ordered()):
                print(f"Stage {idx+1} of {len(execution_plan.stages)}")
                print(f"Calculate terminal node {stage.terminal_node}")
                if stage.side_input_dependencies:
                    print(f"  - With dependencies on {stage.side_input_dependencies}")
            print("=============================\n")

        # Execute plan
        execution_record = {}
        dependencies = {}
        evaluator = RDDEvaluator(eval_ctx.nodes_to_cache)
        spark = SparkSession.builder.appName(pyspark_options.application_name).getOrCreate()
        for idx, stage in enumerate(execution_plan.topologically_ordered()):
            print(f"Beginning stage {idx + 1} of {len(execution_plan.stages)}")
            print(f"Beginning evaluation of terminal node {stage.terminal_node}")
            # If results later needed as side-inputs, save them. Else, avoid materializing results to driver
            if stage.terminal_node in execution_plan.all_dependencies:
                result = evaluator.evaluate_node(stage.terminal_node, spark.sparkContext, dependencies).collect()
                dependencies[stage.terminal_node] = result
            else:
                evaluator.evaluate_node(stage.terminal_node, spark.sparkContext, dependencies).foreach(lambda x: x)
                result = f"Side effecting result calculated for {stage.terminal_node}"
            execution_record[stage.terminal_node.full_label] = result
            print(f"Completed stage {idx + 1} of {len(execution_plan.stages)}")
            print(f"Completed evaluation of terminal node {stage.terminal_node}")

        if pyspark_options.print_execution_plan:
            print("\n=============================")
            print(f"Execution Record for {pyspark_options.application_name}")
            print("=============================")
            for node_label, execution in execution_record.items():
                print(f"{node_label}: {execution}")
            print("=============================\n")
            
        return PysparkResult(state=PipelineState.DONE)