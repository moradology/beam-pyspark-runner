import dataclasses
from typing import Dict, List, Sequence, Optional

from pyspark.rdd import RDD
from pyspark.sql import SparkSession

from apache_beam import pvalue
from apache_beam.pipeline import AppliedPTransform
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners.runner import PipelineResult, PipelineRunner, PipelineState
from apache_beam.pipeline import PipelineVisitor
from apache_beam.transforms.external import ExternalTransform

from .evaluator import EvalContext, RDDEvaluator
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
        raise NotImplementedError('metrics later')

@dataclasses.dataclass 
class PysparkStage:
    terminal_node: AppliedPTransform
    side_input_dependencies: set

    def __eq__(self, other):
        if not isinstance(other, PysparkStage):
            return NotImplemented

        return self.terminal_node == other.terminal_node

    def __hash__(self):
        return hash(self.terminal_node)

    @classmethod
    def from_terminal_node(cls, node: AppliedPTransform):
        def find_side_input_dependencies(node, current_deps=set()):
            for si in node.side_inputs:
                current_deps.add(si.pvalue.producer)
            if not node.inputs:
                return current_deps
            else:
                upstream_dependencies = set()
                for pval in node.inputs:
                    if isinstance(pval,pvalue.PBegin):
                        continue
                    producer_node = pval.producer
                    upstream_dependencies  = upstream_dependencies.union(find_side_input_dependencies(producer_node, current_deps))
                return current_deps.union(upstream_dependencies)
        return PysparkStage(node, find_side_input_dependencies(node))

@dataclasses.dataclass
class PysparkPlan:
    stages: List[PysparkStage]

    def optimize(self):
        pass

    def topologically_ordered(self):
        in_degree = {stage: len(stage.side_input_dependencies) for stage in self.stages}
        ordered_stages = []

        queue = [stage for stage in self.stages if in_degree[stage] == 0]

        while queue:
            stage = queue.pop(0)
            ordered_stages.append(stage)

            for potential_dependent in self.stages:
                if stage.terminal_node in potential_dependent.side_input_dependencies:
                    # Reduce in-degree and enqueue if it becomes 0
                    in_degree[potential_dependent] -= 1
                    if in_degree[potential_dependent] == 0:
                        queue.append(potential_dependent)

        if len(ordered_stages) == len(self.stages):
            return ordered_stages
        else:
            raise ValueError("A cycle was detected in the graph, or there are unresolved dependencies.")


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

        pipeline.replace_all(pyspark_overrides)

        # Only accept Python transforms
        pipeline.visit(VerifyNoCrossLanguageTransforms())

        context_visitor = EvalContextPipelineVisitor()
        pipeline.visit(context_visitor)

        eval_ctx = EvalContext(context_visitor)
        if debugging:
            eval_ctx.print_full_graph()
        evaluator = RDDEvaluator()

        leaves = [leaf.applied_transform for leaf in eval_ctx.leaves]
        execution_plan = PysparkPlan([PysparkStage.from_terminal_node(leaf) for leaf in leaves])

        # Construct plan for execution
        # TODO: optimize pipeline
        # Evaluate nodes into RDDs
        results = []
        dependencies = {}
        spark = SparkSession.builder.appName(application_name).getOrCreate()
        for stage in execution_plan.topologically_ordered():
            result = evaluator.evaluate_node(stage.terminal_node, spark.sparkContext, dependencies).collect()
            dependencies[stage.terminal_node] = result
            results.append(result)

        print("RESULTS", results)
            
        return PysparkResult(state=PipelineState.DONE)