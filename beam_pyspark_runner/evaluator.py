import dataclasses
import itertools
from typing import List

from apache_beam import Pipeline
from apache_beam.pipeline import AppliedPTransform
from pyspark import SparkContext

from .eval_context import EvalContext
from .transform_evaluators import EvalMapping


class RDDEvaluator(object):
    def __init__(
        self,
        spark_context: SparkContext,
        eval_context: EvalContext,
        eval_mapping: EvalMapping = EvalMapping(),
    ):
        self.spark_context = spark_context
        self.eval_context = eval_context
        self.eval_mapping = eval_mapping
        self.result_cache = {}

    def eval_cache(self, transform_label):
        if transform_label in self.result_cache:
            evaluated_result = self.result_cache[transform_label]
        else:
            evaluated_result = self.evaluate_node(transform_label)
        return evaluated_result

    def evaluate_node(self, transform_label: str):
        node_context = self.eval_context.get_node_context(transform_label)
        evaluator_class = self.eval_mapping.get_eval_class(node_context.applied_transform)
        if node_context.is_root:
            evaluated = evaluator_class(self.spark_context, self.eval_context, node_context).apply(None)
        else:
            eval_args = [self.eval_cache(input.producer.full_label) for input in node_context.inputs]
            evaluated = evaluator_class(self.spark_context, self.eval_context, node_context).apply(eval_args)
        return evaluated

    def evaluate_path(self, path_labels: List[str]):
        for transform_label in path_labels:
            evaluated = self.eval_cache(transform_label)
        return(evaluated)

    def evaluate_pipeline(self, pipeline: Pipeline):
        path_rdds = []
        for path_labels in self.eval_context.paths:
            path_rdds.append(self.evaluate_path(path_labels))
        return path_rdds
