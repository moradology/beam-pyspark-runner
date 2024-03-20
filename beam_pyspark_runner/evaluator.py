from typing import Any, Dict

from apache_beam import pvalue
from apache_beam.pipeline import AppliedPTransform
from pyspark import SparkContext, StorageLevel

from .transform_evaluators import get_eval_fn


class RDDEvaluator(object):
    def __init__(
        self,
        nodes_to_cache: str
    ):
        self.nodes_to_cache = nodes_to_cache
        self.result_cache = {}

    def evaluate_node(self, applied_ptransform: AppliedPTransform, sc: SparkContext, dependencies: Dict[AppliedPTransform, Any]={}):
        node_label = applied_ptransform.full_label
        if node_label in self.result_cache:
            evaluated = self.result_cache[node_label]
        else:
            evaluator_fn = get_eval_fn(applied_ptransform)
            if not [input for input in applied_ptransform.inputs if not isinstance(input, pvalue.PBegin)]: # this is root
                evaluated = evaluator_fn(applied_ptransform, None, sc, side_inputs=dependencies)
            else:
                eval_args = [self.evaluate_node(input.producer, sc, dependencies) for input in applied_ptransform.inputs]
                evaluated = evaluator_fn(applied_ptransform, eval_args, sc, side_inputs=dependencies)

            # Save results of evaluation being mindful of need to reuse data
            if applied_ptransform.full_label in self.nodes_to_cache:
                evaluated.persist(StorageLevel.MEMORY_AND_DISK)
            self.result_cache[applied_ptransform.full_label] = evaluated
        return evaluated
