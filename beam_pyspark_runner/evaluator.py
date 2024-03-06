import dataclasses
import itertools
from typing import Any, Dict, List

from apache_beam import pvalue, Pipeline
from apache_beam.pipeline import AppliedPTransform
from pyspark import RDD, SparkContext

from .eval_context import EvalContext
from .pyspark_visitors import NodeContext
from .transform_evaluators import get_eval_fn


class RDDEvaluator(object):
    def __init__(
        self
    ):
        self.result_cache = {}

    def evaluate_node(self, aptrans: AppliedPTransform, sc: SparkContext, dependencies: Dict[AppliedPTransform, Any]={}):
        node_label = aptrans.full_label
        if node_label in self.result_cache:
            evaluated = self.result_cache[node_label]
        else:
            evaluator_fn = get_eval_fn(aptrans)
            if not [input for input in aptrans.inputs if not isinstance(input, pvalue.PBegin)]: # root
                evaluated = evaluator_fn(aptrans, None, sc, side_inputs=dependencies)
                self.result_cache[aptrans.full_label] = evaluated
            else:
                eval_args = [self.evaluate_node(input.producer, sc, dependencies) for input in aptrans.inputs]
                evaluated = evaluator_fn(aptrans, eval_args, sc, side_inputs=dependencies)
                self.result_cache[aptrans.full_label] = evaluated
        return evaluated
