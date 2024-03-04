import dataclasses
import itertools
from typing import List

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

    def evaluate_node(self, aptrans: AppliedPTransform, sc: SparkContext):
        node_label = aptrans.full_label
        if node_label in self.result_cache:
            evaluated = self.result_cache[node_label]
        else:
            evaluator_fn = get_eval_fn(aptrans)
            if not [input for input in aptrans.inputs if not isinstance(input, pvalue.PBegin)]: # root
                evaluated = evaluator_fn(aptrans, None, sc)
                self.result_cache[aptrans.full_label] = evaluated
            else:
                eval_args = [self.evaluate_node(input.producer, sc) for input in aptrans.inputs]
                evaluated = evaluator_fn(aptrans, eval_args, sc)
                self.result_cache[aptrans.full_label] = evaluated
        return evaluated

    # def evaluate_path(self, path: List[NodeContext], sc: SparkContext) -> RDD:
    #     import pprint
    #     path_rdds = []
    #     print("path to evaluate")
    #     pprint.pprint(list(map(lambda x: x.as_dict(), path)))
    #     for ctx in path:
    #         print("STARTING EVAL OF:", ctx)
    #         path_rdds.append(self.evaluate_node(ctx, sc))
    #     # The final path should represent the *full* lazy transform to be computed here
    #     return path_rdds[-1]

