from abc import ABC, abstractmethod
from dataclasses import dataclass
import math
from typing import Union, Sequence

from apache_beam import pvalue
from apache_beam.pipeline import AppliedPTransform
from apache_beam.transforms import core
from pyspark import RDD, SparkContext

from .eval_context import EvalContext, NodeContext
from .overrides import _Create


OpInput = Union[Sequence[RDD], None]

class _TransformEvaluator(ABC):
    """An evaluator of a specific application of a transform."""
    def __init__(
        self,
        spark_context: SparkContext,
        evaluation_context: EvalContext,
        node_context: NodeContext
    ):
        self.evaluation_context = evaluation_context
        self.node_context = node_context
        self.applied_transform = node_context.applied_transform
        self.transform = self.applied_transform.transform
        self.inputs = node_context.inputs
        self.outputs = node_context.outputs
        self.spark_context = spark_context

    @abstractmethod
    def apply(self, sc: SparkContext, eval_args: OpInput):
       pass

class Create(_TransformEvaluator):
    def apply(self, eval_args: None) -> RDD:
        assert eval_args is None, 'Create expects no input'
        items = self.applied_transform.transform.values
        num_partitions = max(1, math.ceil(math.sqrt(len(items)) / math.sqrt(100)))
        rdd = self.spark_context.parallelize(items, num_partitions)
        return rdd

class _ParDo(_TransformEvaluator):
    def apply(self, eval_args: RDD):
       assert len(eval_args) == 1, "ParDo expects input of length 1"
       rdd = eval_args[0]
       return rdd.flatMap(lambda x: self.transform.fn.process(x, *self.transform.args, **self.transform.kwargs))


class EvalMapping(object):
    evaluator_mapping = {
        _Create: Create,
        core.ParDo: _ParDo
    }

    def get_eval_class(self, applied_ptransform: AppliedPTransform) -> _TransformEvaluator:
        # Walk up the class hierarchy to find an evaluable type. This is necessary
        # for supporting sub-classes of core transforms.
        print(f"Finding eval class for {applied_ptransform}")
        for cls in applied_ptransform.transform.__class__.mro():
            eval_class = self.evaluator_mapping.get(cls)
            if eval_class:
                print(f"Eval: {cls} -> {eval_class} ")
                break
            if not eval_class:
                raise NotImplementedError(
                    'Execution of [%s] not implemented in runner %s.' %
                    (type(applied_ptransform.transform), self))

        return eval_class
