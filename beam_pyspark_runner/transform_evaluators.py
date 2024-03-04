from abc import ABC, abstractmethod
from dataclasses import dataclass
import math
from typing import Callable, List, Optional, Sequence

from apache_beam import pvalue
from apache_beam.pipeline import AppliedPTransform
from apache_beam.transforms import core
from pyspark import RDD, SparkContext

from .eval_context import EvalContext, NodeContext
from .overrides import _Create

def eval_Impulse(applied_)

def eval_Create(applied_transform: AppliedPTransform, eval_args: None, sc: SparkContext) -> RDD:
    assert eval_args is None, 'Create expects no input'
    items = applied_transform.transform.values
    num_partitions = max(1, math.ceil(math.sqrt(len(items)) / math.sqrt(100)))
    rdd = sc.parallelize(items, num_partitions)
    return rdd

def eval_ParDo(applied_transform: AppliedPTransform, eval_args: List[RDD], sc: SparkContext):
    assert len(eval_args) == 1, "ParDo expects input of length 1"
    print("EVAL ARGS IN PARDO")
    print(applied_transform)
    print(eval_args)
    print("EVAL ARGS IN PARDO")
    transform = applied_transform.transform
    rdd = eval_args[0]
    return rdd.flatMap(lambda x: transform.fn.process(x, *transform.args, **transform.kwargs))

def eval_Flatten(applied_transform: AppliedPTransform, eval_args: List[RDD], sc: SparkContext):
    if not eval_args:
        sc.emptyRDD()
    return sc.union(eval_args)

def eval_GroupByKey(applied_transform: AppliedPTransform, eval_args: List[RDD], sc: SparkContext):
    assert len(eval_args) == 1, "GroupByKey expects input of length 1"
    print("EVAL ARGS IN GROUPBYKEY")
    print(applied_transform)
    print(eval_args)
    print("EVAL ARGS IN GROUPBYKEY")
    rdd = eval_args[0]
    return rdd.groupByKey()


evaluator_mapping = {
    core.Impulse: eval_Impulse,
    core.ParDo: eval_ParDo,
    core.Flatten: eval_Flatten,
    core.GroupByKey: eval_GroupByKey
}

def get_eval_fn(applied_ptransform: AppliedPTransform) -> Callable[[AppliedPTransform, Optional[RDD], SparkContext], RDD]:
    # Walk up the class hierarchy to find an evaluable type. This is necessary
    # for supporting sub-classes of core transforms.
    for cls in applied_ptransform.transform.__class__.mro():
        eval_fn = evaluator_mapping.get(cls)
        if eval_fn:
            print(f"Eval: {cls} -> {eval_fn} ")
            break

    if not eval_fn:
        raise NotImplementedError(
            'Execution of [%s] not implemented in pyspark runner.' %
            type(applied_ptransform.transform))

    return eval_fn
