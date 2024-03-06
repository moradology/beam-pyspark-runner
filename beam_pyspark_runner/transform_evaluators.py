from abc import ABC, abstractmethod
from dataclasses import dataclass
import math
from typing import Any, Callable, List, Optional, Sequence

from apache_beam import pvalue
from apache_beam.pipeline import AppliedPTransform
from apache_beam.transforms import core
from pyspark import RDD, SparkContext

from .eval_context import EvalContext, NodeContext
from .overrides import _Create, _GroupByKey, _CombinePerKey


def eval_Create(applied_transform: AppliedPTransform, eval_args: None, sc: SparkContext) -> RDD:
    assert eval_args is None, 'Create expects no input'
    items = applied_transform.transform.values
    num_partitions = max(1, math.ceil(math.sqrt(len(items)) / math.sqrt(100)))
    rdd = sc.parallelize(items, num_partitions)
    # print("===============================")
    # print("RESULTS IN CREATE")
    # print(rdd.collect())
    # print("===============================\n")
    return rdd

def eval_ParDo(applied_transform: AppliedPTransform, eval_args: List[RDD], sc: SparkContext):
    assert len(eval_args) == 1, "ParDo expects input of length 1"
    transform = applied_transform.transform
    rdd = eval_args[0]
    # print("===============================")
    # print("START EVAL PARDO", applied_transform.full_label)
    # print("pardo aptrans", applied_transform, applied_transform.full_label)
    # print("pardo trans", transform)
    # print("RESULTS IN PARDO")
    # print(rdd.flatMap(lambda x: transform.fn.process(x, *transform.args, **transform.kwargs)).collect())
    # print("END EVAL PARDO")
    # print("===============================\n")
    return rdd.flatMap(lambda x: transform.fn.process(x, *transform.args, **transform.kwargs))

def eval_Flatten(applied_transform: AppliedPTransform, eval_args: List[RDD], sc: SparkContext):
    # print("===============================")
    # print("RESULTS IN flatten")
    # print(sc.union(eval_args).collect())
    # print("END flatten")
    # print("===============================\n")
    return sc.union(eval_args)

def eval_GroupByKey(applied_transform: AppliedPTransform, eval_args: List[RDD], sc: SparkContext):
    assert len(eval_args) == 1, "ParDo expects input of length 1"
    rdd = eval_args[0]
    # print("===============================")
    # print("RESULTS IN GROUPBYKEY")
    # print(rdd.groupByKey().mapValues(list).collect())
    # print("END GROUPBYKEY")
    # print("===============================\n")
    return rdd.groupByKey().mapValues(list)

def eval_CombinePerKey(applied_transform, eval_args: List[RDD], sc: SparkContext):
    assert len(eval_args) == 1, "CombinePerKey expects input of length 1"
    rdd = eval_args[0]
    combine_fn = applied_transform.transform._combine_fn
    result_rdd = rdd.aggregateByKey(combine_fn.create_accumulator(), combine_fn.add_input, combine_fn.merge_accumulators)
    # print("===============================")
    # print("RESULTS IN COMBINE PER KEY")
    # print(result_rdd.collect())
    # print("===============================\n")
    return result_rdd


def NoOp(applied_transform: AppliedPTransform, eval_args: Any, sc: SparkContext):
    print("===============================")
    print(f"NOOP VALUE HERE: {NoOp}", applied_transform)
    print("===============================\n")
    return eval_args[0]


evaluator_mapping = {
    _Create: eval_Create,
    core.Flatten: eval_Flatten,
    _GroupByKey: eval_GroupByKey,
    _CombinePerKey: eval_CombinePerKey,
    core.ParDo: eval_ParDo,
}

def get_eval_fn(applied_ptransform: AppliedPTransform) -> Callable[[AppliedPTransform, Optional[RDD], SparkContext], RDD]:
    # Walk up the class hierarchy to find an evaluable type. This is necessary
    # for supporting sub-classes of core transforms.
    # for cls in applied_ptransform.transform.__class__.mro():
    #     eval_fn = evaluator_mapping.get(cls)
    #     if eval_fn:
    #         print(f"Eval: {cls} -> {eval_fn} ")
    #         break
    cls = applied_ptransform.transform.__class__
    eval_fn = evaluator_mapping.get(cls, NoOp)
    print(f"USING {eval_fn} FOR {cls} @ {applied_ptransform.full_label}")

    if not eval_fn:
        raise NotImplementedError(
            'Execution of [%s] not implemented in pyspark runner.' %
            type(applied_ptransform.transform))

    return eval_fn
