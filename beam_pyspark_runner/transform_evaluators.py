import math
from typing import Any, Callable, List, Optional

from apache_beam.pipeline import AppliedPTransform
from apache_beam.transforms import core
from pyspark import RDD, SparkContext

from .overrides import _Create, _GroupByKey, _CombinePerKey


def eval_Create(applied_transform: AppliedPTransform, eval_args: None, sc: SparkContext, side_inputs={}) -> RDD:
    assert eval_args is None, 'Create expects no input'
    items = applied_transform.transform.values
    num_partitions = max(1, math.ceil(math.sqrt(len(items)) / math.sqrt(100)))
    rdd = sc.parallelize(items, num_partitions)
    return rdd

def eval_ParDo(applied_transform: AppliedPTransform, eval_args: List[RDD], sc: SparkContext, side_inputs={}):
    assert len(eval_args) == 1, "ParDo expects input of length 1"
    transform = applied_transform.transform
    rdd = eval_args[0]
    broadcast_args = [sc.broadcast(side_inputs[si.pvalue.producer]) for si in applied_transform.side_inputs]
    def apply_with_side_input(x):
        broadcast_values = [broadcast.value for broadcast in broadcast_args]
        return transform.fn.process(x, *broadcast_values, **transform.kwargs)
    return rdd.flatMap(apply_with_side_input)

def eval_Flatten(applied_transform: AppliedPTransform, eval_args: List[RDD], sc: SparkContext, side_inputs={}):
    return sc.union(eval_args)

def eval_GroupByKey(applied_transform: AppliedPTransform, eval_args: List[RDD], sc: SparkContext, side_inputs={}):
    assert len(eval_args) == 1, "ParDo expects input of length 1"
    rdd = eval_args[0]
    return rdd.groupByKey().mapValues(list)

def eval_CombinePerKey(applied_transform, eval_args: List[RDD], sc: SparkContext, side_inputs={}):
    assert len(eval_args) == 1, "CombinePerKey expects input of length 1"
    rdd = eval_args[0]
    combine_fn = applied_transform.transform._combine_fn
    result_rdd = rdd.aggregateByKey(
        combine_fn.create_accumulator(),
        combine_fn.add_input,
        combine_fn.merge_accumulators
    ).mapValues(combine_fn.extract_output)
    return result_rdd


def NoOp(applied_transform: AppliedPTransform, eval_args: Any, sc: SparkContext, side_inputs={}):
    return eval_args[0]


evaluator_mapping = {
    _Create: eval_Create,
    core.Flatten: eval_Flatten,
    _GroupByKey: eval_GroupByKey,
    _CombinePerKey: eval_CombinePerKey,
    core.ParDo: eval_ParDo,
}

def get_eval_fn(applied_ptransform: AppliedPTransform) -> Callable[[AppliedPTransform, Optional[RDD], SparkContext], RDD]:
    cls = applied_ptransform.transform.__class__
    eval_fn = evaluator_mapping.get(cls, NoOp)

    if not eval_fn:
        raise NotImplementedError(
            'Execution of [%s] not implemented in pyspark runner.' %
            type(applied_ptransform.transform))

    return eval_fn
