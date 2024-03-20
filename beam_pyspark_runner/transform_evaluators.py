import math
from typing import Any, Callable, List, Optional

from apache_beam import pvalue
from apache_beam.pipeline import AppliedPTransform
from apache_beam.internal import util
from apache_beam.transforms import core
from pyspark import RDD, SparkContext
from pyspark.broadcast import Broadcast


from .overrides import _Create, _GroupByKey, _CombinePerKey, _ReadFromText


def eval_Create(applied_transform: AppliedPTransform, eval_args: None, sc: SparkContext, side_inputs={}) -> RDD:
    assert eval_args is None, 'Create expects no input'
    items = applied_transform.transform.values
    num_partitions = max(1, math.ceil(math.sqrt(len(items)) / math.sqrt(100)))
    rdd = sc.parallelize(items, num_partitions)
    return rdd

def eval_ReadFromText(applied_transform: AppliedPTransform, eval_args: None, sc: SparkContext, side_inputs={}) -> RDD:
    assert eval_args is None, 'ReadFromText expects no input'
    file_path = applied_transform.transform.values
    rdd = sc.textFile(file_path)
    return rdd

def eval_ParDo(applied_transform: AppliedPTransform, eval_args: List[RDD], sc: SparkContext, side_inputs={}):
    assert len(eval_args) == 1, "ParDo expects input of length 1"
    transform = applied_transform.transform
    rdd = eval_args[0]

    # Handle side inputs
    broadcast_args = []
    for si in applied_transform.side_inputs:
        if isinstance(si, pvalue._UnpickledSideInput):
            view_fn = si._data.view_fn
            broadcast_args.append(sc.broadcast(view_fn(side_inputs[si.pvalue.producer])))
        elif isinstance(si, pvalue.AsSingleton):
            broadcast_args.append(sc.broadcast(side_inputs[si.pvalue.producer][0]))
        elif isinstance(si, pvalue.AsIter) or isinstance(si, pvalue.AsList):
            broadcast_args.append(sc.broadcast(side_inputs[si.pvalue.producer]))
        else:
            raise NotImplementedError("Singleton, Iter, and List side inputs supported here")
    full_args, full_kwargs = util.insert_values_in_args(transform.args, transform.kwargs, broadcast_args)

    def apply_with_side_input(partition_iter):
        # Deserialize broadcast vars
        broadcasted_args = [value.value if isinstance(value, Broadcast) else value for value in full_args]
        broadcasted_kwargs = {key: value.value if isinstance(value, Broadcast) else value for key, value in full_kwargs.items()}

        # Fine. We'll accomodate the bizarre DoFn lifecycle.
        transform.fn.setup()
        transform.fn.start_bundle()
        processed_elements = []

        # Sometimes results are `None`. Sometimes, they are generators.
        for elem in partition_iter:
            elem_result = transform.fn.process(elem, *broadcasted_args, **broadcasted_kwargs)
            if elem_result is None:
                processed_elements.append(elem_result)
            else:
                for processed in elem_result:
                    processed_elements.append(processed)

        # The DoFn lifecycle can also spit out results after things are finished. Don't blame me.
        finish_bundle_generator = transform.fn.finish_bundle()
        if finish_bundle_generator:
            for finish_res in finish_bundle_generator:
                processed_elements.append(finish_res.value)
        transform.fn.teardown()

        return iter(processed_elements)

    return rdd.mapPartitions(apply_with_side_input)

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
    # Coerce the beam way of doing things into spark's preferred merge format
    binary_merge = lambda x, y: combine_fn.merge_accumulators([x, y])
    result_rdd = rdd.aggregateByKey(
        combine_fn.create_accumulator(),
        combine_fn.add_input,
        binary_merge
    ).mapValues(combine_fn.extract_output)
    return result_rdd


def NoOp(applied_transform: AppliedPTransform, eval_args: Any, sc: SparkContext, side_inputs={}):
    return eval_args[0]


evaluator_mapping = {
    _Create: eval_Create,
    _ReadFromText: eval_ReadFromText,
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
