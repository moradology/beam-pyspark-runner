import dataclasses
import itertools
import re
import typing as t

import apache_beam
from apache_beam import typehints
from apache_beam.internal.util import ArgumentPlaceholder
from apache_beam.pipeline import AppliedPTransform
from apache_beam.pipeline import PTransformOverride
from apache_beam.transforms import ptransform
from apache_beam.transforms.combiners import _CurriedFn
from apache_beam.transforms.window import GlobalWindows
from apache_beam.io.textio import ReadFromText


K = t.TypeVar("K")
V = t.TypeVar("V")

@dataclasses.dataclass
class _Create(apache_beam.PTransform):
    values: t.Tuple[t.Any]

    def expand(self, input_or_inputs):
        return apache_beam.pvalue.PCollection.from_(input_or_inputs)

    def get_windowing(self, inputs: t.Any) -> apache_beam.Windowing:
        return apache_beam.Windowing(GlobalWindows())
    
    def __post_init__(self):
        self.inputs = []

@dataclasses.dataclass
class _ReadFromText(apache_beam.PTransform):
    values: t.Tuple[t.Any]

    def expand(self, input_or_inputs):
        return apache_beam.pvalue.PCollection.from_(input_or_inputs)

    def get_windowing(self, inputs: t.Any) -> apache_beam.Windowing:
        return apache_beam.Windowing(GlobalWindows())
    
    def __post_init__(self):
        self.inputs = []

@typehints.with_input_types(t.Tuple[K, V])
@typehints.with_output_types(t.Tuple[K, t.Iterable[V]])
class _GroupByKey(apache_beam.PTransform):
  def expand(self, input_or_inputs):
    return apache_beam.pvalue.PCollection.from_(input_or_inputs)

  def infer_output_type(self, input_type):

    key_type, value_type = typehints.trivial_inference.key_value_types(
      input_type
    )
    return typehints.KV[key_type, typehints.Iterable[value_type]]
  
class _CombinePerKey(apache_beam.PTransform):
  """An implementation of CombinePerKey that does mapper-side pre-combining."""
  def __init__(self, combine_fn, args, kwargs):
    args_to_check = itertools.chain(args, kwargs.values())
    if isinstance(combine_fn, _CurriedFn):
      args_to_check = itertools.chain(
          args_to_check, combine_fn.args, combine_fn.kwargs.values())
    if any(isinstance(arg, ArgumentPlaceholder) for arg in args_to_check):
      # This isn't implemented in dataflow either...
      raise NotImplementedError('Deferred CombineFn side inputs.')
    self._combine_fn = apache_beam.transforms.combiners.curry_combine_fn(combine_fn, args, kwargs)

  def expand(self, pcoll):
    return apache_beam.pvalue.PCollection.from_(pcoll)

class CreateOverride(PTransformOverride):
    def matches(self, applied_ptransform: AppliedPTransform) -> bool:
        return isinstance(applied_ptransform.transform, apache_beam.Create)
    
    def get_replacement_transform_for_applied_ptransform(self, applied_ptransform: AppliedPTransform) -> ptransform.PTransform:
        return _Create(applied_ptransform.transform.values)

class ReadFromTextOverride(PTransformOverride):
    def matches(self, applied_ptransform: AppliedPTransform) -> bool:
        return isinstance(applied_ptransform.transform, ReadFromText)
    
    def get_replacement_transform_for_applied_ptransform(self, applied_ptransform: AppliedPTransform) -> ptransform.PTransform:
        return _ReadFromText(applied_ptransform.transform._source.display_data()['file_pattern'].value)
    
class GroupByKeyOverride(PTransformOverride):
    def matches(self, applied_ptransform: AppliedPTransform) -> bool:
        return isinstance(applied_ptransform.transform, apache_beam.GroupByKey)

    def get_replacement_transform_for_applied_ptransform(self, applied_ptransform: AppliedPTransform) -> ptransform.PTransform:
        return _GroupByKey() 

class CombinePerKeyOverride(PTransformOverride):
    def matches(self, applied_ptransform):
        if isinstance(applied_ptransform.transform, apache_beam.CombinePerKey):
            return applied_ptransform.inputs[0].windowing.is_default()

    def get_replacement_transform_for_applied_ptransform(self, applied_ptransform):
        transform = applied_ptransform.transform
        return _CombinePerKey(transform.fn, transform.args, transform.kwargs)

# Order matters here!
pyspark_overrides = [CreateOverride(), ReadFromTextOverride(), CombinePerKeyOverride(), GroupByKeyOverride()]


