import dataclasses
import typing as t

import apache_beam
from apache_beam import typehints
from apache_beam.io.iobase import SourceBase
from apache_beam.pipeline import AppliedPTransform
from apache_beam.pipeline import PTransformOverride
from apache_beam.runners.direct.direct_runner import _GroupAlsoByWindowDoFn
from apache_beam.transforms import ptransform
from apache_beam.transforms.window import GlobalWindows

K = t.TypeVar("K")
V = t.TypeVar("V")


@dataclasses.dataclass
class _Create(apache_beam.PTransform):
    values: t.Tuple[t.Any]

    def expand(self, input_or_inputs):
        return apache_beam.pvalue.PCollection.from_(input_or_inputs)

    def get_windowing(self, inputs: t.Any) -> apache_beam.Windowing:
        return apache_beam.Windowing(GlobalWindows())
    
@typehints.with_input_types(K)
@typehints.with_output_types(K)
class _Reshuffle(apache_beam.PTransform):
    def expand(self, input_or_inputs):
        return apache_beam.pvalue.PCollection.from_(input_or_inputs)

@dataclasses.dataclass
class _Read(apache_beam.PTransform):
  source: SourceBase

  def expand(self, input_or_inputs):
    return apache_beam.pvalue.PCollection.from_(input_or_inputs)

@typehints.with_input_types(t.Tuple[K, V])
@typehints.with_output_types(t.Tuple[K, t.Iterable[V]])
class _GroupByKeyOnly(apache_beam.PTransform):
    def expand(self, input_or_inputs):
        return apache_beam.pvalue.PCollection.from_(input_or_inputs)

    def infer_output_type(self, input_type):
        key_type, value_type = typehints.trivial_inference.key_value_types(
            input_type
        )
        return typehints.KV[key_type, typehints.Iterable[value_type]]


@typehints.with_input_types(t.Tuple[K, t.Iterable[V]])
@typehints.with_output_types(t.Tuple[K, t.Iterable[V]])
class _GroupAlsoByWindow(apache_beam.ParDo):
    """Not used yet..."""
    def __init__(self, windowing):
        super().__init__(_GroupAlsoByWindowDoFn(windowing))
        self.windowing = windowing

    def expand(self, input_or_inputs):
        return apache_beam.pvalue.PCollection.from_(input_or_inputs)


@typehints.with_input_types(t.Tuple[K, V])
@typehints.with_output_types(t.Tuple[K, t.Iterable[V]])
class _GroupByKey(apache_beam.PTransform):
    def expand(self, input_or_inputs):
        return input_or_inputs | "GroupByKey" >> _GroupByKeyOnly()

class _Flatten(apache_beam.PTransform):
    def expand(self, input_or_inputs):
        is_bounded = all(pcoll.is_bounded for pcoll in input_or_inputs)
        return apache_beam.pvalue.PCollection(self.pipeline, is_bounded=is_bounded)


def pyspark_overrides() -> t.List[PTransformOverride]:
    class CreateOverride(PTransformOverride):
        def matches(self, applied_ptransform: AppliedPTransform) -> bool:
            return applied_ptransform.transform.__class__ == apache_beam.Create
        
        def get_replacement_transform_for_applied_ptransform(self, applied_ptransform: AppliedPTransform) -> ptransform.PTransform:
            return _Create(t.cast(apache_beam.Create, applied_ptransform.transform).values)
        
    class ReshuffleOverride(PTransformOverride):
        def matches(self, applied_ptransform: AppliedPTransform) -> bool:
            return applied_ptransform.transform.__class__ == apache_beam.Reshuffle

        def get_replacement_transform_for_applied_ptransform(self, applied_ptransform: AppliedPTransform) -> ptransform.PTransform:
            return _Reshuffle()
        
    class ReadOverride(PTransformOverride):
        def matches(self, applied_ptransform: AppliedPTransform) -> bool:
            return applied_ptransform.transform.__class__ == apache_beam.io.Read

        def get_replacement_transform_for_applied_ptransform(
            self, applied_ptransform: AppliedPTransform) -> ptransform.PTransform:
            return _Read(t.cast(apache_beam.io.Read, applied_ptransform.transform).source)
        
    class GroupByKeyOverride(PTransformOverride):
        def matches(self, applied_ptransform: AppliedPTransform) -> bool:
            return applied_ptransform.transform.__class__ == apache_beam.GroupByKey

        def get_replacement_transform_for_applied_ptransform(self, applied_ptransform: AppliedPTransform) -> ptransform.PTransform:
            return _GroupByKey()
        
    class FlattenOverride(PTransformOverride):
        def matches(self, applied_ptransform: AppliedPTransform) -> bool:
            return applied_ptransform.transform.__class__ == apache_beam.Flatten

        def get_replacement_transform_for_applied_ptransform(self, applied_ptransform: AppliedPTransform) -> ptransform.PTransform:
            return _Flatten()
    
    return [CreateOverride(), ReshuffleOverride(), ReadOverride(), GroupByKeyOverride(), FlattenOverride()]