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
    
    def __post_init__(self):
        self.inputs = []
    
def pyspark_overrides() -> t.List[PTransformOverride]:
    class CreateOverride(PTransformOverride):
        def matches(self, applied_ptransform: AppliedPTransform) -> bool:
            return applied_ptransform.transform.__class__ == apache_beam.Create
        
        def get_replacement_transform_for_applied_ptransform(self, applied_ptransform: AppliedPTransform) -> ptransform.PTransform:
            return _Create(t.cast(apache_beam.Create, applied_ptransform.transform).values)
        
    return [CreateOverride()]