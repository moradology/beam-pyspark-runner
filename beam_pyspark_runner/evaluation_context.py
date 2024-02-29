import collections
import threading
from typing import TYPE_CHECKING
from typing import Any
from typing import DefaultDict
from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

from apache_beam.transforms import sideinputs
from apache_beam.utils import counters

class EvaluationContext(object):
    """Evaluation context with the global state information of the pipeline.

    The evaluation context for a specific pipeline being executed by the
    PySparkRunner. Contains state shared within the execution across all
    transforms.

    EvaluationContext contains shared state for an execution of the
    PySparkRunner that can be used while evaluating a PTransform. This
    consists of views into underlying state and watermark implementations, access
    to read and write side inputs, and constructing counter sets and
    execution contexts. This includes executing callbacks asynchronously when
    state changes to the appropriate point (e.g. when a side input is
    requested and known to be empty).
    """

    def __init__(self,
                pipeline_options,
                root_transforms,
                step_names,
                views,  # type: Iterable[pvalue.AsSideInput]
                ):
        self.pipeline_options = pipeline_options
        self._root_transforms = root_transforms
        self._step_names = step_names
        self.views = views
        self._pcollection_to_views = collections.defaultdict(
            list)  # type: DefaultDict[pvalue.PValue, List[pvalue.AsSideInput]]
        for view in views:
            self._pcollection_to_views[view.pvalue].append(view)

    def is_root_transform(self, applied_ptransform):
        # type: (AppliedPTransform) -> bool
        return applied_ptransform in self._root_transforms
  
    def print_context(self):
        from pprint import pprint
        print("EVALUATION CONTEXT")
        print("\n=========================\n")
        print("ROOT TRANSFORMS")
        pprint(self._root_transforms)
        print("\n=========================\n")
        print("PCOLLECTION -> VIEWS")
        pprint(self._pcollection_to_views)
        print("\n=========================\n")
        print("PVALUE -> PVALUE-CONSUMERS")
        pprint(self._value_to_consumers)
        print("\n=========================\n")
        print("STEP NAMES")
        pprint(self._step_names)
