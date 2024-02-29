import itertools

from apache_beam import pvalue
from apache_beam.pipeline import AppliedPTransform
from apache_beam.transforms import core
from .evaluation_context import EvaluationContext


class _TransformEvaluator(object):
    """An evaluator of a specific application of a transform."""
    def __init__(self,
                evaluation_context, # type: EvaluationContext
                applied_ptransform):  # type: AppliedPTransform
        self._evaluation_context = evaluation_context
        self._applied_ptransform = applied_ptransform
        self._transform = self._applied_ptransform.transform
        self._expand_outputs()

    def apply(self, input):
       pass

    def _expand_outputs(self):
        outputs = set()
        for pval in self._applied_ptransform.outputs.values():
            if isinstance(pval, pvalue.DoOutputsTuple):
                pvals = (v for v in pval)
            else:
                pvals = (pval, )
            for v in pvals:
                outputs.add(v)
        self._outputs = frozenset(outputs)

class _ImpulseEvaluator(_TransformEvaluator):
    pass

class Evaluator(object):
    def __init__(
        self,
        value_to_consumers,
        evaluation_context  # type: EvaluationContext
    ):
        self.value_to_consumers = value_to_consumers
        self.evaluation_context = evaluation_context

    def evaluate(self, roots):
        self.root_nodes = frozenset(roots)
        self.all_nodes = frozenset(
            itertools.chain(
                roots,
                *itertools.chain(self.value_to_consumers.values())
            )
        )
