import itertools

from apache_beam import pvalue
from apache_beam.transforms import core
from .evaluation_context import EvaluationContext

class _TransformEvaluator(object):
    """An evaluator of a specific application of a transform."""
    def __init__(self,
                evaluation_context, # type: EvaluationContext
                applied_ptransform,  # type: AppliedPTransform
                input_committed_bundle,
                side_inputs):
        self._evaluation_context = evaluation_context
        self._applied_ptransform = applied_ptransform
        self._input_committed_bundle = input_committed_bundle
        self._side_inputs = side_inputs
        self._expand_outputs()
        self._execution_context = evaluation_context.get_execution_context(
            applied_ptransform)
        self._step_context = self._execution_context.get_step_context()

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
  """TransformEvaluator for Impulse transform."""
  def finish_bundle(self):
    assert len(self._outputs) == 1
    output_pcollection = list(self._outputs)[0]
    bundle = self._evaluation_context.create_bundle(output_pcollection)
    bundle.output(GlobalWindows.windowed_value(b''))
    return TransformResult(self, [bundle], [], None, None)

class TransformEvaluatorRegistry(object):
  """For internal use only; no backwards-compatibility guarantees.

  Creates instances of TransformEvaluator for the application of a transform.
  """

  _test_evaluators_overrides = {
  }  # type: Dict[Type[core.PTransform], Type[_TransformEvaluator]]

  def __init__(self, evaluation_context):
    # type: (EvaluationContext) -> None
    assert evaluation_context
    self._evaluation_context = evaluation_context
    self._evaluators = {
        core.Impulse: _ImpulseEvaluator,
    }  # type: Dict[Type[core.PTransform], Type[_TransformEvaluator]]
    self._evaluators.update(self._test_evaluators_overrides)

  def get_evaluator(
      self, applied_ptransform, input_committed_bundle, side_inputs):
    """Returns a TransformEvaluator suitable for processing given inputs."""
    assert applied_ptransform
    assert bool(applied_ptransform.side_inputs) == bool(side_inputs)

    # Walk up the class hierarchy to find an evaluable type. This is necessary
    # for supporting sub-classes of core transforms.
    for cls in applied_ptransform.transform.__class__.mro():
      evaluator = self._evaluators.get(cls)
      if evaluator:
        break

    if not evaluator:
      raise NotImplementedError(
          'Execution of [%s] not implemented in runner %s.' %
          (type(applied_ptransform.transform), self))
    return evaluator(
        self._evaluation_context,
        applied_ptransform,
        input_committed_bundle,
        side_inputs)

  def get_root_bundle_provider(self, applied_ptransform):
    provider_cls = None
    for cls in applied_ptransform.transform.__class__.mro():
      provider_cls = self._root_bundle_providers.get(cls)
      if provider_cls:
        break
    if not provider_cls:
      raise NotImplementedError(
          'Root provider for [%s] not implemented in runner %s' %
          (type(applied_ptransform.transform), self))
    return provider_cls(self._evaluation_context, applied_ptransform)

class Evaluator(object):
    def __init__(
        self,
        value_to_consumers,
        transform_evaluator_registry,
        evaluation_context  # type: EvaluationContext
    ):
        self.value_to_consumers = value_to_consumers
        self.transform_evaluator_registry = transform_evaluator_registry
        self.evaluation_context = evaluation_context

    def evaluate(self, roots):
        self.root_nodes = frozenset(roots)
        self.all_nodes = frozenset(
            itertools.chain(
                roots,
                *itertools.chain(self.value_to_consumers.values())
            )
        )
        self.leaves = frozenset(

        )
        print("ROOT NODES")
        print(self.root_nodes)

        print("ALL NODES")
        print(self.all_nodes)

        print("OTHER THINGs")
        print(self.value_to_consumers.keys())
        print("OTHER THINGs")
        print(self.value_to_consumers)