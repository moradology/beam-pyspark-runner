from apache_beam.pipeline import PipelineVisitor, AppliedPTransform
from apache_beam.transforms.external import ExternalTransform


class VerifyNoCrossLanguageTransforms(PipelineVisitor):
    def visit_transform(self, applied_ptransform):
        if isinstance(applied_ptransform.transform, ExternalTransform):
            raise RuntimeError(
                "Batch pyspark runner "
                "does not support cross-language pipelines.")


class EvalContextPipelineVisitor(PipelineVisitor):
    """For internal use only; no backwards-compatibility guarantees."""
    def __init__(self):
        # all ptransforms with relevant context exposed
        self.applied_ptransforms = {}
        # Map transform labels to a list of their children's labels
        self.child_map = {}
        # Map transform labels to a list of their parent's labels
        self.producer_map = {}

    def visit_transform(self, applied_ptransform: AppliedPTransform) -> None:
        # self.applied_ptransforms
        transform_label = applied_ptransform.full_label
        self.applied_ptransforms[transform_label] = applied_ptransform

        # self.child_map
        input_producer_labels = [input.producer.full_label for input in applied_ptransform.inputs if input.producer is not None]
        self.child_map.setdefault(transform_label, [])
        for producer_label in input_producer_labels:
            self.child_map.setdefault(producer_label, []).append(transform_label)
