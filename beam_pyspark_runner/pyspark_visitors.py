import dataclasses
from typing import Dict, List, Optional, Union

from apache_beam import pvalue
from apache_beam.pipeline import PipelineVisitor, AppliedPTransform
from apache_beam.transforms.external import ExternalTransform


class VerifyNoCrossLanguageTransforms(PipelineVisitor):
    def visit_transform(self, applied_ptransform):
        if isinstance(applied_ptransform.transform, ExternalTransform):
            raise RuntimeError(
                "Batch pyspark runner "
                "does not support cross-language pipelines.")


@dataclasses.dataclass
class NodeContext:
    type: type
    applied_transform: AppliedPTransform
    inputs: List[pvalue.PValue]
    side_inputs: List[pvalue.AsSideInput]
    outputs: Dict[Union[str, int, None], pvalue.PValue]
    parent: Optional[str]
    input_producer_labels: List[str]

    @property
    def is_root(self):
        return len(self.inputs) == 0

    def as_dict(self):
        return {
            "type": self.type,
            "applied_transform": self.applied_transform,
            "inputs": self.inputs,
            "side_inputs": self.side_inputs,
            "outputs": self.outputs,
            "parent": self.parent,
            "input_producer_labels": self.input_producer_labels
        }

class EvalContextPipelineVisitor(PipelineVisitor):
    """For internal use only; no backwards-compatibility guarantees."""
    def __init__(self):
        # all ptransforms with relevant context exposed
        self.ptransforms = {}
        # Root to leaf paths in this pipeline
        self.paths = []
        # Map transform labels to a list of their children's labels
        self.child_map = {}

    def visit_transform(self, applied_ptransform: AppliedPTransform) -> None:
        transform_label = applied_ptransform.full_label
        inputs = [input for input in applied_ptransform.inputs if not isinstance(input, pvalue.PBegin)]
        side_inputs = [si for si in applied_ptransform.side_inputs]
        outputs = [output for output in applied_ptransform.outputs.values()]
        parent = applied_ptransform.parent.full_label
        print(applied_ptransform.full_label)
        print(applied_ptransform.inputs)
        input_producer_labels = [input.producer.full_label for input in applied_ptransform.inputs if input.producer is not None]
    
        self.ptransforms[transform_label] = NodeContext(
            type=type(applied_ptransform.transform).__name__,
            applied_transform=applied_ptransform,
            inputs=inputs,
            side_inputs=side_inputs,
            outputs=outputs,
            parent=parent,
            input_producer_labels = input_producer_labels
        )

        # In service of building up dag paths
        self.child_map.setdefault(transform_label, [])
        for producer_label in input_producer_labels:
            self.child_map.setdefault(producer_label, []).append(transform_label)

