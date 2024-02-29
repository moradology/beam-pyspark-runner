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
        inputs = [input._str_internal() for input in applied_ptransform.inputs]
        side_inputs = [si for si in applied_ptransform.side_inputs]
        outputs = [output for output in applied_ptransform.outputs.values()]
        parent = applied_ptransform.parent.full_label
        input_producer_labels = [input.producer.full_label for input in applied_ptransform.inputs]
    
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

    def print_full_graph(self):
        from pprint import pprint
        print("===========================")
        print("ALL APPLIED TRANSFORMS")
        print("===========================")
        pprint({k: v.as_dict() for k, v in self.ptransforms.items()})
        print("===========================")

    def find_roots(self):
        return [label for label, ctx in self.ptransforms.items() if not ctx.inputs]

    def find_paths(self, start_label, path, all_paths):
        path.append(start_label)
        if start_label not in self.child_map or not self.child_map[start_label]:  # Leaf node
            all_paths.append(list(path))
        else:
            for child_label in self.child_map[start_label]:
                self.find_paths(child_label, path, all_paths)
        path.pop()

    def collect_all_paths(self):
        if len(self.paths) > 0:
            return self.paths
        else:
            roots = self.find_roots()
            for root in roots:
                self.find_paths(root, [], self.paths)

    def print_all_paths(self):
        from pprint import pprint
        print("===========================")
        print("ALL DAG PATHS")
        print("===========================")
        pprint(self.paths)
        print("===========================")

