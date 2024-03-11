from pprint import pprint 

from apache_beam.pipeline import AppliedPTransform

from .pyspark_visitors import EvalContextPipelineVisitor


def applied_ptransform_context(applied_ptransform: AppliedPTransform):
    return {
        "type": type(applied_ptransform.transform).__name__,
        "inputs": applied_ptransform.inputs,
        "side_inputs": applied_ptransform.side_inputs,
        "outputs": applied_ptransform.outputs.values(),
        "parent": applied_ptransform.parent.full_label,
        "input_producers": [input.producer for input in applied_ptransform.inputs if input.producer is not None],
        "input_producer_labels": [input.producer.full_label for input in applied_ptransform.inputs if input.producer is not None]
    }


class EvalContext(object):
    def __init__(self, context_visitor: EvalContextPipelineVisitor):
        self.context_visitor = context_visitor
        self.applied_ptransforms = context_visitor.applied_ptransforms
        self.child_map = context_visitor.child_map
        self.producer_map = self.create_producer_map(self.child_map)
        self.nodes_to_cache = self.get_nodes_to_cache(self.child_map)

    @property
    def leaves(self):
        return [aptrans for label, aptrans in self.applied_ptransforms.items() if not self.context_visitor.child_map[label]]

    def get_node_for_label(self, label: str) -> AppliedPTransform:
        return self.context_visitor.ptransforms[label]

    def create_producer_map(self, child_map):
        # Iterate over child_map to populate producer_map
        producer_map = {}
        for parent, children in child_map.items():
            for child in children:
                producer_map.setdefault(child, []).append(parent)
        return producer_map

    def get_nodes_to_cache(self, child_map):
        nodes_with_multiple_children = [node for node, children in child_map.items() if len(children) > 1]
        return nodes_with_multiple_children

    def print_node_contexts(self):
        print("===========================")
        print("ALL APPLIED TRANSFORMS")
        print("===========================")
        pprint({full_label: applied_ptransform_context(applied_ptransform) for full_label, applied_ptransform in self.applied_ptransforms.items()})
        print("===========================")
