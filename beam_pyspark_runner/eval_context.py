from pprint import pprint 

from apache_beam import pvalue
from apache_beam.pipeline import AppliedPTransform

from .pyspark_visitors import EvalContextPipelineVisitor, NodeContext


class EvalContext(object):
    def __init__(self, context_visitor: EvalContextPipelineVisitor):
        self.context_visitor = context_visitor
        self.transform_contexts = context_visitor.ptransforms
        self.paths = []
        #self.collect_all_paths()

    @property
    def leaves(self):
        return [ctx for label, ctx in self.transform_contexts.items() if not self.context_visitor.child_map[label]]

    def get_node_context(self, label: str) -> NodeContext:
        return self.context_visitor.ptransforms[label]

    def print_full_graph(self):
        print("===========================")
        print("ALL APPLIED TRANSFORMS")
        print("===========================")
        pprint({k: v.as_dict() for k, v in self.transform_contexts.items()})
        print("===========================")
