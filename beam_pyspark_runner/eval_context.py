from pprint import pprint 

from apache_beam import pvalue
from apache_beam.pipeline import AppliedPTransform

from .pyspark_visitors import EvalContextPipelineVisitor, NodeContext


class EvalContext(object):
    def __init__(self, context_visitor: EvalContextPipelineVisitor):
        self.context_visitor = context_visitor
        self.transform_contexts = context_visitor.ptransforms
        self.paths = []
        self.collect_all_paths()

    def get_node_context(self, label: str) -> NodeContext:
        return self.context_visitor.ptransforms[label]

    def print_full_graph(self):
        print("===========================")
        print("ALL APPLIED TRANSFORMS")
        print("===========================")
        pprint({k: v.as_dict() for k, v in self.transform_contexts.items()})
        print("===========================")

    def find_roots(self):
        return [label for label, ctx in self.transform_contexts.items() if ctx.is_root]

    def find_paths(self, start_label, path, all_paths):
        path.append(start_label)
        if start_label not in self.context_visitor.child_map or not self.context_visitor.child_map[start_label]:  # Leaf node
            all_paths.append(list(path))
        else:
            for child_label in self.context_visitor.child_map[start_label]:
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
        print("===========================")
        print("ALL DAG PATHS")
        print("===========================")
        pprint(self.paths)
        print("===========================")