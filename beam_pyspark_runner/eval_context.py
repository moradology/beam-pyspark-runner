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


    # def find_roots(self):
    #     return [label for label, ctx in self.transform_contexts.items() if ctx.is_root]

    # def find_paths(self, start_label, path, all_paths):
    #     path.append(start_label)
    #     if start_label not in self.context_visitor.child_map:  # Leaf node
    #         all_paths.append(list(path))
    #     else:
    #         for child_label in self.context_visitor.child_map[start_label]:
    #             self.find_paths(child_label, path, all_paths)
    #     path.pop()

    # def collect_all_paths(self):
    #     if len(self.paths) > 0:
    #         return self.paths
    #     else:
    #         roots = self.find_roots()
    #         for root in roots:
    #             self.find_paths(root, [], self.paths)
        
    # def collect_all_paths(self):
    #     if len(self.paths) > 0:
    #         return self.paths
    #     else:
    #         leaf_ctxs = [ctx for label, ctx in self.transform_contexts.keys() if label not in self.context_visitor.child_map]
    #         paths = []
    #         for leaf_ctx in leaf_ctxs:
    #             next_node = leaf_ctx
    #             path = []
    #             while next_node:
    #                 path.insert(0, current_node.applied_transform.full_label)
    #                 parent_contexts = list(map(lambda label: self.transform_contexts[label], leaf_ctx.input_producer_labels))


    def print_all_paths(self):
        print("===========================")
        print("ALL DAG PATHS")
        print("===========================")
        pprint(self.paths)
        print("===========================")