from dataclasses import dataclass
from typing import List

from apache_beam import pvalue
from apache_beam.pipeline import AppliedPTransform

@dataclass 
class PysparkStage:
    terminal_node: AppliedPTransform
    side_input_dependencies: set

    def __eq__(self, other):
        if not isinstance(other, PysparkStage):
            return NotImplemented

        return self.terminal_node == other.terminal_node

    def __hash__(self):
        return hash(self.terminal_node)

    @classmethod
    def from_node(cls, node: AppliedPTransform):
        def find_side_input_dependencies(node, current_deps=set()):
            for si in node.side_inputs:
                current_deps.add(si.pvalue.producer)
            if not node.inputs:
                return current_deps
            else:
                upstream_dependencies = set()
                for pval in node.inputs:
                    if isinstance(pval, pvalue.PBegin):
                        continue
                    producer_node = pval.producer
                    upstream_dependencies  = upstream_dependencies.union(find_side_input_dependencies(producer_node, current_deps))
                return current_deps.union(upstream_dependencies)
        return PysparkStage(node, find_side_input_dependencies(node))

@dataclass
class PysparkPlan:
    stages: List[PysparkStage]

    def topologically_ordered(self):
        in_degree = {stage: len(stage.side_input_dependencies) for stage in self.stages}
        ordered_stages = []
        queue = [stage for stage in self.stages if in_degree[stage] == 0]

        while queue:
            stage = queue.pop(0)
            ordered_stages.append(stage)

            for potential_dependent in self.stages:
                if stage.terminal_node in potential_dependent.side_input_dependencies:
                    # Reduce in-degree and enqueue if it becomes 0
                    in_degree[potential_dependent] -= 1
                    if in_degree[potential_dependent] == 0:
                        queue.append(potential_dependent)

        if len(ordered_stages) == len(self.stages):
            return ordered_stages
        else:
            raise ValueError("A cycle was detected in the graph, or there are unresolved dependencies.")
        
    @property
    def all_dependencies(self):
        return set().union(*[stage.side_input_dependencies for stage in self.stages])
