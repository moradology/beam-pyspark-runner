from apache_beam import pvalue
from apache_beam.pipeline import PipelineVisitor
from apache_beam.transforms.external import ExternalTransform


class VerifyNoCrossLanguageTransforms(PipelineVisitor):
    def visit_transform(self, applied_ptransform):
        if isinstance(applied_ptransform.transform, ExternalTransform):
            raise RuntimeError(
                "Batch pyspark runner "
                "does not support cross-language pipelines.")


class ConsumerTrackingPipelineVisitor(PipelineVisitor):
    """For internal use only; no backwards-compatibility guarantees.

    Visitor for extracting value-consumer relations from the graph.

    Tracks the AppliedPTransforms that consume each PValue in the Pipeline. This
    is used to schedule consuming PTransforms to consume input after the upstream
    transform has produced and committed output.
    """
    def __init__(self):

        self.value_to_consumers = {}  # type: Dict[pvalue.PValue, Set[AppliedPTransform]]
        self.root_transforms = set()  # type: Set[AppliedPTransform]
        self.all_transforms = set()
        self.step_names = {}  # type: Dict[AppliedPTransform, str]

        self._num_transforms = 0
        self._views = set()

    @property
    def views(self):
        """Returns a list of side intputs extracted from the graph.

        Returns:
        A list of pvalue.AsSideInput.
        """
        return list(self._views)

    def visit_transform(self, applied_ptransform):
        # type: (AppliedPTransform) -> None
        print("APPLIED_PTRANS", applied_ptransform)
        self.all_transforms.add(applied_ptransform)
        inputs = list(applied_ptransform.inputs)

        if inputs:
            for input_value in inputs:
                if isinstance(input_value, pvalue.PBegin):
                    self.root_transforms.add(applied_ptransform)
                if input_value not in self.value_to_consumers:
                    self.value_to_consumers[input_value] = set()
                self.value_to_consumers[input_value].add(applied_ptransform)
        else:
            self.root_transforms.add(applied_ptransform)
            self.step_names[applied_ptransform] = 's%d' % (self._num_transforms)
            self._num_transforms += 1

        for side_input in applied_ptransform.side_inputs:
            self._views.add(side_input)


# @staticmethod
# def to_spark_rdd_visitor(spark: SparkSession):
#     class SparkRDDVisitor(PipelineVisitor):
#         def __init__(self):
#             self.rdds: Dict[AppliedPTransform, RDD] = {}
#             self.last_rdd: Optional[RDD]
            
#         def visit_transform(self, transform_node):
#             op_class = TRANSLATIONS.get(transform_node.transform.__class__, NoOp)
#             # print(f"{transform_node.transform.__class__} -> {op_class}")
#             op = op_class(transform_node, spark)
                
#             inputs = list(transform_node.inputs)

#             if inputs:
#                 rdd_inputs = []
#                 for input_value in inputs:
#                     if isinstance(input_value, pvalue.PBegin):
#                         rdd_inputs.append(None)

#                     prev_op = input_value.producer
#                     if prev_op in self.rdds:
#                         rdd_inputs.append(self.rdds[prev_op])
#                 if len(rdd_inputs) == 1:
#                     self.rdds[transform_node] = op.apply(rdd_inputs[0])
#                     self.last_rdd = op.apply(rdd_inputs[0])
#                 else:
#                     self.rdds[transform_node] = op.apply(rdd_inputs)
#                     self.last_rdd = op.apply(rdd_inputs)
            
#             else:
#                 self.rdds[transform_node] = op.apply(None)
#                 self.last_rdd = op.apply(None)
                    
#     return SparkRDDVisitor()