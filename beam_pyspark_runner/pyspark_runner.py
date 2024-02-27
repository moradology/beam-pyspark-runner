import dataclasses
from typing import Dict, Sequence, Optional

from pyspark.rdd import RDD
from pyspark.sql import SparkSession

from apache_beam.pipeline import AppliedPTransform
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners.direct.direct_runner import BundleBasedDirectRunner
from apache_beam.runners.runner import PipelineResult
from apache_beam.runners.runner import PipelineState
from apache_beam.pipeline import PipelineVisitor
from apache_beam import pvalue

from .transform_evaluator import NoOp, TRANSLATIONS
from .overrides import pyspark_overrides


class PySparkOptions(PipelineOptions):
    """TODO: Any spark-specific options we want to include"""
    @classmethod
    def _add_argparse_args(cls, parser):
        # Add an argument for specifying the application name
        parser.add_value_provider_argument('--application_name',
                                           type=str,
                                           help='Name of the PySpark application',
                                           default='BeamPySparkApp')


@dataclasses.dataclass
class PySparkRunnerResult(PipelineResult):

    def __post_init__(self):
        super().__init__(PipelineState.RUNNING)

    def wait_until_finish(self, duration=None) -> str:
        try:
            # lineages = [rdd.toDebugString() for rdd in self.rdds]
            # results = [rdd.collect() for rdd in self.rdds]
            # print("START LINEAGE")
            # for idx in range(len(lineages)):
            #     print("\n\n")
            #     print(idx)
            #     print(results[idx])
            #     print("============")
            #     print(lineages[idx].decode())
            # print("END LINEAGE")
            # self.stop_session()
            # print(f"FINAL RESULTS {results}")
            self._state = PipelineState.DONE
        except:  # pylint: disable=broad-except
            self._state = PipelineState.FAILED
            raise
        return self._state

    def cancel(self) -> str:
        self._state = PipelineState.CANCELLING
        self._state = PipelineState.CANCELLED
        return self._state

    def metrics(self):
        raise NotImplementedError('collecting metrics will come later!')

import pprint
class PySparkRunner(BundleBasedDirectRunner):
    """Executes a pipeline using Apache Spark."""
    
    @staticmethod
    def to_spark_rdd_visitor(spark: SparkSession):
        class SparkRDDVisitor(PipelineVisitor):
            def __init__(self):
                self.rdds: Dict[AppliedPTransform, RDD] = {}
                self.last_rdd: Optional[RDD]
                
            def visit_transform(self, transform_node):
                op_class = TRANSLATIONS.get(transform_node.transform.__class__, NoOp)
                # print(f"{transform_node.transform.__class__} -> {op_class}")
                op = op_class(transform_node, spark)
                    
                inputs = list(transform_node.inputs)

                if inputs:
                    rdd_inputs = []
                    for input_value in inputs:
                        if isinstance(input_value, pvalue.PBegin):
                            rdd_inputs.append(None)

                        prev_op = input_value.producer
                        if prev_op in self.rdds:
                            rdd_inputs.append(self.rdds[prev_op])
                    # print("IN VISITOR")
                    # print(transform_node)
                    # pprint.pprint(self.rdds)
                    if len(rdd_inputs) == 1:
                        self.rdds[transform_node] = op.apply(rdd_inputs[0])
                        self.last_rdd = op.apply(rdd_inputs[0])
                    else:
                        self.rdds[transform_node] = op.apply(rdd_inputs)
                        self.last_rdd = op.apply(rdd_inputs)
                
                else:
                    self.rdds[transform_node] = op.apply(None)
                    self.last_rdd = op.apply(None)
                        
        return SparkRDDVisitor()

    def run_pipeline(self, pipeline, options):
        pyspark_options = options.view_as(PySparkOptions)
        application_name = pyspark_options.application_name
        spark = SparkSession.builder.appName(application_name).getOrCreate()
        
        spark_visitor = self.to_spark_rdd_visitor(spark)
        pipeline.replace_all(pyspark_overrides())
        pipeline.visit(spark_visitor)
        
        # Trigger actions to compute the RDDs
        # print("RDDS")
        # for trans, rdd in spark_visitor.rdds.items():
        #     print(f"TRANS: {trans}")
        #     print(f"RDD: {rdd}")
        rdds = spark_visitor.rdds.values()
        # [rdd.collect() for rdd in rdds]
        spark_visitor.last_rdd.collect()
        spark.stop()
        
        return PySparkRunnerResult()