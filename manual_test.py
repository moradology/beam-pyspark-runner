#!/usr/bin/env python
from beam_pyspark_runner.pyspark_runner import PySparkRunner

import apache_beam as beam

class UseMultimap(beam.DoFn):
    def process(self, element, side_input_multimap):
        # 'element' could be a key to look up in the multimap 'side_input_multimap'
        # This example assumes each key maps to a list of values
        values_for_key = side_input_multimap.get(element, [])
        for value in values_for_key:
            yield f"{element}: {value}"

with beam.Pipeline(runner=PySparkRunner()) as p:
    side_input_multimap = p | "Create Side Input" >> beam.Create([("key1", ["value1a", "value1b"]), ("key2", ["value2a"])])
    side_input_dict = beam.pvalue.AsDict(side_input_multimap)

    results = (
        p | "Create Elements" >> beam.Create(["key1", "key2"])
        | "Use Multimap" >> beam.ParDo(UseMultimap(), side_input_multimap=side_input_dict)
        | beam.Map(print)
    )

