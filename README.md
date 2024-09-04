# PySpark Apache Beam Runner

## Overview

This project introduces a custom Apache Beam runner that leverages PySpark directly. Unlike the default Spark runner shipped with Beam, this runner is designed for environments where a SparkSession is available but a Spark master server is not. This makes it particularly useful for serverless environments where jobs are triggered without a long-running cluster.

### Why Another Spark Runner?

1. **Serverless Compatibility**: Ideal for environments without a dedicated Spark master, supporting execution in serverless frameworks such as EMR Serverless.
2. **Python-Centric Approach**: The entire stack - compilation, optimizations, and execution planning - happens in Python, which can be advantageous for Python-focused teams and environments.
3. **Simplified Setup**: Potentially reduces the complexity of job submission by avoiding the need for port listening on a Spark master.
4. **Direct PySpark Integration**: Utilizes an assumed PySpark SparkSession directly.

## Features

- Direct integration with PySpark
- Serverless compatibility
- Simplified setup process
- Python-centric execution stack
- Support for key Beam transforms (Create, ReadFromText, ParDo, Flatten, GroupByKey, CombinePerKey)
- Efficient handling of side inputs and DoFn lifecycle

## Prerequisites

- Apache Spark
- Apache Beam
- Python 3.8 or later

## Installation

To use this custom runner, simply install it using pip:

```bash
pip install beam-pyspark-runner
```

## Usage

Here's a basic example of how to use the PySpark Beam Runner:

```python
from apache_beam import Pipeline
from apache_beam.options.pipeline_options import PipelineOptions
from beam_pyspark_runner import PySparkRunner

# Define a pipeline with the custom runner implemented here
with Pipeline(runner=PySparkRunner(), options=PipelineOptions()) as p:
    # Your Beam pipeline definition here
    # For example:
    lines = p | 'ReadLines' >> beam.io.ReadFromText('input.txt')
    counts = (lines
              | 'Split' >> beam.Map(lambda x: x.split())
              | 'PairWithOne' >> beam.Map(lambda words: [(w, 1) for w in words])
              | 'GroupAndSum' >> beam.CombinePerKey(sum))
    counts | 'WriteOutput' >> beam.io.WriteToText('output.txt')

# The pipeline will be executed using PySpark
```

## Current Status and Limitations

This project is currently in development. While it supports many common Beam transforms, some advanced features may not be fully implemented. Contributions and feedback are welcome!

## Contributing

We welcome contributions to the PySpark Apache Beam Runner! Here are ways you can contribute:

1. Report bugs or request features by opening an issue
2. Improve documentation
3. Submit pull requests with bug fixes or new features

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Acknowledgements

This project builds upon the great work done by the Apache Beam and Apache Spark communities.
We're grateful for their ongoing efforts in advancing distributed data processing technologies.