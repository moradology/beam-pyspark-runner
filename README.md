# PySpark Apache Beam Runner

## Overview
(WHY? Doesn't Beam ship with a Spark runner?)

This project introduces a custom Apache Beam runner that leverages PySpark directly.
This is not a 'portability' framework compliant runner! It is designed for environments
where a SparkSession is available but a Spark master server is not. This is useful for
e.g. serverless environments where jobs are triggered without a long-running cluster,
sidestepping the expectations of Beam's default Spark runner.

The other benefit is that this strategy for building a runner helps to keep the stack as
python-centric as possible. The compilation process, the optimizations, the execution
planning - these all happen in python (for better or worse). Depending on your needs,
this might be a significant advantage.

## Features
- **Direct Integration with PySpark**: Utilizes a PySpark  assumed SparkSession directly.
- **Serverless Compatibility**: Ideal for environments without a dedicated Spark master, supporting execution in serverless frameworks.
- **Simplified Setup**: Potentially reduces the complexity of job submission by avoiding the need for port listening on a Spark master.

## Getting Started

### Prerequisites
- Apache Spark
- Apache Beam
- Python 3.8 or later

### Installation
To use this custom runner, just `pip install` as you would any library

```bash
pip install beam-pyspark-runner
```
