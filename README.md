![logo](ldbc-logo.png)

# FinBench DataGen

![Build status](https://github.com/ldbc/ldbc_finbench_datagen/actions/workflows/ci.yml/badge.svg?branch=main)

The LDBC FinBench Data Generator (Datagen) produces the datasets for the [LDBC FinBench's workloads](https://ldbcouncil.org/benchmarks/finbench/). 

This data generator produces labelled directed property graphs based on the simulation of financial activities in business systems. The key features include generation, factorization, and transformation. A detailed description of the schema produced by Datagen, as well as the format of the output files, can be found in the latest version of the official LDBC FinBench specification document.

Related Repositories:
- FinBench Specification: https://github.com/ldbc/ldbc_finbench_docs
- FinBench Driver: https://github.com/ldbc/ldbc_finbench_driver
- FinBench Reference Implementation: https://github.com/ldbc/ldbc_finbench_transaction_impls
- FinBench ACID Suite: https://github.com/ldbc/finbench-acid

**Note: The main branch is a work-in-progress for upcoming `v0.2` release aiming scale larger than SF100. For the stable version, please refer to version `0.1.0` on the `v0.1.0` branch.**

## DataGen Design

### Data Schema

![Schema](./data-schema.png)

### Implementation

- Generation: Generation simulates financial activities in business systems to produce the raw data.
- Factorization: Factorization profiles of the raw data to produce factor tables used for further parameter curation.
- Transformation: Transformation transforms the raw data to the data for SUT and benchmark driver.

Note:

- Generation and Factorization are implemented in Scala while transformation is implemented in Python
  under `transformation/`.
- SUT stands for System Under Test.

## Quick Start

### Pre-requisites

- Java 8 installed.
- Python3 and related packages installed. See each `install-dependencies.sh` for details.
- Scala 2.12, note that it will be integrated when Maven builds.
- Spark deployed. Spark 3.2.x is the recommended runtime to use. The rest of the instructions are provided assuming
  Spark 3.2.x.

### Workflow

- Use the Spark application to generate the factor tables and raw data.
- Use the Python scripts to transform the data into snapshot data and write queries.

### Generation of Raw Data

- Deploy Spark
    - Use `scripts/get-spark-to-home.sh` to download pre-built Spark to the home directory and then decompress it.
    - Set the PATH environment variable to include the Spark binaries.
- Build the project
    - run `mvn clean package -DskipTests` to package the artifacts.
- Run locally with scripts
    - See `scripts/run_local.sh` for details. It uses spark-submit to run the data generator. Please ensure that you have the prerequisites installed and that the build is successful.
- Run in cloud: To be supported
- Run in cluster: To be supported

### Transformation of Raw Data

- Set the `${FinBench_DATA_ROOT}` variable in `transformation/transform.sh` and run.

 
