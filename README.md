# SQLBench

This repository contains core files of the SQLBench. Technical description of the benchmark structure can be found in [BenchmarkStructure.pdf](https://github.com/sqlbench/sqlbench/blob/master/BenchmarkStructure.pdf). The subdirectories contain the following:

- **BenchmarkXML** -- the benchmark definition file in an XML format. It contains a definition of all the tests of the SQLBench.
- **Tool** -- a .NET tool that we have used to create and run of our benchmarks
- **DatabaseOfResults** -- Contains the results of rewriting and redundancy benchmarks. They are in a format of a SQL Server script.
