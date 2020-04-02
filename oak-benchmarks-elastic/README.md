Oak Elastic Benchmark Jar
=========================

This jar is runnable and contains test related run modes. 

The following runmodes are currently available:

    * benchmark       : Run benchmark tests against different Oak repository fixtures.
    
See the subsections below for more details on how to use these modes.

Benchmark mode
--------------

The benchmark mode is used for executing various micro-benchmarks. It can
be invoked like this:

    $ java -jar oak-benchmarks-*.jar benchmark [options] [testcases] [fixtures]
    
The following benchmark options are required :

    --elasticHost        - Elastic host server (e.g. localhost)
    --elasticPort        - Elastic server port (e.g 9200)
    --elasticScheme      - Eastic server scheme (e.g. http)


Example Command for benchmark execution
---------------------------------------

The below command executes ElasticPropertyFullTextSeparated 

`benchmark ElasticPropertyFullTextSeparated Oak-Segment-Tar --wikipedia Path_to_wiki_dump_xml --elasticHost localhost --elasticPort 9200 --elasticScheme http`

Available benchmarks are listed in [ElasticBenchmarkRunner](src/main/java/org/apache/jackrabbit/oak/benchmark/ElasticBenchmarkRunner.java)

Some other useful JVM parameters are -

`-Dlogback.configurationFile=<path to logback-benchmark>\logback-benchmark.xml` (Useful for additional logging, [Sample](src/main/resources/logback-benchmark.xml))
`-Druntime=180`(Change the benchmark execution time, default is 60 seconds)
`-DskipWarmup=true`(skip warmup test execution)


To add new benchmarks or to know about other options supported, please refer the README from oak-benchmarks at [0]


[[0]Oak-Benchmarks README](../oak-benchmarks/README.md)


License
-------

(see the top-level [LICENSE.txt](../LICENSE.txt) for full license details)

Collective work: Copyright 2012 The Apache Software Foundation.

Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
