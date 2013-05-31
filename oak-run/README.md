Oak Runnable Jar
================

Benchmark mode
--------------

The oak-run jar has a "benchmark" mode for executing various micro-benchmarks.
It can be invoked like this:

    $ java -jar oak-run-*.jar benchmark [options] [testcases] [fixtures]

The following benchmark options (with default values) are currently supported:

    --host localhost   - MongoDB host
    --port 27101       - MongoDB port
    --cache 100        - cache size (in MB)
    --wikipedia <file> - Wikipedia dump

These options are passed to the test cases and repository fixtures
that need them. For example the Wikipedia dump option is needed by the
WikipediaImport test case and the MongoDB address information by the
MongoMK and SegmentMK -based repository fixtures. The cache setting
controls the bundle cache size in Jackrabbit, the KernelNodeState
cache size in MongoMK and the default H2 MK, and the segment cache
size in SegmentMK.

You can use extra JVM options like `-Xmx` settings to better control the
benchmark environment. It's also possible to attach the JVM to a
profiler to better understand benchmark results. For example, I'm
using `-agentlib:hprof=cpu=samples,depth=100` as a basic profiling
tool, whose results can be processed with `perl analyze-hprof.pl
java.hprof.txt` to produce a somewhat easier-to-read top-down and
bottom-up summaries of how the execution time is distributed across
the benchmarked codebase.

The test case names like `ReadPropertyTest`, `SmallFileReadTest` and
`SmallFileWriteTest` indicate the specific test case being run. You can
specify one or more test cases in the benchmark command line, and
oak-run will execute each benchmark in sequence. The benchmark code is
located under `org.apache.jackrabbit.oak.benchmark` in the oak-run
component. Each test case tries to exercise some tightly scoped aspect
of the repository. You might remember many of these tests from the
Jackrabbit benchmark reports like
http://people.apache.org/~jukka/jackrabbit/report-2011-09-27/report.html
that we used to produce earlier.

Finally the benchmark runner supports the following repository fixtures:

| Fixture     | Description                                           |
|-------------|-------------------------------------------------------|
| Jackrabbit  | Jackrabbit with the default embedded Derby  bundle PM |
| Oak-Memory  | Oak with the default MK using in-memory storage       |
| Oak-Default | Oak with the default MK using embedded H2 database    |
| Oak-Mongo   | Oak with the new MongoMK                              |
| Oak-Segment | Oak with MongoDB-based SegmentMK                      |
| Oak-Tar     | Oak with Tar file -based SegmentMK                    |

Once started, the benchmark runner will execute each listed test case
against all the listed repository fixtures. After starting up the
repository and preparing the test environment, the test case is first
executed a few times to warm up caches before measurements are
started. Then the test case is run repeatedly for one minute (or at
least 10 times) and the number of milliseconds used by each execution
is recorded. Once done, the following statistics are computed and
reported:

| Column      | Description                                           |
|-------------|-------------------------------------------------------|
| min         | minimum time (in ms) taken by a test run              |
| 10%         | time (in ms) in which the fastest 10% of test runs    |
| 50%         | time (in ms) taken by the median test run             |
| 90%         | time (in ms) in which the fastest 90% of test runs    |
| max         | maximum time (in ms) taken by a test run              |
| N           | total number of test runs in one minute (or more)     |

The most useful of these numbers is probably the 90% figure, as it
shows the time under which the majority of test runs completed and
thus what kind of performance could reasonably be expected in a normal
usage scenario. However, the reason why all these different numbers
are reported, instead of just the 90% one, is that often seeing the
distribution of time across test runs can be helpful in identifying
things like whether a bigger cache might help.

Finally, and most importantly, like in all benchmarking, the numbers
produced by these tests should be taken with a large dose of salt.
They DO NOT directly indicate the kind of application performance you
could expect with (the current state of) Oak. Instead they are
designed to isolate implementation-level bottlenecks and to help
measure and profile the performance of specific, isolated features.
