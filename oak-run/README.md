Oak Runnable Jar
================

This jar contains everything you need for a simple Oak installation.
The following runmodes are currently available:

    * backup    : Backup an existing Oak repository
    * benchmark : Run benchmark tests against different Oak repository fixtures.
    * debug     : Print status information about an Oak repository.
    * upgrade   : Upgrade from Jackrabbit 2.x repository to Oak.
    * server    : Run the Oak Server

See the subsections below for more details on how to use these modes.

Backup
------

The 'backup' mode creates a backup from an existing oak repository. To start this
mode, use:

    $ java -jar oak-run-*.jar backup /path/to/repository /path/to/backup


Debug
-----

The 'debug' mode allows to obtain information about the status of the specified
store. Currently this is only supported for the segment store (aka tar mk). To
start this mode, use:

    $ java -jar oak-run-*.jar debug /path/to/oakrepository [id...]


Upgrade
-------

The 'upgrade' mode allows to upgrade an existing Jackrabbit 2.x installation to
Oak. To start the upgrade, use

    $ java -jar oak-run-*.jar upgrade /path/to/jr2repository /path/to/oakrepository


Oak server mode
---------------

The Oak server mode starts a full Oak instance with the standard JCR plugins
and makes it available over a simple HTTP mapping defined in the `oak-http`
component. To start this mode, use:

    $ java -jar oak-run-*.jar [/path/to/mk...]

If no arguments are specified, the command starts an in-memory repository
and makes it available at http://localhost:8080/. Possible path arguments
specify the locations of on-disk MicroKernel backends that are each opened
and mapped to URLs under http://localhost:8080/.

See the documentation in the `oak-http` component for details about the
available functionality.


Benchmark mode
--------------

The benchmark mode is used for executing various micro-benchmarks. It can
be invoked like this:

    $ java -jar oak-run-*.jar benchmark [options] [testcases] [fixtures]

The following benchmark options (with default values) are currently supported:

    --host localhost       - MongoDB host
    --port 27101           - MongoDB port
    --db <name>            - MongoDB database (default is a generated name)
    --dropDBAfterTest true - Whether to drop the MongoDB database after the test
    --mmap <64bit?>        - TarMK memory mapping (the default on 64 bit JVMs)
    --cache 100            - cache size (in MB)
    --wikipedia <file>     - Wikipedia dump
    --runAsAdmin false     - Run test as admin session
    --itemsToRead 1000     - Number of items to read
    --report false         - Whether to output intermediate results
    --csvFile <file>       - Optional csv file to report the benchmark results
    --concurrency <levels> - Comma separated list of concurrency levels

These options are passed to the test cases and repository fixtures
that need them. For example the Wikipedia dump option is needed by the
WikipediaImport test case and the MongoDB address information by the
MongoMK and SegmentMK -based repository fixtures. The cache setting
controls the bundle cache size in Jackrabbit, the KernelNodeState
cache size in MongoMK and the default H2 MK, and the segment cache
size in SegmentMK.

The `--concurrency` levels can be specified as comma separated list of values, eg: `--concurrency 1,4,8`, which will execute the same test with the number of respective threads. Note that the `beforeSuite()` and `afterSuite()` are executed before and after the concurrency loop. eg. in the example above, the execution order is: `beforeSuite()`, 1x `runTest()`, 4x `runTest()`, 8x `runTest()`, `afterSuite()`. Tests that create their own background threads, should be executed with `--concurrency 1` which is the default.

You can use extra JVM options like `-Xmx` settings to better control the
benchmark environment. It's also possible to attach the JVM to a
profiler to better understand benchmark results. For example, I'm
using `-agentlib:hprof=cpu=samples,depth=100` as a basic profiling
tool, whose results can be processed with `perl analyze-hprof.pl
java.hprof.txt` to produce a somewhat easier-to-read top-down and
bottom-up summaries of how the execution time is distributed across
the benchmarked codebase.

Some system properties are also used to control the benchmarks. For example:

    -Dwarmup=5         - warmup time (in seconds)
    -Druntime=60       - how long a single benchmark should run (in seconds)
    -Dprofile=true     - to collect and print profiling data

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
| Oak-Memory  | Oak with in-memory storage                            |
| Oak-Mongo   | Oak with the Mongo backend                            |
| Oak-Tar     | Oak with the Tar backend (aka Segment NodeStore)      |
| Oak-H2      | Oak with the MK using embedded H2 database            |


Once started, the benchmark runner will execute each listed test case
against all the listed repository fixtures. After starting up the
repository and preparing the test environment, the test case is first
executed a few times to warm up caches before measurements are
started. Then the test case is run repeatedly for one minute
and the number of milliseconds used by each execution
is recorded. Once done, the following statistics are computed and
reported:

| Column      | Description                                           |
|-------------|-------------------------------------------------------|
| C           | concurrency level                                     |
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

How to add a new benchmark
--------------------------

To add a new test case to this benchmark suite, you'll need to implement
the `Benchmark` interface and add an instance of the new test to the
`allBenchmarks` array in the `BenchmarkRunner` class in the
`org.apache.jackrabbit.oak.benchmark` package.

The best way to implement the `Benchmark` interface is to extend the
`AbstractTest` base class that takes care of most of the benchmarking
details. The outline of such a benchmark is:

    class MyTest extends AbstracTest {
        @Override
        protected void beforeSuite() throws Exception {
            // optional, run once before all the iterations,
            // not included in the performance measurements
        }
        @Override
        protected void beforeTest() throws Exception {
            // optional, run before runTest() on each iteration,
            // but not included in the performance measurements
        }
        @Override
        protected void runTest() throws Exception {
            // required, run repeatedly during the benchmark,
            // and the time of each iteration is measured.
            // The ideal execution time of this method is
            // from a few hundred to a few thousand milliseconds.
            // Use a loop if the operation you're hoping to measure
            // is faster than that.
        }
        @Override
        protected void afterTest() throws Exception {
            // optional, run after runTest() on each iteration,
            // but not included in the performance measurements
        }
        @Override
        protected void afterSuite() throws Exception {
            // optional, run once after all the iterations,
            // not included in the performance measurements
        }
    }

The rough outline of how the benchmark will be run is:

    test.beforeSuite();
    for (...) {
        test.beforeTest();
        recordStartTime();
        test.runTest();
        recordEndTime();
        test.afterTest();
    }
    test.afterSuite();

You can use the `loginWriter()` and `loginReader()` methods to create admin
and anonymous sessions. There's no need to logout those sessions (unless doing
so is relevant to the benchmark) as they will automatically be closed after
the benchmark is completed and the `afterSuite()` method has been called.

Similarly, you can use the `addBackgroundJob(Runnable)` method to add
background tasks that will be run concurrently while the main benchmark is
executing. The relevant background thread works like this:

    while (running) {
        runnable.run();
        Thread.yield();
    }

As you can see, the `run()` method of the background task gets invoked
repeatedly. Such threads will automatically close once all test iterations
are done, before the `afterSuite()` method is called.
