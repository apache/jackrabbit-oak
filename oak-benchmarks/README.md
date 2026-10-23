Oak Benchmark Jar
=================

This jar is runnable and contains test related run modes.

The following runmodes are currently available:

    * benchmark       : Run benchmark tests against different Oak repository fixtures.
    * scalability     : Run scalability tests against different Oak repository fixtures.

See the subsections below for more details on how to use these modes.

Benchmark mode
--------------

The benchmark mode is used for executing various micro-benchmarks. It can
be invoked like this:

    $ java -jar oak-benchmarks-*.jar benchmark [options] [testcases] [fixtures]

The following benchmark options (with default values) are currently supported:

    --azure                - Azure Connection String (default:
                               DefaultEndpointsProtocol=http;
                               AccountName=devstoreaccount1;
                               AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;
                                           BlobEndpoint=http://127.0.0.1:
                                           10000/devstoreaccount1;)
    --azureContainerName   - Azure container name (default: oak)
    --azureRootPath        - Azure root path (default: /oak)
    --host localhost       - MongoDB host
    --port 27101           - MongoDB port
    --db <name>            - MongoDB database (default is a generated name)
    --mongouri             - MongoDB URI (takes precedence over host, port and db)
    --dropDBAfterTest true - Whether to drop the MongoDB database after the test
    --base target          - Path to the base file (Tar setup),
    --mmap <64bit?>        - TarMK memory mapping (the default on 64 bit JVMs)
    --cache 100            - cache size (in MB)
    --wikipedia <file>     - Wikipedia dump
    --runAsAdmin false     - Run test as admin session
    --itemsToRead 1000     - Number of items to read
    --report false         - Whether to output intermediate results
    --csvFile <file>       - Optional csv file to report the benchmark results
    --concurrency <levels> - Comma separated list of concurrency levels
    --metrics false        - Enable metrics based stats collection
    --rdbjdbcuri           - JDBC URL for RDB persistence (defaults to local file-based H2)
    --rdbjdbcuser          - JDBC username (defaults to "")
    --rdbjdbcpasswd        - JDBC password (defaults to "")
    --rdbjdbctableprefix   - for RDB persistence: prefix for table names (defaults to "")
    --vgcMaxAge            - Continuous DocumentNodeStore VersionGC max age in sec (RDB only)
    --throttlingEnabled    - Whether throttling is enabled for document store or not. Default is false. 
                                (Currently it is supported for only Mongo Document Store)

Please run `--help` to list all options.

These options are passed to the test cases and repository fixtures
that need them. For example the Wikipedia dump option is needed by the
WikipediaImport test case and the MongoDB address information by the
MongoMK and SegmentMK -based repository fixtures. The cache setting
controls the bundle cache size in Jackrabbit, the NodeState
cache size in MongoMK, and the segment cache size in SegmentMK.

The `--concurrency` levels can be specified as comma separated list of values,
eg: `--concurrency 1,4,8`, which will execute the same test with the number of
respective threads. Note that the `beforeSuite()` and `afterSuite()` are executed
before and after the concurrency loop. eg. in the example above, the execution order
is: `beforeSuite()`, 1x `runTest()`, 4x `runTest()`, 8x `runTest()`, `afterSuite()`.
Tests that create their own background threads, should be executed with
`--concurrency 1` which is the default.

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

| Fixture                      | Description                                                    |
|------------------------------|----------------------------------------------------------------|
| Jackrabbit                   | Jackrabbit with the default embedded Derby  bundle PM          |
| Oak-Memory                   | Oak with default in-memory storage                             |
| Oak-MemoryNS                 | Oak with default in-memory NodeStore                           |
| Oak-Mongo                    | Oak with the default Mongo backend                             |
| Oak-Mongo-DS                 | Oak with the default Mongo backend and DataStore               |
| Oak-MongoNS                  | Oak with the Mongo NodeStore                                   |
| Oak-Segment-Tar              | Oak with the Segment Tar backend                               |
| Oak-Segment-Tar-DS           | Oak with the Segment Tar backend and DataStore                 |
| Oak-Segment-Azure            | Oak with the Azure Segment backend                             |
| Oak-RDB                      | Oak with the DocumentMK/RDB persistence                        |
| Oak-RDB-DS                   | Oak with the DocumentMK/RDB persistence and DataStore          |
| Oak-Composite-Store          | Oak with the Composite Node store with Segment Tar backend     |
| Oak-Composite-Memory-Store   | Oak with the Composite Node store with in-memory NodeStore     |
| Oak-Composite-Mongo-Store    | Oak with the Composite Node store with Mongo backend           |


(Note that for Oak-RDB, the required JDBC drivers either need to be embedded
into oak-run, or be specified separately in the class path. Furthermore,
dropDBAfterTest is interpreted to drop the *tables*, not the database
itself, if and only if they have been auto-created)

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

Document node-cache maintenance benchmark
-----------------------------------------

`DocumentCacheBenchmark` measures the node cache obtained from a production
`DocumentNodeStoreBuilder` with a memory document store. Persistent caching is enabled by default.
CacheLIRS and Caffeine SYNC use the original metadata path; only Caffeine ASYNC
uses entry-owned metadata and includes its weight overhead. All modes receive
the same node-cache memory budget; ASYNC entry overhead reduces the number of
entries fitting that budget. Working-set ratios use the raw-value base capacity. The loader creates fixed-weight node
states and reports loader calls; this measures cache behavior, without simulating
MongoDB or RDB latency.

Run each policy in a fresh Java 17 process. Cache implementation selection is
sampled from `oak.documentMK.caffeineCache` when the document builder class loads.
CACHE_LIRS also requires `oak.documentMK.guavaCache=false`.
The shared API defaults to ASYNC, while DocumentNodeStore defaults to SYNC; the
benchmark explicitly selects each node-cache maintenance mode and checks the effective mode.
ASYNC requires the explicit startup opt-in `-Doak.documentMK.asyncCacheMaintenance=true`
for `FT_OAK-12437`, together with `-Doak.documentMK.caffeineCache=true`.
Without the Caffeine opt-in the benchmark defaults to CacheLIRS; with Caffeine
enabled and the ASYNC FT off it selects SYNC and rejects an ASYNC label.

```sh
mvn package -pl oak-benchmarks -Dtest=DocumentCacheBenchmarkTest

java -Xms2g -Xmx2g -Doak.documentMK.caffeineCache=false \
  -Ddocument.cache.policies=CACHE_LIRS \
  -jar oak-benchmarks/target/oak-benchmarks-2.7-SNAPSHOT.jar \
  benchmark DocumentCacheBenchmark Oak-MemoryNS

java -Xms2g -Xmx2g -Doak.documentMK.caffeineCache=true \
  -Ddocument.cache.policies=CAFFEINE_SYNC \
  -jar oak-benchmarks/target/oak-benchmarks-2.7-SNAPSHOT.jar \
  benchmark DocumentCacheBenchmark Oak-MemoryNS

java -Xms2g -Xmx2g -Doak.documentMK.caffeineCache=true \
  -Doak.documentMK.asyncCacheMaintenance=true \
  -Ddocument.cache.policies=CAFFEINE_ASYNC \
  -jar oak-benchmarks/target/oak-benchmarks-2.7-SNAPSHOT.jar \
  benchmark DocumentCacheBenchmark Oak-MemoryNS
```

Use the same JVM, heap, machine and workload properties for all three commands.
Defaults are 10,000 cache entries, 2,000,000 measured operations, at least 10,000
warmup operations, a seed of 42, and working-set/cache-size ratios of 0.5, 1, 2, 5
and 10. `document.cache.entries`, `.operations`, `.warmup` and `.threads` adjust
those settings. `.scenarios` accepts STEADY_STATE, CHURN, INVALIDATION and
CONCURRENT; `.states` accepts WARM and COLD. COLD clears memory and persistent
state after warming the code, before collecting measurements.

All policies use the same production value weigher; ASYNC additionally charges its
entry metadata against the same memory budget.
Persistent writes remain ASYNC independently of maintenance mode. Set
`document.cache.persistent.enabled=false` for a separate comparison without the
persistent cache, or `document.cache.caffeine.maximumWeightMultiplier` for an
explicitly different Caffeine memory allocation (default 1.0).

Output includes throughput, batch time divided by completed operations, p95 of
one independently seeded latency sample per block of up to 64 operations, hit
rate, misses, evictions and loader calls. Worker counters and sample buffers are
local; their aggregation and percentile calculation run outside the batch timer.
Timed batches include memory-cache cleanup; persistent background writes are not drained at
the end of the batch. Run several repetitions before comparing results.

Record matching scenario, cache state and ratio rows in this format:

| Scenario/state/ratio | Policy | Operations/s | ns/op | Sampled p95 ns | Hit % | Misses | Evictions | Loader calls |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| same workload | CACHE_LIRS | measured | measured | measured | measured | measured | measured | measured |
| same workload | CAFFEINE_SYNC | measured | measured | measured | measured | measured | measured | measured |
| same workload | CAFFEINE_ASYNC | measured | measured | measured | measured | measured | measured | measured |

For a quick smoke run, add `-Ddocument.cache.entries=64
-Ddocument.cache.operations=2048 -Ddocument.cache.warmup=256
-Ddocument.cache.threads=3` before `-jar` in all three commands. These short runs
verify configuration and workload accounting; they are too short to establish
performance or choose a maintenance policy.

### Repository workloads complement the direct-cache results

`DocumentCacheRepositoryBenchmark` exercises real DocumentNodeStore operations:
POINT_READ reads populated values; CHILDREN_SCAN enumerates and checks complete
child groups; COMMIT_DIFF merges property updates and compares actual revisions;
COMMIT_ONLY merges and verifies the saved property without an explicit group diff;
CONCURRENT_MIXED runs one writer alongside readers; CONCURRENT_WRITERS runs
writers on distinct nodes; REOPEN_READ disposes and reconstructs the store while
retaining the same document backend and persistent-cache files.

Use the same three startup opt-ins above, replacing `DocumentCacheBenchmark`
with `DocumentCacheRepositoryBenchmark`. This benchmark explicitly selects the
policy for NODE, CHILDREN, DIFF, LOCAL_DIFF, DOCUMENT and PREV_DOCUMENT, retaining
the production cache distribution. Both Caffeine policies include LOCAL_DIFF
in their named implementation and maintenance mode.
Effective-mode checks reject missing startup opt-ins and mismatched cache modes.
Defaults: `document.repository.nodes=10000`,
`.operations=20000`, `.cacheMB=256`, `.childrenPerGroup=100`,
`.backgroundMillis=1000`, `.warmupSeconds=20`, `.durationSeconds=60`; `document.cache.threads` and
`document.cache.persistent.enabled` also apply. Each logical child scan covers up
to the configured group width; each commit verifies its saved value; COMMIT_DIFF
additionally compares revisions. Concurrent writers also request explicit diffs.
Concurrent work includes every completed read/write, with an uneven final batch.

Each scenario first warms its own code for the configured interval in a disposable
store, then builds a fresh store for measurement. Initial reads alone do not warm
commit/diff code. `.operations` is a minimum operation count; the scenario continues
until it also reaches `.durationSeconds`, reporting actual completed work and elapsed
time. Set both timing options to zero only for fixed-count smoke tests.
Select workloads with `.scenarios=POINT_READ,COMMIT_ONLY,COMMIT_DIFF`. Run each policy
and selected scenario in a separate JVM for comparisons. Duration-based concurrent
workers continue for the interval so readers overlap writers throughout the run.

Data nodes are grouped below `/benchmark/bucket-N/group-N/node-N`, with at most
100 groups per bucket. `nodes` is the total data-node count, not a sibling count.
At the default width, 10,000 nodes occupy 100 groups in one bucket; 120,000 occupy
1,200 groups in 12 buckets. These synthetic trees are explicit workload assumptions,
not a measured production traffic distribution. Use `.childrenPerGroup=1000` only
as a wide-folder stress case. The setting is sampled at class initialization.

Data setup and initial reads run outside timing. Reopen also runs outside timing,
retains disk contents, and reports subsequent persistent hits rather than
assuming disk reuse. Values vary in size instead of using the fixed synthetic
loader. Output reports throughput, completed reads/writes, document finds and
queries, node-cache hit rate, persistent-node-cache hits, and local-diff memory
hits/misses. These counters include wrapper rechecks, so they are underlying
lookups rather than unique logical requests or lost revision pairs.
Merge, explicit diff and value-readback times sum worker time;
concurrent totals can exceed elapsed wall time. A local-diff memory miss can
still be satisfied by persistence and does not alone prove reconstruction.
Document counters include calls to the MemoryDocumentStore, not remote database I/O. Persistent
writes remain asynchronous; their pending queue is not explicitly drained.
Repository background work uses the production one-second interval, including
during population and measurement. `.backgroundMillis=0` deliberately disables
it for diagnostics: unflushed bulk-load journal paths can greatly amplify diff
cache misses, so such runs must not be labeled normal production behavior.

Both benchmarks use an in-memory document backend, irrespective of the supplied
fixture label. Neither measures remote MongoDB/RDB latency, cluster invalidation,
failover, or application throughput. Run these workloads separately and repeat
measurements; do not pool logical repository operations with cache microbenchmark
operations or extrapolate their ratios to application performance.

The default 256 MB repository budget matches Oak's default. Use `.cacheMB=384`
for 50% more memory. Explicit 16/24 MB runs are memory-pressure diagnostics.
Initial 500-node setup commits followed by small updates model
bulk loading followed by editing; the forced COMMIT_DIFF comparison tests diff
consumers separately from COMMIT_ONLY. EmptyHook omits application-specific
validators and index hooks. These are targeted repository scenarios, not a
production traffic distribution. A 10,000-node working set at 256 MB does not
represent cache pressure on a large repository; size the working set separately.

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

Scalability mode
--------------

The scalability mode is used for executing various scalability suites to test the
performance of various associated tests. It can be invoked like this:

    $ java -jar oak-benchmarks-*.jar scalability [options] [suites] [fixtures]

The following scalability options (with default values) are currently supported:

    --host localhost       - MongoDB host
    --port 27101           - MongoDB port
    --db <name>            - MongoDB database (default is a generated name)
    --dropDBAfterTest true - Whether to drop the MongoDB database after the test
    --base target          - Path to the base file (Tar setup),
    --mmap <64bit?>        - TarMK memory mapping (the default on 64 bit JVMs)
    --cache 100            - cache size (in MB)
    --csvFile <file>       - Optional csv file to report the benchmark results
    --rdbjdbcuri           - JDBC URL for RDB persistence (defaults to local file-based H2)
    --rdbjdbcuser          - JDBC username (defaults to "")
    --rdbjdbcpasswd        - JDBC password (defaults to "")

These options are passed to the various suites and repository fixtures
that need them. For example the the MongoDB address information by the
MongoMK and SegmentMK -based repository fixtures. The cache setting
controls the NodeState cache size in MongoMK, and the segment cache
size in SegmentMK.

You can use extra JVM options like `-Xmx` settings to better control the
scalability suite test environment. It's also possible to attach the JVM to a
profiler to better understand benchmark results. For example, I'm
using `-agentlib:hprof=cpu=samples,depth=100` as a basic profiling
tool, whose results can be processed with `perl analyze-hprof.pl
java.hprof.txt` to produce a somewhat easier-to-read top-down and
bottom-up summaries of how the execution time is distributed across
the benchmarked codebase.

The scalability suite creates the relevant repository load before starting the tests.
Each test case tries to benchmark and profile a specific aspect of the repository.

Each scalability suite is configured to run a number of related tests which require the
same base load to be available in the repository.
Either the entire suite can be executed or individual tests within the suite can be run.
If the suite names are specified like `ScalabilityBlobSearchSuite` then all the tests
configured for the suite are executed. To execute particular tests in the
suite, suite names appended with tests of the form `suite:test1,test2` must be specified like
`ScalabilityBlobSearchSuite:FormatSearcher,NodeTypeSearcher`. You can specify one or more
suites in the scalability command line, and oak-run will execute each suite in sequence.

Finally the scalability runner supports the following repository fixtures:

| Fixture               | Description                                                    |
|-----------------------|----------------------------------------------------------------|
| Oak-Memory            | Oak with default in-memory storage                             |
| Oak-MemoryNS          | Oak with default in-memory NodeStore                           |
| Oak-Mongo             | Oak with the default Mongo backend                             |
| Oak-Mongo-DS          | Oak with the default Mongo backend and DataStore               |
| Oak-MongoNS           | Oak with the Mongo NodeStore                                   |
| Oak-Segment-Tar       | Oak with the Tar backend (aka Segment NodeStore)               |
| Oak-Segment-Tar-DS    | Oak with the Tar backend (aka Segment NodeStore) and DataStore |
| Oak-RDB               | Oak with the DocumentMK/RDB persistence                        |
| Oak-RDB-DS            | Oak with the DocumentMK/RDB persistence and DataStore          |

(Note that for Oak-RDB, the required JDBC drivers either need to be embedded
into oak-run, or be specified separately in the class path.)

Once started, the scalability runner will execute each listed suite against all the listed
repository fixtures. After starting up the repository and preparing the test environment,
the scalability suite executes all the configured tests to warm up caches before measurements
are started. Then each configured test within the suite are run and the number of
milliseconds used by each execution is recorded. Once done, the following statistics are
computed and reported:

| Column      | Description                                           |
|-------------|-------------------------------------------------------|
| min         | minimum time (in ms) taken by a test run              |
| 10%         | time (in ms) in which the fastest 10% of test runs    |
| 50%         | time (in ms) taken by the median test run             |
| 90%         | time (in ms) in which the fastest 90% of test runs    |
| max         | maximum time (in ms) taken by a test run              |
| N           | total number of test runs in one minute (or more)     |

Also, for each test, the execution times are reported for each iteration/load configured.

| Column      | Description                                           |
|-------------|-------------------------------------------------------|
| Load        | time (in ms) taken by a test run              |

The latter is more useful of these numbers as it shows how the individual execution
times are scaling for each load.

How to add a new scalability suite
--------------------------
The scalability code is
located under `org.apache.jackrabbit.oak.scalabiity` in the oak-run
component.

To add a new scalability suite, you'll need to implement
the `ScalabilitySuite` interface and add an instance of the new suite to the
`allSuites` array in the `ScalabilityRunner` class, along with the test benchmarks,
in the `org.apache.jackrabbit.oak.scalability` package.
To implement the test benchmarks, it is required to extend the `ScalabilityBenchmark`
abstract class and implement the `execute()` method.
In addition, the methods `beforeExecute()` and `afterExecute()` can overridden to do processing
before and after the benchmark executes.

The best way to implement the `ScalabilitySuite` interface is to extend the
`ScalabilityAbstractSuite` base class that takes care of most of the benchmarking
details. The outline of such a suite is:

    class MyTestSuite extends ScalabilityAbstractSuite {
        @Override
        protected void beforeSuite() throws Exception {
            // optional, run once before all the iterations,
            // not included in the performance measurements
        }
        @Override
        protected void beforeIteration(ExecutionContext) throws Exception {
            // optional, Typically, this can be configured to create additional
            // loads for each iteration.
            // This method will be called before each test iteration begins
        }

        @Override
        protected void executeBenchmark(ScalabilityBenchmark benchmark,
            ExecutionContext context) throws Exception {
            // required, executes the specified benchmark
        }

        @Override
        protected void afterIteration() throws Exception {
            // optional, executed after runIteration(),
            // but not included in the performance measurements
        }
        @Override
        protected void afterSuite() throws Exception {
            // optional, run once after all the iterations are complete,
            // not included in the performance measurements
        }
    }

The rough outline of how the individual suite will be run is:

    test.beforeSuite();
    for (iteration...) {
        test.beforeIteration();
        for (benchmarks...) {
              recordStartTime();
              test.executeBenchmark();
              recordEndTime();
        }
        test.afterIteration();
    }
    test.afterSuite();

You can specify any context information to the test benchmarks using the ExecutionContext
object passed as parameter to the `beforeIteration()` and the `executeBenchmark()` methods.
`ExecutionBenchmark` exposes two methods `getMap()` and `setMap()` which can be used to
pass context information.

You can use the `loginWriter()` and `loginReader()` methods to create admin
and anonymous sessions. There's no need to logout those sessions (unless doing
so is relevant to the test) as they will automatically be closed after
the suite is complete and the `afterSuite()` method has been called.

Similarly, you can use the `addBackgroundJob(Runnable)` method to add
background tasks that will be run concurrently while the test benchmark is
executing. The relevant background thread works like this:

    while (running) {
        runnable.run();
        Thread.yield();
    }

As you can see, the `run()` method of the background task gets invoked
repeatedly. Such threads will automatically close once all test iterations
are done, before the `afterSuite()` method is called.

`ScalabilityAbstractSuite` defines some system properties which are used to control the
suites extending from it :

    -Dincrements=10,100,1000,1000     - defines the varying loads for each test iteration
    -Dprofile=true                    - to collect and print profiling data
    -Ddebug=true                      - to output any intermediate results during the suite
                                        run

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
