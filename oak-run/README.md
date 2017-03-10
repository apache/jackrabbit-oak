Oak Runnable Jar
================

This jar contains everything you need for a simple Oak installation. 

The following runmodes are currently available:

    * backup          : Backup an existing Oak repository.
    * restore         : Restore a backup of an Oak repository.
    * benchmark       : Run benchmark tests against different Oak repository fixtures.
    * debug           : Print status information about an Oak repository.
    * compact         : Segment compaction on a TarMK repository.
    * upgrade         : Migrate existing Jackrabbit 2.x repository to Oak.
    * server          : Run the Oak Server.
    * console         : Start an interactive console.
    * explore         : Starts a GUI browser based on java swing.
    * graph           : Export the segment graph of a segment store to a file.
    * history         : Trace the history of a node
    * check           : Check the FileStore for inconsistencies
    * scalability     : Run scalability tests against different Oak repository fixtures.
    * recovery        : Run a _lastRev recovery on a MongoMK repository
    * checkpoints     : Manage checkpoints
    * tika            : Performs text extraction
    * garbage         : Identifies blob garbage on a DocumentMK repository
    * tarmkdiff       : Show changes between revisions on TarMk
    * tarmkrecovery   : Lists candidates for head journal entries
    * datastorecheck  : Consistency checker for data store 
    * resetclusterid  : Resets the cluster id
    * datastorecacheupgrade : Upgrades the JR2 DataStore cache
    * help            : Print a list of available runmodes
    

Some of the features related to Jackrabbit 2.x are provided by oak-run-jr2 jar. See
the [Oak Runnable JR2](#jr2) section for more details.

See the subsections below for more details on how to use these modes.

Backup
------

See the [official documentation](http://jackrabbit.apache.org/oak/docs/nodestore/segment/overview.html#backup).

Restore
-------

See the [official documentation](http://jackrabbit.apache.org/oak/docs/nodestore/segment/overview.html#restore).

Debug
-----

See the [official documentation](http://jackrabbit.apache.org/oak/docs/nodestore/segment/overview.html#debug).

Console
-------

The 'console' mode allows to work with an interactive console and browse an
existing oak repository. Type ':help' within the console to get a list of all
supported commands. The console currently supports TarMK and MongoMK. To start
the console for a TarMK repository, use:

    $ java -jar oak-run-*.jar console /path/to/oak/repository
    
To start the console for a DocumentMK/Mongo repository, use:

    $ java -jar oak-run-*.jar console mongodb://host

To start the console for a DocumentMK/RDB repository, use:

    $ java -jar oak-run-*.jar --rdbjdbcuser username --rdbjdbcpasswd password console jdbc:...
    
To start the console connecting to a repository in read-write mode, use either of:

    $ java -jar oak-run-*.jar console --read-write /path/to/oak/repository
    $ java -jar oak-run-*.jar console --read-write mongodb://host
    $ java -jar oak-run-*.jar console --read-write --rdbjdbcuser username --rdbjdbcpasswd password console jdbc:...

To specify FDS path while connecting to a repository, use `--fds-path` option (valid for segment and document repos):

    $ java -jar oak-run-*.jar console --fds-path /path/to-data/store /path/to/oak/repository

Console is based on [Groovy Shell](http://groovy.codehaus.org/Groovy+Shell) and hence one 
can use all Groovy constructs. It also exposes the `org.apache.jackrabbit.oak.console.ConsoleSession`
instance as through `session` variable. For example when using SegmentNodeStore you can 
dump the current segment info to a file

    > new File("segment.txt") << session.workingNode.segment.toString()
    
In above case the `workingNode` captures the current `NodeState` which in case of 
Segment/TarMK is `SegmentNodeState`

You can also load external script at launch time via passing an extra argument as shown 
below

    $ java -jar oak-run-*.jar console mongodb://host ":load /path/to/script.groovy"

Explore
-------

The 'explore' mode starts a desktop browser GUI based on java swing which allows for read-only
browsing of an existing oak repository.

    $ java -jar oak-run-*.jar explore /path/to/oak/repository [skip-size-check]

Graph
-----

The 'graph' mode export the segment graph of a file store to a text file in the
[Guess GDF format](https://gephi.github.io/users/supported-graph-formats/gdf-format/),
which is easily imported into [Gephi](https://gephi.github.io).

As the GDF format only supports integer values but the segment time stamps are encoded as long
values an optional 'epoch' argument can be specified. If no epoch is given on the command line
the start of the day of the last modified date of the 'journal.log' is used. The epoch specifies
a negative offset translating all timestamps into a valid int range.

    $ java -jar oak-run-*.jar graph [File] <options>

    [File] -- Path to segment store (required)

    Option           Description
    ------           -----------
    --epoch <Long>   Epoch of the segment time stamps
                       (derived from journal.log if not
                       given)
    --output <File>  Output file (default: segments.gdf)
    --gc             Write the gc generation graph instead of the full graph
    --pattern        Regular exception specifying which
                       nodes to include (optional). Ignore
                       when --gc is specified.

History
-------

See the [official documentation](http://jackrabbit.apache.org/oak/docs/nodestore/segment/overview.html#history).

Check
-----

See the [official documentation](http://jackrabbit.apache.org/oak/docs/nodestore/segment/overview.html#check).

Compact
-------

See the [official documentation](http://jackrabbit.apache.org/oak/docs/nodestore/segment/overview.html#compact).

Checkpoints
-----------

The 'checkpoints' mode can be used to list or remove repository checkpoints
To start this mode, use:

    $ java -jar oak-run-*.jar checkpoints { /path/to/oak/repository | mongodb://host:port/database } [list|rm-all|rm-unreferenced|rm <checkpoint>]

The 'list' option (treated as a default when nothing is specified) will list all existing checkpoints.
The 'rm-all' option will wipe clean the 'checkpoints' node.
The 'rm-unreferenced' option will remove all checkpoints except the one referenced from the async indexer (/:async@async).
The 'rm <checkpoint>' option will remove a specific checkpoint from the repository.

<a name="tika"></a>
Tika
----

The 'tika' mode enables performing text extraction, report generation and 
csv generation required for text extraction


    Apache Jackrabbit Oak 1.4-SNAPSHOT
    Non-option arguments:                                                         
    tika [extract|report|generate]                                                
    report   : Generates a summary report related to binary data                  
    extract  : Performs the text extraction                                       
    generate : Generates the csv data file based on configured NodeStore/BlobStore
    
    Option                 Description                            
    ------                 -----------                            
    -?, -h, --help         show help                              
    --data-file <File>     Data file in csv format containing the 
                             binary metadata                      
    --fds-path <File>      Path of directory used by FileDataStore
    --nodestore            NodeStore detail                       
                             /path/to/oak/repository | mongodb:   
                             //host:port/database                 
    --path                 Path in repository under which the     
                             binaries would be searched           
    --pool-size <Integer>  Size of the thread pool used to        
                             perform text extraction. Defaults to 
                             number of cores on the system        
    --store-path <File>    Path of directory used to store        
                             extracted text content               
    --tika-config <File>   Tika config file path   

<a name="tika-csv"></a>
### CSV File Format

Text extraction tool reads a csv file which contains details regarding those
binary files from which text needs to be extracted. Entries in csv file look like
below

```
43844ed22d640a114134e5a25550244e8836c00c#28705,28705,"application/octet-stream",,"/content/activities/jcr:content/folderThumbnail/jcr:content"
43844ed22d640a114134e5a25550244e8836c00c#28705,28705,"application/octet-stream",,"/content/snowboarding/jcr:content/folderThumbnail/jcr:content"
...
```

Where the columns are in following order

1. BlobId - Value of [Jackrabbit ContentIdentity](http://jackrabbit.apache.org/api/2.0/org/apache/jackrabbit/api/JackrabbitValue.html)
2. Length
3. jcr:mimeType
4. jcr:encoding
5. path of parent node    

The csv file can be generated programmatically. For Oak based repositories
it can be generated via `generate` command. 

### Generate

CSV file required for `extract` and `report` can  be generated via `generate` 
mode

    java -jar oak-run.jar tika \  
    --fds-path /path/to/datastore \
    --nodestore /path/to/segmentstore --data-file dump.csv generate

Above command would scan the NodeStore and create the csv file. This file can 
then be passed to `extract` command
    
### Report

Tool can generate a summary report from a [csv](#tika-csv) file

    java -jar oak-run.jar tika \ 
        --data-file /path/to/binary-stats.csv report

The report provides a summary like

```
14:39:05.402 [main] INFO  o.a.j.o.p.tika.TextExtractorMain - MimeType Stats
        Total size         : 89.3 MB
        Total indexed size : 3.4 MB
        Total count        : 1048

               Type                 Indexed   Supported    Count       Size   
___________________________________________________________________________________
application/epub+zip              |      true|      true|  1       |    3.4 MB
image/png                         |     false|      true|  544     |   40.2 MB
image/jpeg                        |     false|      true|  444     |   34.0 MB
image/tiff                        |     false|      true|  11      |    6.1 MB
application/x-indesign            |     false|     false|  1       |    3.7 MB
application/octet-stream          |     false|     false|  39      |    1.2 MB
application/x-shockwave-flash     |     false|     false|  4       |  372.2 kB
application/pdf                   |     false|     false|  3       |  168.3 kB
video/quicktime                   |     false|     false|  1       |   95.9 kB
```

### Extract

Extraction can be performed via following command

    java -cp oak-run.jar:tika-app-1.8.jar \
    org.apache.jackrabbit.oak.run.Main tika \
    --data-file binary-stats.csv \
    --store-path ./store 
    --fds-path /path/to/datastore  extract
    
You would need to provide the tika-app jar which contains all the parsers. 
It can be downloaded from [here](https://tika.apache.org/download.html). 
Extraction would then be performed in a multi threaded mode. Extracted text
would be stored in the `store-path`

Upgrade
-------

The 'upgrade' mode allows to migrate the contents of an existing
Jackrabbit 2.x repository to Oak. To run the migration, use:

    $ java -jar oak-run-*.jar upgrade [--datastore] \
          /path/to/jackrabbit/repository [/path/to/jackrabbit/repository.xml] \
          { /path/to/oak/repository | mongodb://host:port/database }

The source repository is opened from the given repository directory, and
should not be concurrently accessed by any other client. Repository
configuration is read from the specified configuration file, or from
a `repository.xml` file within the repository directory if an explicit
configuration file is not given.

The target repository is specified either as a local filesystem path to
a directory (which will be automatically created if it doesn't already exist)
of a new TarMK repository or as a MongoDB client URI that specifies the
location of a MongoDB database where a new DocumentMK repository.

The `--datastore` option (if present) prevents the copying of binary data
from a data store of the source repository to the target Oak repository.
Instead the binaries are copied by reference, and you need to make the
source data store available to the new Oak repository.

The content migration will automatically adjust things like node type,
privilege and user account settings that work a bit differently in Oak.
Unsupported features like same-name-siblings are migrated on a best-effort
basis, with no strict guarantees of completeness. Warnings will be logged
for any content inconsistencies that might be encountered; such content
should be manually reviewed after the migration is complete. Note that
things like search index configuration work differently in Oak than in
Jackrabbit 2.x, and will need to be manually recreated after the migration.
See the relevant documentation for more details.

Oak server mode
---------------

The Oak server mode starts a NodeStore or full Oak instance with the
standard JCR plugins and makes it available over a simple HTTP mapping 
defined in the `oak-http` component. To start this mode, use:

    $ java -jar oak-run-*.jar server [uri] [fixture] [options]

If no arguments are specified, the command starts an in-memory repository
and makes it available at http://localhost:8080/. Specify an `uri` and a
`fixture` argument to change the host name and port and specify a different
repository backend.

The optional fixture argument allows to specify the repository implementation
to be used. The following fixtures are currently supported:

| Fixture              | Description                                           |
|----------------------|-------------------------------------------------------|
| Jackrabbit(*)        | Jackrabbit with the default embedded Derby  bundle PM |
| Oak-Memory           | Oak with default in-memory storage                    |
| Oak-MemoryNS         | Oak with default in-memory NodeStore                  |
| Oak-Mongo            | Oak with the default Mongo backend                    |
| Oak-Mongo-DS         | Oak with the default Mongo backend and DataStore      |
| Oak-MongoNS          | Oak with the Mongo NodeStore                          |
| Oak-Segment-Tar      | Oak with the Tar backend (aka Segment NodeStore)      |
| Oak-Segment-Tar-DS   | Oak with the Tar backend and DataStore                |

Jackrabbit fixture requires [Oak Runnable JR2 jar](#jr2)

Depending on the fixture the following options are available:

    --cache 100            - cache size (in MB)
    --host localhost       - MongoDB host
    --port 27101           - MongoDB port
    --db <name>            - MongoDB database (default is a generated name)
    --clusterIds           - Cluster Ids for the Mongo setup: a comma separated list of integers
    --base <file>          - Tar: Path to the base file
    --mmap <64bit?>        - TarMK memory mapping (the default on 64 bit JVMs)
    --rdbjdbcuri           - JDBC URL for RDB persistence
    --rdbjdbcuser          - JDBC username (defaults to "")
    --rdbjdbcpasswd        - JDBC password (defaults to "")
    --rdbjdbctableprefix   - for RDB persistence: prefix for table names (defaults to "")

Examples:

    $ java -jar oak-run-*.jar server
    $ java -jar oak-run-*.jar server http://localhost:4503 Oak-Segment-Tar --base myOak
    $ java -jar oak-run-*.jar server http://localhost:4502 Oak-Mongo --db myOak --clusterIds c1,c2,c3

See the documentation in the `oak-http` component for details about the available functionality.


Benchmark mode
--------------

The benchmark mode is used for executing various micro-benchmarks. It can
be invoked like this:

    $ java -jar oak-run-*.jar benchmark [options] [testcases] [fixtures]

The following benchmark options (with default values) are currently supported:

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

| Fixture             | Description                                                    |
|---------------------|----------------------------------------------------------------|
| Jackrabbit          | Jackrabbit with the default embedded Derby  bundle PM          |
| Oak-Memory          | Oak with default in-memory storage                             |
| Oak-MemoryNS        | Oak with default in-memory NodeStore                           |
| Oak-Mongo           | Oak with the default Mongo backend                             |
| Oak-Mongo-DS        | Oak with the default Mongo backend and DataStore               |
| Oak-MongoNS         | Oak with the Mongo NodeStore                                   |
| Oak-Segment-Tar     | Oak with the Segment Tar backend                               |
| Oak-Segment-Tar-DS  | Oak with the Segment Tar backend and DataStore                 |
| Oak-RDB             | Oak with the DocumentMK/RDB persistence                        |
| Oak-RDB-DS          | Oak with the DocumentMK/RDB persistence and DataStore          |

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

    $ java -jar oak-run-*.jar scalability [options] [suites] [fixtures]

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

Recovery Mode
=============

The recovery mode can be used to check the consistency of `_lastRev` fields
of a MongoMK repository. It can be invoked like this:

    $ java -jar oak-run-*.jar recovery [options] mongodb://host:port/database [dryRun]

The following recovery options (with default values) are currently supported:

    --clusterId         - MongoMK clusterId (default: 0 -> automatic)

The recovery tool will only perform the check and fix for the given clusterId.
It is therefore recommended to explicitly specify a clusterId. The tool will
fix the documents it identified, unless the `dryRun` keyword is specified.

Garbage
=======

The garbage mode can the used to identify blob garbage still referenced by
documents in a MongoMK repository. It can be invoked like this:

    $ java -jar oak-run-*.jar garbage [options] mongodb://host:port/database

The following recovery options (with default values) are currently supported:

    --clusterId         - MongoMK clusterId (default: 0 -> automatic)

The tool will scan the store for documents with blob references and print a
report with the top 100 documents with blob references considered garbage. The
rank is based on the size of the referenced blobs.

<a name="jr2"></a>
Oak Runnable Jar - JR 2
===============================

This jar provides Jackrabbit 2.x related features

The following runmodes are currently available:

    * upgrade     : Upgrade from Jackrabbit 2.x repository to Oak.
    * benchmark   : Run benchmark tests against Jackrabbit 2.x repository fixture.
    * server      : Run the JR2 Server.

Oak Mongo Shell Helpers
=======================

To simplify making sense of data created by Oak in Mongo a javascript file oak-mongo.js
is provided. It includes [some useful function][1] to navigate the data in Mongo

    $ wget https://s.apache.org/oak-mongo.js
    $ mongo localhost/oak --shell oak-mongo.js
    MongoDB shell version: 2.6.3
    connecting to: localhost/oak
    type "help" for help
    > oak.countChildren('/oak:index/')
    356787
    > oak.getChildStats('/oak:index')
    { "count" : 356788, "size" : 127743372, "simple" : "121.83 MB" }
    > oak.getChildStats('/')
    { "count" : 593191, "size" : 302005011, "simple" : "288.01 MB" }
    >
    
For reporting any issue related to Oak the script provides a function to collect important stats and 
can be dumped to a file

    $ mongo localhost/oak --eval "load('/path/to/oak-mongo.js');printjson(oak.systemStats());" --quiet > oak-stats.json

[1]: http://jackrabbit.apache.org/oak/docs/oak-mongo-js/oak.html

Oak TarMK Revision Diff
=======================

See the [official documentation](http://jackrabbit.apache.org/oak/docs/nodestore/segment/overview.html#diff).

<a name="tarmkrecovery"></a>
Oak TarMK Revision Recovery
---------------------------

Lists candidates for head journal entries. Uses a read-only store, so no updates will be performed on target repository.

    $ java -jar oak-run-*.jar tarmkrecovery path/to/repository [--version-v10]

The following options are available:

    --version-v10           - Uses V10 version repository reading (see OAK-2527)

Oak DataStore Check
-------------------

Consistency checker for the DataStore.
Also can be used to list all the blob references in the node store and all the blob ids available in the data store. 
Use the following command:

    $ java -jar oak-run-*.jar datastorecheck [--id] [--ref] [--consistency] \
            [--store <path>|<mongo_uri>] \
            [--s3ds <s3ds_config>|--fds <fds_config>] \
            [--dump <path>] \
            [--track <DataStore local tracking path>]

The following options are available:

    --id             - List all the ids in the data store
    --ref            - List all the blob references in the node store
    --consistency    - List all the missing blobs by doing a consistency check
    Atleast one of the above should be specified
    
    --store          - Path to the segment store of mongo uri (Required for --ref & --consistency option above)
    --dump           - Path where to dump the files (Optional). Otherwise, files will be dumped in the user tmp directory.
    --s3ds           - Path to the S3DataStore configuration file
    --fds            - Path to the FileDataStore configuration file ('path' property is mandatory)
    --track          - Path of the local reposity home folder (Optional). This will place a copy of the downloaded blob ids to be tracked.

Note:
For using S3DataStore the following additional jars have to be downloaded
    - [commons-logging-1.1.3.jar](http://central.maven.org/maven2/commons-logging/commons-logging/1.1.3/commons-logging-1.1.3.jar)
    - [httpcore-4.4.4.jar](http://central.maven.org/maven2/org/apache/httpcomponents/httpcore/4.4.4/httpcore-4.4.4.jar)
    - [aws-java-sdk-osgi-1.10.76.jar](http://central.maven.org/maven2/com/amazonaws/aws-java-sdk-osgi/1.10.76/aws-java-sdk-osgi-1.10.76.jar)
    
The command to be executed for S3DataStore

    java -classpath oak-run-*.jar:httpcore-4.4.4.jar:aws-java-sdk-osgi-1.10.76.jar:commons-logging-1.1.3.jar \
        org.apache.jackrabbit.oak.run.Main \
        datastorecheck --id --ref --consistency \
        --store <path>|<mongo_uri> \
        --s3ds <s3ds_config> \
        --dump <dump_path>

The config files should be formatted according to the OSGi configuration admin specification

    E.g.
    cat > org.apache.jackrabbit.oak.plugins.S3DataStore.config << EOF 
    accessKey="XXXXXXXXX"
    secretKey="YYYYYY"
    s3Bucket="bucket1"
    s3Region="region1"
    EOF
    
    cat > org.apache.jackrabbit.oak.plugins.FileDataStore.config << EOF 
    path="/data/datastore"
    EOF        
    


Reset Cluster Id
---------------

Resets the cluster id generated internally. Use the following command after stopping the server

    $ java -jar oak-run-*.jar resetclusterid \
            { /path/to/oak/repository | mongodb://host:port/database }

The cluster id will be removed and will be generated on next server start up.

Oak DataStore Cache Upgrade
---------------------------

Upgrades the JR2 DataStore cache by moving files to the Upload staging and the download cache of the DataStore.

    $ java -classpath oak-run-*.jar datastorecacheupgrade \
        --homeDir <home_directory> \
        --path <path> \
        --moveCache <true|false> \
        --deleteMapFile <true|false>

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
