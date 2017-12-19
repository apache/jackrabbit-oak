/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.benchmark;

import static java.util.Arrays.asList;

import java.io.File;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Counting;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.MoreExecutors;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.benchmark.authentication.external.ExternalLoginTest;
import org.apache.jackrabbit.oak.benchmark.authentication.external.ListIdentitiesTest;
import org.apache.jackrabbit.oak.benchmark.authentication.external.PrincipalNameResolutionTest;
import org.apache.jackrabbit.oak.benchmark.authentication.external.SyncAllExternalUsersTest;
import org.apache.jackrabbit.oak.benchmark.authentication.external.SyncExternalUsersTest;
import org.apache.jackrabbit.oak.benchmark.authorization.AceCreationTest;
import org.apache.jackrabbit.oak.benchmark.wikipedia.WikipediaImport;
import org.apache.jackrabbit.oak.fixture.JackrabbitRepositoryFixture;
import org.apache.jackrabbit.oak.fixture.OakFixture;
import org.apache.jackrabbit.oak.fixture.OakRepositoryFixture;
import org.apache.jackrabbit.oak.fixture.RepositoryFixture;
import org.apache.jackrabbit.oak.plugins.metric.MetricStatisticsProvider;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;

public class BenchmarkRunner {

    private static final int MB = 1024 * 1024;

    public static void main(String[] args) throws Exception {
        OptionParser parser = new OptionParser();
        OptionSpec<File> base = parser.accepts("base", "Base directory")
                .withRequiredArg().ofType(File.class)
                .defaultsTo(new File("target"));
        OptionSpec<String> host = parser.accepts("host", "MongoDB host")
                .withRequiredArg().defaultsTo("localhost");
        OptionSpec<Integer> port = parser.accepts("port", "MongoDB port")
                .withRequiredArg().ofType(Integer.class).defaultsTo(27017);
        OptionSpec<String> dbName = parser.accepts("db", "MongoDB database")
                .withRequiredArg();
        OptionSpec<String> mongouri = parser.accepts("mongouri", "MongoDB URI")
                .withRequiredArg();
        OptionSpec<Boolean> dropDBAfterTest = parser.accepts("dropDBAfterTest", "Whether to drop the MongoDB database after the test")
                .withOptionalArg().ofType(Boolean.class).defaultsTo(true);
        OptionSpec<String> rdbjdbcuri = parser.accepts("rdbjdbcuri", "RDB JDBC URI")
                .withOptionalArg().defaultsTo("jdbc:h2:target/benchmark");
        OptionSpec<String> rdbjdbcuser = parser.accepts("rdbjdbcuser", "RDB JDBC user")
                .withOptionalArg().defaultsTo("");
        OptionSpec<String> rdbjdbcpasswd = parser.accepts("rdbjdbcpasswd", "RDB JDBC password")
                .withOptionalArg().defaultsTo("");
        OptionSpec<String> rdbjdbctableprefix = parser.accepts("rdbjdbctableprefix", "RDB JDBC table prefix")
                .withOptionalArg().defaultsTo("");
        OptionSpec<Boolean> mmap = parser.accepts("mmap", "TarMK memory mapping")
                .withOptionalArg().ofType(Boolean.class)
                .defaultsTo("64".equals(System.getProperty("sun.arch.data.model")));
        OptionSpec<Integer> cache = parser.accepts("cache", "cache size (MB)")
                .withRequiredArg().ofType(Integer.class).defaultsTo(100);
        OptionSpec<Integer> fdsCache = parser.accepts("blobCache", "cache size (MB)")
                .withRequiredArg().ofType(Integer.class).defaultsTo(32);
        OptionSpec<File> wikipedia = parser
                .accepts("wikipedia", "Wikipedia dump").withRequiredArg()
                .ofType(File.class);
        OptionSpec<Boolean> luceneIndexOnFS = parser
                .accepts("luceneIndexOnFS", "Store Lucene index on file system")
                .withOptionalArg()
                .ofType(Boolean.class).defaultsTo(false);
        OptionSpec<Boolean> metrics = parser
                .accepts("metrics", "Enable Metrics collection")
                .withOptionalArg()
                .ofType(Boolean.class).defaultsTo(false);
        OptionSpec<Boolean> withStorage = parser
                .accepts("storage", "Index storage enabled").withOptionalArg()
                .ofType(Boolean.class);
        OptionSpec<String> withServer = parser
                .accepts("server", "Solr server host").withOptionalArg()
                .ofType(String.class);
        OptionSpec<Boolean> runAsAdmin = parser.accepts("runAsAdmin", "Run test using admin session")
                .withRequiredArg().ofType(Boolean.class).defaultsTo(Boolean.FALSE);
        OptionSpec<String> runAsUser = parser.accepts("runAsUser", "Run test using admin, anonymous or a test user")
                .withOptionalArg().ofType(String.class).defaultsTo("admin");
        OptionSpec<Boolean> runWithToken = parser.accepts("runWithToken", "Run test using a login token vs. simplecredentials")
                .withOptionalArg().ofType(Boolean.class).defaultsTo(Boolean.FALSE);
        OptionSpec<Integer> noIterations = parser.accepts("noIterations", "Change default 'passwordHashIterations' parameter.")
                .withOptionalArg().ofType(Integer.class).defaultsTo(AbstractLoginTest.DEFAULT_ITERATIONS);
        OptionSpec<Long> expiration = parser.accepts("expiration", "Expiration time (e.g. principal cache.")
                        .withOptionalArg().ofType(Long.class).defaultsTo(AbstractLoginTest.NO_CACHE);
        OptionSpec<Integer> numberOfGroups = parser.accepts("numberOfGroups", "Number of groups to create.")
                        .withOptionalArg().ofType(Integer.class).defaultsTo(LoginWithMembershipTest.NUMBER_OF_GROUPS_DEFAULT);
        OptionSpec<Integer> numberOfInitialAce = parser.accepts("numberOfInitialAce", "Number of ACE to create before running the test.")
                .withOptionalArg().ofType(Integer.class).defaultsTo(AceCreationTest.NUMBER_OF_INITIAL_ACE_DEFAULT);
        OptionSpec<Boolean> nestedGroups = parser.accepts("nestedGroups", "Use nested groups.")
                        .withOptionalArg().ofType(Boolean.class).defaultsTo(false);
        OptionSpec<Integer> batchSize = parser.accepts("batchSize", "Batch size before persisting operations.")
                .withOptionalArg().ofType(Integer.class).defaultsTo(AddMembersTest.DEFAULT_BATCH_SIZE);
        OptionSpec<String> importBehavior = parser.accepts("importBehavior", "Protected Item Import Behavior")
                                .withOptionalArg().ofType(String.class).defaultsTo(ImportBehavior.NAME_BESTEFFORT);
        OptionSpec<Integer> itemsToRead = parser.accepts("itemsToRead", "Number of items to read")
                .withRequiredArg().ofType(Integer.class).defaultsTo(1000);
        OptionSpec<Integer> concurrency = parser.accepts("concurrency", "Number of test threads.")
                .withRequiredArg().ofType(Integer.class).withValuesSeparatedBy(',');
        OptionSpec<Boolean> report = parser.accepts("report", "Whether to output intermediate results")
                .withOptionalArg().ofType(Boolean.class)
                .defaultsTo(Boolean.FALSE);
        OptionSpec<Boolean> randomUser = parser.accepts("randomUser", "Whether to use a random user to read.")
                .withOptionalArg().ofType(Boolean.class)
                .defaultsTo(Boolean.FALSE);
        OptionSpec<File> csvFile = parser.accepts("csvFile", "File to write a CSV version of the benchmark data.")
                .withOptionalArg().ofType(File.class);
        OptionSpec<Boolean> flatStructure = parser.accepts("flatStructure", "Whether the test should use a flat structure or not.")
                .withOptionalArg().ofType(Boolean.class).defaultsTo(Boolean.FALSE);
        OptionSpec<Integer> numberOfUsers = parser.accepts("numberOfUsers")
                .withOptionalArg().ofType(Integer.class).defaultsTo(10000);
        OptionSpec<Boolean> setScope = parser.accepts("setScope", "Whether to use include setScope in the user query.")
                        .withOptionalArg().ofType(Boolean.class)
                .defaultsTo(Boolean.FALSE);
        OptionSpec<Boolean> reverseOrder = parser.accepts("reverseOrder", "Invert order of configurations in composite setup.")
                .withOptionalArg().ofType(Boolean.class)
                .defaultsTo(Boolean.FALSE);
        OptionSpec<String> supportedPaths = parser.accepts("supportedPaths", "Supported paths in composite setup.")
                .withOptionalArg().ofType(String.class).withValuesSeparatedBy(',');
        OptionSpec<Boolean> dynamicMembership = parser.accepts("dynamicMembership", "Enable dynamic membership handling during synchronisation of external users.")
                .withOptionalArg().ofType(Boolean.class).defaultsTo(Boolean.FALSE);
        OptionSpec<String> autoMembership = parser.accepts("autoMembership", "Ids of those groups a given external identity automatically become member of.")
                .withOptionalArg().ofType(String.class).withValuesSeparatedBy(',');
        OptionSpec<Integer> roundtripDelay = parser.accepts("roundtripDelay", "Use simplified principal name lookup from ExtIdRef by specifying roundtrip delay of value < 0.")
                        .withOptionalArg().ofType(Integer.class).defaultsTo(0);
        OptionSpec<Boolean> transientWrites = parser.accepts("transient", "Do not save data.")
                .withOptionalArg().ofType(Boolean.class)
                .defaultsTo(Boolean.FALSE);
        OptionSpec<Integer> mounts = parser.accepts("mounts", "Number of mounts for the composite node store.")
                .withOptionalArg().ofType(Integer.class).defaultsTo(2);
        OptionSpec<Integer> pathsPerMount = parser.accepts("pathsPerMount", "Number of paths per one mount.")
                .withOptionalArg().ofType(Integer.class).defaultsTo(1000);
        OptionSpec<Integer> vgcMaxAge = parser.accepts("vgcMaxAge", "Continuous DocumentNodeStore VersionGC max age in sec (RDB only)")
                .withRequiredArg().ofType(Integer.class).defaultsTo(-1);
        OptionSpec<Integer> coldSyncInterval = parser.accepts("coldSyncInterval", "interval between sync cycles in sec (Segment-Tar-Cold only)")
                .withRequiredArg().ofType(Integer.class).defaultsTo(5);
        OptionSpec<Boolean> coldUseDataStore = parser
                .accepts("useDataStore",
                        "Whether to use a datastore in the cold standby topology (Segment-Tar-Cold only)")
                .withOptionalArg().ofType(Boolean.class)
                .defaultsTo(Boolean.TRUE);
        OptionSpec<Boolean> coldShareDataStore = parser
                .accepts("shareDataStore",
                        "Whether to share the datastore for primary and standby in the cold standby topology (Segment-Tar-Cold only)")
                .withOptionalArg().ofType(Boolean.class)
                .defaultsTo(Boolean.FALSE);
        OptionSpec<Boolean> coldOneShotRun = parser
                .accepts("oneShotRun",
                        "Whether to do a continuous sync between client and server or sync only once (Segment-Tar-Cold only)")
                .withOptionalArg().ofType(Boolean.class)
                .defaultsTo(Boolean.FALSE);
        OptionSpec<Boolean> coldSecure = parser
                .accepts("secure",
                        "Whether to enable secure communication between primary and standby in the cold standby topology (Segment-Tar-Cold only)")
                .withOptionalArg().ofType(Boolean.class)
                .defaultsTo(Boolean.FALSE);
        
        OptionSpec<?> verbose = parser.accepts("verbose", "Enable verbose output");
        OptionSpec<String> nonOption = parser.nonOptions();
        OptionSpec<?> help = parser.acceptsAll(asList("h", "?", "help"), "show help").forHelp();
        OptionSet options = parser.parse(args);

        if(options.has(help)){
            parser.printHelpOn(System.out);
            System.exit(0);
        }

        String uri = mongouri.value(options);
        if (uri == null) {
            String db = dbName.value(options);
            if (db == null) {
                db = OakFixture.getUniqueDatabaseName(OakFixture.OAK_MONGO);
            }
            uri = "mongodb://" + host.value(options) + ":" + port.value(options) + "/" + db;
        }
        StatisticsProvider statsProvider = options.has(metrics) ? getStatsProvider() : StatisticsProvider.NOOP;
        int cacheSize = cache.value(options);
        RepositoryFixture[] allFixtures = new RepositoryFixture[] {
                new JackrabbitRepositoryFixture(base.value(options), cacheSize),
                OakRepositoryFixture.getMemoryNS(cacheSize * MB),
                OakRepositoryFixture.getMongo(uri,
                        dropDBAfterTest.value(options), cacheSize * MB),
                OakRepositoryFixture.getMongoWithDS(uri,
                        dropDBAfterTest.value(options),
                        cacheSize * MB,
                        base.value(options),
                        fdsCache.value(options)),
                OakRepositoryFixture.getMongoNS(uri,
                        dropDBAfterTest.value(options),
                        cacheSize * MB),
                OakRepositoryFixture.getSegmentTar(base.value(options), 256, cacheSize,
                        mmap.value(options)),
                OakRepositoryFixture.getSegmentTarWithDataStore(base.value(options), 256, cacheSize,
                        mmap.value(options), fdsCache.value(options)),
                OakRepositoryFixture.getSegmentTarWithColdStandby(base.value(options), 256, cacheSize,
                        mmap.value(options), coldUseDataStore.value(options), fdsCache.value(options), 
                        coldSyncInterval.value(options), coldShareDataStore.value(options), coldSecure.value(options), 
                        coldOneShotRun.value(options)),
                OakRepositoryFixture.getRDB(rdbjdbcuri.value(options), rdbjdbcuser.value(options),
                        rdbjdbcpasswd.value(options), rdbjdbctableprefix.value(options), 
                        dropDBAfterTest.value(options), cacheSize * MB, vgcMaxAge.value(options)),
                OakRepositoryFixture.getRDBWithDS(rdbjdbcuri.value(options), rdbjdbcuser.value(options),
                        rdbjdbcpasswd.value(options), rdbjdbctableprefix.value(options),
                        dropDBAfterTest.value(options), cacheSize * MB, base.value(options),
                        fdsCache.value(options), vgcMaxAge.value(options)),
                OakRepositoryFixture.getCompositeStore(base.value(options), 256, cacheSize,
                        mmap.value(options), mounts.value(options), pathsPerMount.value(options)),
                OakRepositoryFixture.getCompositeMemoryStore(mounts.value(options), pathsPerMount.value(options))
        };

        Benchmark[] allBenchmarks = new Benchmark[] {
            new OrderedIndexQueryOrderedIndexTest(),
            new OrderedIndexQueryStandardIndexTest(),
            new OrderedIndexQueryNoIndexTest(),
            new OrderedIndexInsertOrderedPropertyTest(),
            new OrderedIndexInsertStandardPropertyTest(),
            new OrderedIndexInsertNoIndexTest(),
            new LoginTest(
                    runAsUser.value(options),
                    runWithToken.value(options),
                    noIterations.value(options)),
            new LoginLogoutTest(
                    runAsUser.value(options),
                    runWithToken.value(options),
                    noIterations.value(options)),
            new LoginGetRootLogoutTest(
                    runAsUser.value(options),
                    runWithToken.value(options),
                    noIterations.value(options)),
            new LoginWithTokensTest(numberOfUsers.value(options)),
            new LoginSystemTest(),
            new LoginImpersonateTest(),
            new LoginWithMembershipTest(
                    runWithToken.value(options),
                    noIterations.value(options),
                    numberOfGroups.value(options),
                    nestedGroups.value(options),
                    expiration.value(options)),
            new LoginWithMembersTest(
                    runWithToken.value(options),
                    noIterations.value(options),
                    numberOfGroups.value(options),
                    expiration.value(options)),
            new NamespaceTest(),
            new NamespaceRegistryTest(),
            new ReadPropertyTest(),
            GetNodeTest.withAdmin(),
            GetNodeTest.withAnonymous(),
            new GetDeepNodeTest(),
            new SetPropertyTest(),
            new SetMultiPropertyTest(),
            new SmallFileReadTest(),
            new SmallFileWriteTest(),
            new ConcurrentReadTest(),
            new ConcurrentReadWriteTest(),
            new ConcurrentWriteReadTest(),
            new ConcurrentWriteTest(),
            new SimpleSearchTest(),
            new UUIDLookupTest(),
            new SQL2SearchTest(),
            new DescendantSearchTest(),
            new SQL2DescendantSearchTest(),
            new FlatTreeUpdateTest(),
            new CreateManyChildNodesTest(),
            new CreateManyNodesTest(),
            new UpdateManyChildNodesTest(),
            new TransientManyChildNodesTest(),
            new WikipediaImport(
                    wikipedia.value(options),
                    flatStructure.value(options),
                    report.value(options)),
            new CreateNodesBenchmark(),
            new ManyNodes(options.has(verbose)),
            new ObservationTest(),
            new RevisionGCTest(),
            new ContinuousRevisionGCTest(),
            new XmlImportTest(),
            new FlatTreeWithAceForSamePrincipalTest(),
            new ReadDeepTreeTest(
                    runAsAdmin.value(options),
                    itemsToRead.value(options),
                    report.value(options)),
            new CompositeAuthorizationTest(
                        runAsAdmin.value(options),
                        itemsToRead.value(options)), // NOTE: this is currently the no of configurations
            new CugTest(runAsAdmin.value(options),
                        itemsToRead.value(options),
                        randomUser.value(options),
                        supportedPaths.values(options),
                        reverseOrder.value(options)),
            new CugOakTest(runAsAdmin.value(options),
                        itemsToRead.value(options),
                        randomUser.value(options),
                        supportedPaths.values(options),
                        reverseOrder.value(options)),
            new ConcurrentReadDeepTreeTest(
                    runAsAdmin.value(options),
                    itemsToRead.value(options),
                    report.value(options)),
            new ConcurrentReadSinglePolicyTreeTest(
                    runAsAdmin.value(options),
                    itemsToRead.value(options),
                    report.value(options)),
            new ConcurrentReadAccessControlledTreeTest(
                    runAsAdmin.value(options),
                    itemsToRead.value(options),
                    report.value(options)),
            new ConcurrentReadAccessControlledTreeTest2(
                    runAsAdmin.value(options),
                    itemsToRead.value(options),
                    report.value(options)),
            new ConcurrentReadRandomNodeAndItsPropertiesTest(
                    runAsAdmin.value(options),
                    itemsToRead.value(options),
                    report.value(options)),
            new ConcurrentHasPermissionTest(
                    runAsAdmin.value(options),
                    itemsToRead.value(options),
                    report.value(options)),
            new ConcurrentHasPermissionTest2(
                    runAsAdmin.value(options),
                    itemsToRead.value(options),
                    report.value(options)),
            new ManyUserReadTest(
                    runAsAdmin.value(options),
                    itemsToRead.value(options),
                    report.value(options),
                    randomUser.value(options)),
            new ConcurrentTraversalTest(
                    runAsAdmin.value(options),
                    itemsToRead.value(options),
                    report.value(options),
                    randomUser.value(options)),
            new ConcurrentWriteACLTest(itemsToRead.value(options)),
            new ConcurrentEveryoneACLTest(runAsAdmin.value(options), itemsToRead.value(options)),
            new AceCreationTest(batchSize.value(options), numberOfInitialAce.value(options), transientWrites.value(options)),

            ReadManyTest.linear("LinearReadEmpty", 1, ReadManyTest.EMPTY),
            ReadManyTest.linear("LinearReadFiles", 1, ReadManyTest.FILES),
            ReadManyTest.linear("LinearReadNodes", 1, ReadManyTest.NODES),
            ReadManyTest.uniform("UniformReadEmpty", 1, ReadManyTest.EMPTY),
            ReadManyTest.uniform("UniformReadFiles", 1, ReadManyTest.FILES),
            ReadManyTest.uniform("UniformReadNodes", 1, ReadManyTest.NODES),
            new ConcurrentCreateNodesTest(),
            new SequentialCreateNodesTest(),
            new CreateManyIndexedNodesTest(),
            new GetPoliciesTest(),
            new ConcurrentFileWriteTest(),
            new GetAuthorizableByIdTest(
                    numberOfUsers.value(options),
                    flatStructure.value(options)),
            new GetAuthorizableByPrincipalTest(
                    numberOfUsers.value(options),
                    flatStructure.value(options)),
            new GetPrincipalTest(
                    numberOfUsers.value(options),
                    flatStructure.value(options)),
            new GetGroupPrincipalsTest(
                    numberOfGroups.value(options),
                    nestedGroups.value(options)),

            // benchmarks adding multiple or single members
            new AddMembersTest(
                    numberOfUsers.value(options),
                    batchSize.value(options),
                    importBehavior.value(options)),
            new AddMemberTest(
                    numberOfUsers.value(options),
                    batchSize.value(options)),
            new AddUniqueMembersTest(
                    numberOfUsers.value(options),
                    batchSize.value(options)),

            // benchmarks removing multiple or single members
            new RemoveMembersTest(
                    numberOfUsers.value(options),
                    batchSize.value(options)),
            new RemoveMemberTest(
                    numberOfUsers.value(options),
                    batchSize.value(options)),

            // benchmark testing isMember/isDeclared member; each user only being member of 1 group
            new IsMemberTest(
                    numberOfUsers.value(options),
                    nestedGroups.value(options)),
            new IsDeclaredMemberTest(
                    numberOfUsers.value(options),
                    nestedGroups.value(options)),

            // 4 benchmarks with the same setup test various membership operations.
            new MemberDeclaredMemberOf(
                    numberOfGroups.value(options),
                    nestedGroups.value(options),
                    numberOfUsers.value(options)),
            new MemberMemberOf(
                    numberOfGroups.value(options),
                    nestedGroups.value(options),
                    numberOfUsers.value(options)),
            new MemberIsDeclaredMember(
                    numberOfGroups.value(options),
                    nestedGroups.value(options),
                    numberOfUsers.value(options)),
            new MemberIsMember(
                    numberOfGroups.value(options),
                    nestedGroups.value(options),
                    numberOfUsers.value(options)),

            new FullTextSearchTest(
                    wikipedia.value(options),
                    flatStructure.value(options),
                    report.value(options), withStorage.value(options)),
            new FullTextSolrSearchTest(
                    wikipedia.value(options),
                    flatStructure.value(options),
                    report.value(options), withStorage.value(options), withServer.value(options)),
            new FindAuthorizableWithScopeTest(numberOfUsers.value(options), setScope.value(options)),
            new LucenePropertyFullTextTest(
                wikipedia.value(options),
                flatStructure.value(options),
                report.value(options), withStorage.value(options)),
            new LucenePropertyFTSeparated(
                wikipedia.value(options),
                flatStructure.value(options),
                report.value(options), withStorage.value(options)),
            new ReplicaCrashResilienceTest(),

            // benchmarks for oak-auth-external
            new ExternalLoginTest(numberOfUsers.value(options), numberOfGroups.value(options), expiration.value(options), dynamicMembership.value(options), autoMembership.values(options)),
            new SyncAllExternalUsersTest(numberOfUsers.value(options), numberOfGroups.value(options), expiration.value(options), dynamicMembership.value(options), autoMembership.values(options)),
            new SyncExternalUsersTest(numberOfUsers.value(options), numberOfGroups.value(options), expiration.value(options), dynamicMembership.value(options), autoMembership.values(options), batchSize.value(options)),
            new PrincipalNameResolutionTest(numberOfUsers.value(options), numberOfGroups.value(options), expiration.value(options), roundtripDelay.value(options)),
            new ListIdentitiesTest(numberOfUsers.value(options)),

            new HybridIndexTest(base.value(options), statsProvider),
            new BundlingNodeTest(),
            new PersistentCacheTest(statsProvider),
            new BasicWriteTest()
        };

        Set<String> argset = Sets.newHashSet(nonOption.values(options));
        List<RepositoryFixture> fixtures = Lists.newArrayList();
        for (RepositoryFixture fixture : allFixtures) {
            if (argset.remove(fixture.toString())) {
                fixtures.add(fixture);
                configure(fixture, statsProvider);
            }
        }

        if (fixtures.isEmpty()) {
            System.err.println("Warning: no repository fixtures specified, supported fixtures are: "
                    + asSortedString(Arrays.asList(allFixtures)));
        }

        List<Benchmark> benchmarks = Lists.newArrayList();
        for (Benchmark benchmark : allBenchmarks) {
            if (argset.remove(benchmark.toString())) {
                benchmarks.add(benchmark);
            }
        }

        if (benchmarks.isEmpty()) {
            System.err.println("Warning: no benchmarks specified, supported benchmarks are: "
                    + asSortedString(Arrays.asList(allBenchmarks)));
        }

        if (argset.isEmpty()) {
            PrintStream out = null;
            if (options.has(csvFile)) {
                out = new PrintStream(FileUtils.openOutputStream(csvFile.value(options), true));
            }
            for (Benchmark benchmark : benchmarks) {
                if (benchmark instanceof CSVResultGenerator) {
                    ((CSVResultGenerator) benchmark).setPrintStream(out);
                }
                benchmark.run(fixtures, options.valuesOf(concurrency));
            }
            if (out != null) {
                out.close();
            }
            reportMetrics(statsProvider);
        } else {
            System.err.println("Unknown arguments: " + argset);
        }
    }

    private static void reportMetrics(StatisticsProvider statsProvider) {
        if (statsProvider instanceof MetricStatisticsProvider) {
            MetricRegistry metricRegistry = ((MetricStatisticsProvider) statsProvider).getRegistry();
            ConsoleReporter.forRegistry(metricRegistry)
                    .outputTo(System.out)
                    .filter(new MetricFilter() {
                        @Override
                        public boolean matches(String name, Metric metric) {
                            if (metric instanceof Counting) {
                                //Only report non zero metrics
                                return ((Counting) metric).getCount() > 0;
                            }
                            return true;
                        }
                    })
                    .build()
                    .report();
        }
    }

    private static void configure(RepositoryFixture fixture, StatisticsProvider statsProvider) {
        if (fixture instanceof OakRepositoryFixture) {
            ((OakRepositoryFixture) fixture).setStatisticsProvider(statsProvider);
        }
    }

    private static String asSortedString(List<?> in) {
        List<String> tmp = new ArrayList<String>();
        for (Object o : in) {
            tmp.add(o.toString());
        }
        Collections.sort(tmp);
        return tmp.toString();
    }

    private static MetricStatisticsProvider getStatsProvider(){
        ScheduledExecutorService executorService = MoreExecutors.getExitingScheduledExecutorService(
                (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(1));
        return new MetricStatisticsProvider(null, executorService);
    }
}
