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


import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Counting;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import org.apache.jackrabbit.guava.common.util.concurrent.MoreExecutors;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.benchmark.authentication.external.AutoMembershipTest;
import org.apache.jackrabbit.oak.benchmark.authentication.external.ExternalLoginTest;
import org.apache.jackrabbit.oak.benchmark.authentication.external.ListIdentitiesTest;
import org.apache.jackrabbit.oak.benchmark.authentication.external.CachedMembershipLoginTest;
import org.apache.jackrabbit.oak.benchmark.authentication.external.PrincipalNameResolutionTest;
import org.apache.jackrabbit.oak.benchmark.authentication.external.SyncAllExternalUsersTest;
import org.apache.jackrabbit.oak.benchmark.authentication.external.SyncAllUsersTest;
import org.apache.jackrabbit.oak.benchmark.authentication.external.SyncExternalUsersTest;
import org.apache.jackrabbit.oak.benchmark.authorization.AceCreationTest;
import org.apache.jackrabbit.oak.benchmark.authorization.CanReadNonExisting;
import org.apache.jackrabbit.oak.benchmark.authorization.GetPrivilegeCollectionIncludeNamesTest;
import org.apache.jackrabbit.oak.benchmark.authorization.MvGlobsAndSubtreesTest;
import org.apache.jackrabbit.oak.benchmark.authorization.SaveHasItemGetItemTest;
import org.apache.jackrabbit.oak.benchmark.authorization.HasPermissionHasItemGetItemTest;
import org.apache.jackrabbit.oak.benchmark.authorization.HasPrivilegesHasItemGetItemTest;
import org.apache.jackrabbit.oak.benchmark.authorization.RefreshHasItemGetItemTest;
import org.apache.jackrabbit.oak.benchmark.authorization.RefreshHasPrivPermHasItemGetItemTest;
import org.apache.jackrabbit.oak.benchmark.authorization.permission.EagerCacheSizeTest;
import org.apache.jackrabbit.oak.benchmark.authorization.principalbased.HasItemGetItemIsModifiedTest;
import org.apache.jackrabbit.oak.benchmark.authorization.principalbased.PermissionEvaluationTest;
import org.apache.jackrabbit.oak.benchmark.authorization.principalbased.PrinicipalBasedReadTest;
import org.apache.jackrabbit.oak.benchmark.wikipedia.WikipediaImport;
import org.apache.jackrabbit.oak.fixture.JackrabbitRepositoryFixture;
import org.apache.jackrabbit.oak.fixture.OakFixture;
import org.apache.jackrabbit.oak.fixture.OakRepositoryFixture;
import org.apache.jackrabbit.oak.fixture.RepositoryFixture;
import org.apache.jackrabbit.oak.plugins.metric.MetricStatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class BenchmarkRunner {

    private static final int MB = 1024 * 1024;

    protected static List<Benchmark> allBenchmarks = new ArrayList<>();
    protected static StatisticsProvider statsProvider = null;

    private static OptionParser parser = new OptionParser();
    protected static BenchmarkOptions benchmarkOptions = null;
    protected static OptionSet options;
    private static boolean initFlag = false;


    public static void main(String[] args) throws Exception {

        initOptionSet(args);

        if (options.has(benchmarkOptions.getHelp())) {
            parser.printHelpOn(System.out);
            System.exit(0);
        }

        String uri = benchmarkOptions.getMongouri().value(options);
        if (uri == null) {
            String db = benchmarkOptions.getDbName().value(options);
            if (db == null) {
                db = OakFixture.getUniqueDatabaseName(OakFixture.OAK_MONGO);
            }
            uri = "mongodb://" + benchmarkOptions.getHost().value(options) + ":"
                    + benchmarkOptions.getPort().value(options) + "/" + db;
        }

        statsProvider = options.has(benchmarkOptions.getMetrics()) ? getStatsProvider() : StatisticsProvider.NOOP;
        int cacheSize = benchmarkOptions.getCache().value(options);
        RepositoryFixture[] allFixtures = new RepositoryFixture[]{
                new JackrabbitRepositoryFixture(benchmarkOptions.getBase().value(options), cacheSize),
                OakRepositoryFixture.getMemoryNS(cacheSize * MB),
                OakRepositoryFixture.getMongo(uri, benchmarkOptions.getDropDBAfterTest().value(options),
                        cacheSize * MB, benchmarkOptions.isThrottlingEnabled().value(options)),
                OakRepositoryFixture.getMongoWithDS(uri,
                        benchmarkOptions.getDropDBAfterTest().value(options),
                        cacheSize * MB,
                        benchmarkOptions.getBase().value(options),
                        benchmarkOptions.getFdsCache().value(options), benchmarkOptions.isThrottlingEnabled().value(options)),
                OakRepositoryFixture.getMongoNS(uri,
                        benchmarkOptions.getDropDBAfterTest().value(options),
                        cacheSize * MB, benchmarkOptions.isThrottlingEnabled().value(options)),
                OakRepositoryFixture.getSegmentTar(benchmarkOptions.getBase().value(options), 256, cacheSize,
                        benchmarkOptions.getMmap().value(options), benchmarkOptions.getBinariesInlineThreshold().value(options)),
                OakRepositoryFixture.getSegmentTarWithDataStore(benchmarkOptions.getBase().value(options), 256, cacheSize,
                        benchmarkOptions.getMmap().value(options), benchmarkOptions.getBinariesInlineThreshold().value(options), 
                        benchmarkOptions.getFdsCache().value(options)),
                OakRepositoryFixture.getSegmentTarWithColdStandby(benchmarkOptions.getBase().value(options), 256, cacheSize,
                        benchmarkOptions.getMmap().value(options), benchmarkOptions.getBinariesInlineThreshold().value(options),
                        benchmarkOptions.getColdUseDataStore().value(options),
                        benchmarkOptions.getFdsCache().value(options),
                        benchmarkOptions.getColdSyncInterval().value(options),
                        benchmarkOptions.getColdShareDataStore().value(options), benchmarkOptions.getColdSecure().value(options),
                        benchmarkOptions.getColdOneShotRun().value(options)),
                OakRepositoryFixture.getSegmentTarWithAzureSegmentStore(benchmarkOptions.getBase().value(options),
                        benchmarkOptions.getAzureConnectionString().value(options),
                        benchmarkOptions.getAzureContainerName().value(options),
                        benchmarkOptions.getAzureRootPath().value(options),
                        256, cacheSize, benchmarkOptions.getBinariesInlineThreshold().value(options), 
                        true, benchmarkOptions.getFdsCache().value(options)),
                OakRepositoryFixture.getRDB(benchmarkOptions.getRdbjdbcuri().value(options),
                        benchmarkOptions.getRdbjdbcuser().value(options),
                        benchmarkOptions.getRdbjdbcpasswd().value(options), benchmarkOptions.getRdbjdbctableprefix().value(options),
                        benchmarkOptions.getDropDBAfterTest().value(options), cacheSize * MB, benchmarkOptions.getVgcMaxAge().value(options)),
                OakRepositoryFixture.getRDBWithDS(benchmarkOptions.getRdbjdbcuri().value(options),
                        benchmarkOptions.getRdbjdbcuser().value(options),
                        benchmarkOptions.getRdbjdbcpasswd().value(options), benchmarkOptions.getRdbjdbctableprefix().value(options),
                        benchmarkOptions.getDropDBAfterTest().value(options), cacheSize * MB, benchmarkOptions.getBase().value(options),
                        benchmarkOptions.getFdsCache().value(options), benchmarkOptions.getVgcMaxAge().value(options)),
                OakRepositoryFixture.getCompositeStore(benchmarkOptions.getBase().value(options), 256, cacheSize,
                        benchmarkOptions.getMmap().value(options), benchmarkOptions.getBinariesInlineThreshold().value(options)),
                OakRepositoryFixture.getCompositeMemoryStore(),
                OakRepositoryFixture.getCompositeMongoStore(uri, cacheSize * MB,
                        benchmarkOptions.getDropDBAfterTest().value(options), benchmarkOptions.isThrottlingEnabled().value(options))
        };

        addToBenchMarkList(Arrays.asList(
                        new OrderedIndexQueryOrderedIndexTest(),
                        new OrderedIndexQueryStandardIndexTest(),
                        new OrderedIndexQueryNoIndexTest(),
            new OrderedIndexInsertOrderedPropertyTest(),
                        new OrderedIndexInsertStandardPropertyTest(),
                        new OrderedIndexInsertNoIndexTest(),
                        new LoginTest(
                                benchmarkOptions.getRunAsUser().value(options),
                                benchmarkOptions.getRunWithToken().value(options),
                                benchmarkOptions.getNoIterations().value(options)),
                        new LoginLogoutTest(
                                benchmarkOptions.getRunAsUser().value(options),
                                benchmarkOptions.getRunWithToken().value(options),
                                benchmarkOptions.getNoIterations().value(options)),
                        new LoginGetRootLogoutTest(
                                benchmarkOptions.getRunAsUser().value(options),
                                benchmarkOptions.getRunWithToken().value(options),
                                benchmarkOptions.getNoIterations().value(options)),
                        new LoginWithTokensTest(benchmarkOptions.getNumberOfUsers().value(options)),
                        new LoginSystemTest(),
                        new LoginImpersonateTest(),
                        new LoginWithMembershipTest(
                                benchmarkOptions.getRunWithToken().value(options),
                                benchmarkOptions.getNoIterations().value(options),
                                benchmarkOptions.getNumberOfGroups().value(options),
                                benchmarkOptions.getNestedGroups().value(options),
                                benchmarkOptions.getExpiration().value(options)),
                        new LoginWithMembersTest(
                                benchmarkOptions.getRunWithToken().value(options),
                                benchmarkOptions.getNoIterations().value(options),
                                benchmarkOptions.getNumberOfGroups().value(options),
                                benchmarkOptions.getExpiration().value(options)),
                        new NamespaceTest(),
                        new NamespaceRegistryTest(),
                        new ReadPropertyTest(),
            GetNodeTest.withAdmin(),
            GetNodeTest.withAnonymous(),
            GetParentNodeTest.withNodeAPIAndParentVisible(benchmarkOptions.getRunAsAdmin().value(options)),
            GetParentNodeTest.withSessionAPIAndParentVisible(benchmarkOptions.getRunAsAdmin().value(options)),
            GetParentNodeTest.withNodeAPIAndParentNotVisible(benchmarkOptions.getRunAsAdmin().value(options)),
            GetParentNodeTest.withSessionAPIAndParentNotVisible(benchmarkOptions.getRunAsAdmin().value(options)),
            new GetMixinNodeTypesTest(),
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
            new CompareManyChildNodesTest(),
            new CreateManyNodesTest(),
                        new UpdateManyChildNodesTest(),
                        new TransientManyChildNodesTest(),
                        new WikipediaImport(
                                benchmarkOptions.getWikipedia().value(options),
                                benchmarkOptions.getFlatStructure().value(options),
                                benchmarkOptions.getReport().value(options)),
                        new CreateNodesBenchmark(),
                        new ManyNodes(options.has(benchmarkOptions.getVerbose())),
                        new ObservationTest(),
                        new RevisionGCTest(),
                        new ContinuousRevisionGCTest(),
                        new XmlImportTest(),
                        new FlatTreeWithAceForSamePrincipalTest(),
                        new ReadDeepTreeTest(
                                benchmarkOptions.getRunAsAdmin().value(options),
                                benchmarkOptions.getItemsToRead().value(options),
                                benchmarkOptions.getReport().value(options)),
                        new CompositeAuthorizationTest(
                                benchmarkOptions.getRunAsAdmin().value(options),
                                benchmarkOptions.getItemsToRead().value(options)), // NOTE: this is currently the no of configurations
                        new CugTest(benchmarkOptions.getRunAsAdmin().value(options),
                                benchmarkOptions.getItemsToRead().value(options),
                                benchmarkOptions.getRandomUser().value(options),
                                benchmarkOptions.getSupportedPaths().values(options),
                                benchmarkOptions.getReverseOrder().value(options)),
                        new CugOakTest(benchmarkOptions.getRunAsAdmin().value(options),
                                benchmarkOptions.getItemsToRead().value(options),
                                benchmarkOptions.getRandomUser().value(options),
                                benchmarkOptions.getSupportedPaths().values(options),
                                benchmarkOptions.getReverseOrder().value(options)),
                        new PrinicipalBasedReadTest(
                                benchmarkOptions.getItemsToRead().value(options),
                                benchmarkOptions.getNumberOfInitialAce().value(options),
                                benchmarkOptions.getNumberOfUsers().value(options),
                                benchmarkOptions.getEntriesForEachPrincipal().value(options),
                                benchmarkOptions.getReverseOrder().value(options),
                                benchmarkOptions.getCompositionType().value(options),
                                benchmarkOptions.getUseAggregationFilter().value(options),
                                benchmarkOptions.getReport().value(options)),
                        new PermissionEvaluationTest(
                                benchmarkOptions.getItemsToRead().value(options),
                                benchmarkOptions.getNumberOfInitialAce().value(options),
                                benchmarkOptions.getNumberOfUsers().value(options),
                                benchmarkOptions.getEntriesForEachPrincipal().value(options),
                                benchmarkOptions.getReverseOrder().value(options),
                                benchmarkOptions.getCompositionType().value(options),
                                benchmarkOptions.getUseAggregationFilter().value(options),
                                benchmarkOptions.getReport().value(options)),
                        new HasItemGetItemIsModifiedTest(
                                benchmarkOptions.getItemsToRead().value(options),
                                benchmarkOptions.getNumberOfInitialAce().value(options),
                                benchmarkOptions.getNumberOfUsers().value(options),
                                benchmarkOptions.getEntriesForEachPrincipal().value(options),
                                benchmarkOptions.getReverseOrder().value(options),
                                benchmarkOptions.getCompositionType().value(options),
                                benchmarkOptions.getUseAggregationFilter().value(options),
                                benchmarkOptions.getReport().value(options)),
                        new EagerCacheSizeTest(benchmarkOptions.getItemsToRead().value(options),
                                benchmarkOptions.getRepeatedRead().value(options),
                                benchmarkOptions.getNumberOfInitialAce().value(options),
                                benchmarkOptions.getNumberOfUsers().value(options),
                                cacheSize,
                                benchmarkOptions.getReport().value(options)),
                        new HasPrivilegesHasItemGetItemTest(
                                benchmarkOptions.getItemsToRead().value(options),
                                benchmarkOptions.getNumberOfInitialAce().value(options),
                                benchmarkOptions.getNumberOfGroups().value(options),
                                benchmarkOptions.getReport().value(options)),
                        new HasPermissionHasItemGetItemTest(
                                benchmarkOptions.getItemsToRead().value(options),
                                benchmarkOptions.getNumberOfInitialAce().value(options),
                                benchmarkOptions.getNumberOfGroups().value(options),
                                benchmarkOptions.getReport().value(options)),
                        new RefreshHasItemGetItemTest(
                                benchmarkOptions.getItemsToRead().value(options),
                                benchmarkOptions.getNumberOfInitialAce().value(options),
                                benchmarkOptions.getNumberOfGroups().value(options),
                                benchmarkOptions.getReport().value(options)),
                        new RefreshHasPrivPermHasItemGetItemTest(
                                benchmarkOptions.getItemsToRead().value(options),
                                benchmarkOptions.getNumberOfInitialAce().value(options),
                                benchmarkOptions.getNumberOfGroups().value(options),
                                benchmarkOptions.getReport().value(options)),
                        new SaveHasItemGetItemTest(
                                benchmarkOptions.getItemsToRead().value(options),
                                benchmarkOptions.getNumberOfInitialAce().value(options),
                                benchmarkOptions.getNumberOfGroups().value(options),
                                benchmarkOptions.getReport().value(options)),
                        new GetPrivilegeCollectionIncludeNamesTest(benchmarkOptions.getItemsToRead().value(options),
                                benchmarkOptions.getNumberOfInitialAce().value(options),
                                benchmarkOptions.getNumberOfGroups().value(options),
                                benchmarkOptions.getReport().value(options), 
                                benchmarkOptions.getEvalutionType().value(options)),
                        new MvGlobsAndSubtreesTest(benchmarkOptions.getItemsToRead().value(options),
                                benchmarkOptions.getNumberOfInitialAce().value(options),
                                benchmarkOptions.getNumberOfGroups().value(options),
                                benchmarkOptions.getReport().value(options),
                                benchmarkOptions.getEvalutionType().value(options)),
                        new IsCheckedOutAddMixinSetPropertyTest(
                                benchmarkOptions.getRunAsAdmin().value(options), 
                                benchmarkOptions.getItemsToRead().value(options),
                                benchmarkOptions.getReport().value(options)),
                        new ConcurrentReadDeepTreeTest(
                                benchmarkOptions.getRunAsAdmin().value(options),
                                benchmarkOptions.getItemsToRead().value(options),
                                benchmarkOptions.getReport().value(options)),
                        new ConcurrentReadSinglePolicyTreeTest(
                                benchmarkOptions.getRunAsAdmin().value(options),
                                benchmarkOptions.getItemsToRead().value(options),
                                benchmarkOptions.getReport().value(options)),
                        new ConcurrentReadAccessControlledTreeTest(
                                benchmarkOptions.getRunAsAdmin().value(options),
                                benchmarkOptions.getItemsToRead().value(options),
                                benchmarkOptions.getReport().value(options)),
                        new ConcurrentReadAccessControlledTreeTest2(
                                benchmarkOptions.getRunAsAdmin().value(options),
                                benchmarkOptions.getItemsToRead().value(options),
                                benchmarkOptions.getReport().value(options)),
                        new ConcurrentReadRandomNodeAndItsPropertiesTest(
                                benchmarkOptions.getRunAsAdmin().value(options),
                                benchmarkOptions.getItemsToRead().value(options),
                                benchmarkOptions.getReport().value(options)),
                        new ConcurrentHasPermissionTest(
                                benchmarkOptions.getRunAsAdmin().value(options),
                                benchmarkOptions.getItemsToRead().value(options),
                                benchmarkOptions.getReport().value(options)),
                        new ConcurrentHasPermissionTest2(
                                benchmarkOptions.getRunAsAdmin().value(options),
                                benchmarkOptions.getItemsToRead().value(options),
                                benchmarkOptions.getReport().value(options)),
                        new ManyUserReadTest(
                                benchmarkOptions.getRunAsAdmin().value(options),
                                benchmarkOptions.getItemsToRead().value(options),
                                benchmarkOptions.getReport().value(options),
                                benchmarkOptions.getRandomUser().value(options)),
                        new ReadWithMembershipTest(
                                benchmarkOptions.getItemsToRead().value(options),
                                benchmarkOptions.getReport().value(options),
                                benchmarkOptions.getNumberOfGroups().value(options),
                                benchmarkOptions.getNumberOfInitialAce().value(options)),
                        new ConcurrentTraversalTest(
                                benchmarkOptions.getRunAsAdmin().value(options),
                                benchmarkOptions.getItemsToRead().value(options),
                                benchmarkOptions.getReport().value(options),
                                benchmarkOptions.getRandomUser().value(options)),
                        new ConcurrentWriteACLTest(benchmarkOptions.getItemsToRead().value(options)),
                        new ConcurrentEveryoneACLTest(benchmarkOptions.getRunAsAdmin().value(options), benchmarkOptions.getItemsToRead().value(options)),
                        new AceCreationTest(benchmarkOptions.getBatchSize().value(options), benchmarkOptions.getNumberOfInitialAce().value(options),
                                benchmarkOptions.getTransientWrites().value(options)),

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
                                benchmarkOptions.getNumberOfUsers().value(options),
                                benchmarkOptions.getFlatStructure().value(options)),
                        new GetAuthorizableByPrincipalTest(
                                benchmarkOptions.getNumberOfUsers().value(options),
                                benchmarkOptions.getFlatStructure().value(options)),
                        new GetPrincipalTest(
                                benchmarkOptions.getNumberOfUsers().value(options),
                                benchmarkOptions.getFlatStructure().value(options)),
                        new GetGroupPrincipalsTest(
                                benchmarkOptions.getNumberOfGroups().value(options),
                                benchmarkOptions.getNestedGroups().value(options)),

                        // benchmarks adding multiple or single members
                        new AddMembersTest(
                                benchmarkOptions.getNumberOfUsers().value(options),
                                benchmarkOptions.getBatchSize().value(options),
                                benchmarkOptions.getImportBehavior().value(options)),
                        new AddMemberTest(
                                benchmarkOptions.getNumberOfUsers().value(options),
                                benchmarkOptions.getBatchSize().value(options)),
                        new AddUniqueMembersTest(
                                benchmarkOptions.getNumberOfUsers().value(options),
                                benchmarkOptions.getBatchSize().value(options)),

                        // benchmarks removing multiple or single members
                        new RemoveMembersTest(
                                benchmarkOptions.getNumberOfUsers().value(options),
                                benchmarkOptions.getBatchSize().value(options)),
                        new RemoveMemberTest(
                                benchmarkOptions.getNumberOfUsers().value(options),
                                benchmarkOptions.getBatchSize().value(options)),

                        // benchmark testing isMember/isDeclared member; each user only being member of 1 group
                        new IsMemberTest(
                                benchmarkOptions.getNumberOfUsers().value(options),
                                benchmarkOptions.getNestedGroups().value(options)),
                        new IsDeclaredMemberTest(
                                benchmarkOptions.getNumberOfUsers().value(options),
                                benchmarkOptions.getNestedGroups().value(options)),

                        // 4 benchmarks with the same setup test various membership operations.
                        new MemberDeclaredMemberOf(
                                benchmarkOptions.getNumberOfGroups().value(options),
                                benchmarkOptions.getNestedGroups().value(options),
                                benchmarkOptions.getNumberOfUsers().value(options)),
                        new MemberMemberOf(
                                benchmarkOptions.getNumberOfGroups().value(options),
                                benchmarkOptions.getNestedGroups().value(options),
                                benchmarkOptions.getNumberOfUsers().value(options)),
                        new MemberIsDeclaredMember(
                                benchmarkOptions.getNumberOfGroups().value(options),
                                benchmarkOptions.getNestedGroups().value(options),
                                benchmarkOptions.getNumberOfUsers().value(options)),
                        new MemberIsMember(
                                benchmarkOptions.getNumberOfGroups().value(options),
                                benchmarkOptions.getNestedGroups().value(options),
                                benchmarkOptions.getNumberOfUsers().value(options)),
                        new FindAuthorizableWithScopeTest(benchmarkOptions.getNumberOfUsers().value(options),
                                benchmarkOptions.getNumberOfGroups().value(options),
                                benchmarkOptions.getQueryMaxCount().value(options), benchmarkOptions.getSetScope().value(options),
                                benchmarkOptions.getDeclaredMembership().value(options),
                                benchmarkOptions.getRunAsAdmin().value(options)),
                        new ReplicaCrashResilienceTest(),

                        // benchmarks for oak-auth-external
                        new ExternalLoginTest(benchmarkOptions.getNumberOfUsers().value(options),
                                benchmarkOptions.getNumberOfGroups().value(options),
                                benchmarkOptions.getExpiration().value(options), benchmarkOptions.getDynamicMembership().value(options),
                                benchmarkOptions.getAutoMembership().values(options),
                                benchmarkOptions.getReport().value(options), statsProvider,
                                benchmarkOptions.getCacheExpiration().value(options)),
                        new CachedMembershipLoginTest(benchmarkOptions.getNumberOfUsers().value(options),
                                benchmarkOptions.getNumberOfGroups().value(options),
                                benchmarkOptions.getExpiration().value(options), benchmarkOptions.getDynamicMembership().value(options),
                                benchmarkOptions.getAutoMembership().values(options),
                                benchmarkOptions.getReport().value(options), statsProvider,
                                benchmarkOptions.getCacheExpiration().value(options),
                                benchmarkOptions.getNumberOfLocalGroups().value(options)),
                        new SyncAllExternalUsersTest(benchmarkOptions.getNumberOfUsers().value(options),
                                benchmarkOptions.getNumberOfGroups().value(options), benchmarkOptions.getExpiration().value(options),
                                benchmarkOptions.getDynamicMembership().value(options),
                                benchmarkOptions.getAutoMembership().values(options)),
                        new SyncAllUsersTest(benchmarkOptions.getNumberOfUsers().value(options),
                                benchmarkOptions.getNumberOfGroups().value(options), benchmarkOptions.getExpiration().value(options),
                                benchmarkOptions.getDynamicMembership().value(options),
                                benchmarkOptions.getAutoMembership().values(options)),
                        new SyncExternalUsersTest(benchmarkOptions.getNumberOfUsers().value(options),
                                benchmarkOptions.getNumberOfGroups().value(options), benchmarkOptions.getExpiration().value(options),
                                benchmarkOptions.getDynamicMembership().value(options),
                                benchmarkOptions.getAutoMembership().values(options), benchmarkOptions.getBatchSize().value(options)),
                        new PrincipalNameResolutionTest(benchmarkOptions.getNumberOfUsers().value(options),
                                benchmarkOptions.getNumberOfGroups().value(options), benchmarkOptions.getExpiration().value(options),
                                benchmarkOptions.getRoundtripDelay().value(options)),
                        new ListIdentitiesTest(benchmarkOptions.getNumberOfUsers().value(options)),
                        new AutoMembershipTest(benchmarkOptions.getNumberOfUsers().value(options), benchmarkOptions.getNumberOfGroups().value(options),
                                benchmarkOptions.getDynamicMembership().value(options), benchmarkOptions.getAutoMembership().values(options)),
                        new BundlingNodeTest(),
                        new PersistentCacheTest(statsProvider),
                        new StringWriteTest(),
                        new BasicWriteTest(),
                        new CanReadNonExisting(),
                        new IsNodeTypeTest(benchmarkOptions.getRunAsAdmin().value(options)),
                        new SetPropertyTransientTest(),
                        new GetURITest(),
                        new ISO8601FormatterTest(),
                        new ReadBinaryPropertiesTest(),
                        new AccessAfterMoveTest()
                )
        );


        Set<String> argset = new HashSet<>(benchmarkOptions.getNonOption().values(options));
        List<RepositoryFixture> fixtures = new ArrayList<>();
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

        List<Benchmark> benchmarks = new ArrayList<>();
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
            if (options.has(benchmarkOptions.getCsvFile())) {
                out = new PrintStream(FileUtils.openOutputStream(benchmarkOptions.getCsvFile().value(options), true));
            }
            for (Benchmark benchmark : benchmarks) {
                if (benchmark instanceof CSVResultGenerator) {
                    ((CSVResultGenerator) benchmark).setPrintStream(out);
                }
                benchmark.run(fixtures, options.valuesOf(benchmarkOptions.getConcurrency()));
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

    protected static void initOptionSet(String[] args) throws IOException {
        if (!initFlag) {
            benchmarkOptions = new BenchmarkOptions(parser);
            options = parser.parse(args);
            initFlag = true;
        }
    }

    protected static void addToBenchMarkList(List<Benchmark> benchmarks) {
        allBenchmarks.addAll(benchmarks);
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

    protected static StatisticsProvider getStatsProvider() {
        if (statsProvider == null) {
            ScheduledExecutorService executorService = MoreExecutors.getExitingScheduledExecutorService(
                    (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(1));

            return new MetricStatisticsProvider(null, executorService);
        } else {
            return statsProvider;
        }

    }
}
