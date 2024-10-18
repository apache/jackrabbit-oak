/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.benchmark;


import static java.util.Arrays.asList;
import static org.apache.jackrabbit.oak.benchmark.ReadDeepTreeTest.DEFAULT_ITEMS_TD_READ;
import static org.apache.jackrabbit.oak.benchmark.ReadDeepTreeTest.DEFAULT_REPEATED_READ;

import java.io.File;
import joptsimple.OptionParser;
import joptsimple.OptionSpec;
import org.apache.jackrabbit.oak.benchmark.authorization.AceCreationTest;
import org.apache.jackrabbit.oak.security.authorization.composite.CompositeAuthorizationConfiguration;
import org.apache.jackrabbit.oak.segment.Segment;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;

public class BenchmarkOptions {

    private final OptionSpec<File> base;
    private final OptionSpec<String> host;
    private final OptionSpec<Integer> port;
    private final OptionSpec<String> dbName;
    private final OptionSpec<String> mongouri;
    private final OptionSpec<Boolean> dropDBAfterTest;
    private final OptionSpec<String> rdbjdbcuri;
    private final OptionSpec<String> rdbjdbcuser;
    private final OptionSpec<String> rdbjdbcpasswd;
    private final OptionSpec<String> rdbjdbctableprefix;
    private final OptionSpec<String> azureConnectionString;
    private final OptionSpec<String> azureContainerName;
    private final OptionSpec<String> azureRootPath;
    private final OptionSpec<Boolean> mmap;
    private final OptionSpec<Integer> binariesInlineThreshold;
    private final OptionSpec<Integer> cache;
    private final OptionSpec<Integer> fdsCache;
    private final OptionSpec<File> wikipedia;
    private final OptionSpec<Long> expiration;
    private final OptionSpec<Boolean> metrics;
    private final OptionSpec<Boolean> withStorage;
    private final OptionSpec<String> withServer;
    private final OptionSpec<Boolean> runAsAdmin;
    private final OptionSpec<String> runAsUser;
    private final OptionSpec<Boolean> runWithToken;
    private final OptionSpec<Integer> noIterations;
    private final OptionSpec<Boolean> luceneIndexOnFS;
    private final OptionSpec<Integer> numberOfGroups;
    private final OptionSpec<Integer> queryMaxCount;
    private final OptionSpec<Boolean> declaredMembership;
    private final OptionSpec<Integer> numberOfInitialAce;
    private final OptionSpec<Boolean> nestedGroups;
    private final OptionSpec<String> compositionType;
    private final OptionSpec<String> autoMembership;
    private final OptionSpec<Integer> roundtripDelay;
    private final OptionSpec<Boolean> transientWrites;
    private final OptionSpec<Integer> vgcMaxAge;
    private final OptionSpec<Boolean> coldUseDataStore;
    private final OptionSpec<Boolean> coldShareDataStore;
    private final OptionSpec<Boolean> entriesForEachPrincipal;
    private final OptionSpec<Integer> coldSyncInterval;
    private final OptionSpec<Boolean> dynamicMembership;
    private final OptionSpec<Boolean> reverseOrder;
    private final OptionSpec<String> supportedPaths;
    private final OptionSpec<Boolean> setScope;
    private final OptionSpec<Integer> numberOfUsers;
    private final OptionSpec<Boolean> flatStructure;
    private final OptionSpec<Boolean> randomUser;
    private final OptionSpec<File> csvFile;
    private final OptionSpec<Boolean> report;
    private final OptionSpec<Integer> concurrency;
    private final OptionSpec<Integer> repeatedRead;
    private final OptionSpec<Integer> itemsToRead;
    private final OptionSpec<String> importBehavior;
    private final OptionSpec<Integer> batchSize;
    private final OptionSpec<Boolean> coldOneShotRun;
    private final OptionSpec<Boolean> coldSecure;
    private final OptionSpec<?> verbose;
    private final OptionSpec<String> nonOption;
    private final OptionSpec<?> help;
    private final OptionSpec<Boolean> useAggregationFilter;
    private final OptionSpec<String> evalType;
    private final OptionSpec<String> elasticHost;
    private final OptionSpec<String> elasticScheme;
    private final OptionSpec<Integer> elasticPort;
    private final OptionSpec<String> elasticApiKeyId;
    private final OptionSpec<String> elasticApiKeySecret;
    private final OptionSpec<Boolean> throttlingEnabled;
    private final OptionSpec<Long> cacheExpiration;
    private final OptionSpec<Integer> numberOfLocalGroups;

    public OptionSpec<String> getElasticApiKeyId() {
        return elasticApiKeyId;
    }

    public OptionSpec<String> getElasticApiKeySecret() {
        return elasticApiKeySecret;
    }

    public OptionSpec<String> getElasticHost() {
        return elasticHost;
    }

    public OptionSpec<String> getElasticScheme() {
        return elasticScheme;
    }

    public OptionSpec<Integer> getElasticPort() {
        return elasticPort;
    }

    public OptionSpec<File> getBase() {
        return base;
    }

    public OptionSpec<String> getHost() {
        return host;
    }

    public OptionSpec<Integer> getPort() {
        return port;
    }

    public OptionSpec<String> getDbName() {
        return dbName;
    }

    public OptionSpec<String> getMongouri() {
        return mongouri;
    }

    public OptionSpec<Boolean> getDropDBAfterTest() {
        return dropDBAfterTest;
    }

    public OptionSpec<String> getRdbjdbcuri() {
        return rdbjdbcuri;
    }

    public OptionSpec<String> getRdbjdbcuser() {
        return rdbjdbcuser;
    }

    public OptionSpec<String> getRdbjdbcpasswd() {
        return rdbjdbcpasswd;
    }

    public OptionSpec<String> getRdbjdbctableprefix() {
        return rdbjdbctableprefix;
    }

    public OptionSpec<String> getAzureConnectionString() {
        return azureConnectionString;
    }

    public OptionSpec<String> getAzureContainerName() {
        return azureContainerName;
    }

    public OptionSpec<String> getAzureRootPath() {
        return azureRootPath;
    }

    public OptionSpec<Boolean> getMmap() {
        return mmap;
    }

    public OptionSpec<Integer> getBinariesInlineThreshold() {
        return binariesInlineThreshold;
    }

    public OptionSpec<Integer> getCache() {
        return cache;
    }

    public OptionSpec<Integer> getFdsCache() {
        return fdsCache;
    }

    public OptionSpec<File> getWikipedia() {
        return wikipedia;
    }

    public OptionSpec<Long> getExpiration() {
        return expiration;
    }

    public OptionSpec<Boolean> getMetrics() {
        return metrics;
    }

    public OptionSpec<Boolean> getWithStorage() {
        return withStorage;
    }

    public OptionSpec<String> getWithServer() {
        return withServer;
    }

    public OptionSpec<Boolean> getRunAsAdmin() {
        return runAsAdmin;
    }

    public OptionSpec<String> getRunAsUser() {
        return runAsUser;
    }

    public OptionSpec<Boolean> getRunWithToken() {
        return runWithToken;
    }

    public OptionSpec<Integer> getNoIterations() {
        return noIterations;
    }

    public OptionSpec<Boolean> getLuceneIndexOnFS() {
        return luceneIndexOnFS;
    }

    public OptionSpec<Integer> getNumberOfGroups() {
        return numberOfGroups;
    }

    public OptionSpec<Integer> getQueryMaxCount() {
        return queryMaxCount;
    }

    public OptionSpec<Boolean> getDeclaredMembership() {
        return declaredMembership;
    }

    public OptionSpec<Integer> getNumberOfInitialAce() {
        return numberOfInitialAce;
    }

    public OptionSpec<Boolean> getNestedGroups() {
        return nestedGroups;
    }

    public OptionSpec<String> getCompositionType() {
        return compositionType;
    }

    public OptionSpec<String> getAutoMembership() {
        return autoMembership;
    }

    public OptionSpec<Integer> getRoundtripDelay() {
        return roundtripDelay;
    }

    public OptionSpec<Boolean> getTransientWrites() {
        return transientWrites;
    }

    public OptionSpec<Integer> getVgcMaxAge() {
        return vgcMaxAge;
    }

    public OptionSpec<Boolean> getColdUseDataStore() {
        return coldUseDataStore;
    }

    public OptionSpec<Boolean> getColdShareDataStore() {
        return coldShareDataStore;
    }

    public OptionSpec<Boolean> getEntriesForEachPrincipal() {
        return entriesForEachPrincipal;
    }

    public OptionSpec<Integer> getColdSyncInterval() {
        return coldSyncInterval;
    }

    public OptionSpec<Boolean> getDynamicMembership() {
        return dynamicMembership;
    }

    public OptionSpec<Boolean> getReverseOrder() {
        return reverseOrder;
    }

    public OptionSpec<String> getSupportedPaths() {
        return supportedPaths;
    }

    public OptionSpec<Boolean> getSetScope() {
        return setScope;
    }

    public OptionSpec<Integer> getNumberOfUsers() {
        return numberOfUsers;
    }

    public OptionSpec<Boolean> getFlatStructure() {
        return flatStructure;
    }

    public OptionSpec<Boolean> getRandomUser() {
        return randomUser;
    }

    public OptionSpec<File> getCsvFile() {
        return csvFile;
    }

    public OptionSpec<Boolean> getReport() {
        return report;
    }

    public OptionSpec<Integer> getConcurrency() {
        return concurrency;
    }

    public OptionSpec<Integer> getRepeatedRead() {
        return repeatedRead;
    }

    public OptionSpec<Integer> getItemsToRead() {
        return itemsToRead;
    }

    public OptionSpec<String> getImportBehavior() {
        return importBehavior;
    }

    public OptionSpec<Integer> getBatchSize() {
        return batchSize;
    }

    public OptionSpec<Boolean> getColdOneShotRun() {
        return coldOneShotRun;
    }

    public OptionSpec<Boolean> getColdSecure() {
        return coldSecure;
    }

    public OptionSpec<?> getVerbose() {
        return verbose;
    }

    public OptionSpec<String> getNonOption() {
        return nonOption;
    }

    public OptionSpec<?> getHelp() {
        return help;
    }

    public OptionSpec<Boolean> getUseAggregationFilter() {
        return useAggregationFilter;
    }
    
    public OptionSpec<String> getEvalutionType() { return evalType; }

    public OptionSpec<Boolean> isThrottlingEnabled() {
        return throttlingEnabled;
    }

    public OptionSpec<Long> getCacheExpiration() {
        return cacheExpiration;
    }

    public OptionSpec<Integer> getNumberOfLocalGroups() {
        return numberOfLocalGroups;
    }


    public BenchmarkOptions(OptionParser parser) {
        base = parser.accepts("base", "Base directory")
                .withRequiredArg().ofType(File.class)
                .defaultsTo(new File("target"));
        host = parser.accepts("host", "MongoDB host")
                .withRequiredArg().defaultsTo("localhost");
        port = parser.accepts("port", "MongoDB port")
                .withRequiredArg().ofType(Integer.class).defaultsTo(27017);
        dbName = parser.accepts("db", "MongoDB database")
                .withRequiredArg();
        mongouri = parser.accepts("mongouri", "MongoDB URI")
                .withRequiredArg();
        dropDBAfterTest = parser.accepts("dropDBAfterTest", "Whether to drop the MongoDB database after the test")
                .withOptionalArg().ofType(Boolean.class).defaultsTo(true);
        rdbjdbcuri = parser.accepts("rdbjdbcuri", "RDB JDBC URI")
                .withOptionalArg().defaultsTo("jdbc:h2:target/benchmark");
        rdbjdbcuser = parser.accepts("rdbjdbcuser", "RDB JDBC user")
                .withOptionalArg().defaultsTo("");
        rdbjdbcpasswd = parser.accepts("rdbjdbcpasswd", "RDB JDBC password")
                .withOptionalArg().defaultsTo("");
        rdbjdbctableprefix = parser.accepts("rdbjdbctableprefix", "RDB JDBC table prefix")
                .withOptionalArg().defaultsTo("");

        azureConnectionString = parser.accepts("azure", "Azure Connection String")
                .withOptionalArg().defaultsTo("DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;");
        azureContainerName = parser.accepts("azureContainerName", "Azure container name")
                .withOptionalArg().defaultsTo("oak");
        azureRootPath = parser.accepts("azureRootPath", "Azure root path")
                .withOptionalArg().defaultsTo("/oak");

        mmap = parser.accepts("mmap", "TarMK memory mapping")
                .withOptionalArg().ofType(Boolean.class)
                .defaultsTo("64".equals(System.getProperty("sun.arch.data.model")));

        binariesInlineThreshold = parser.accepts("binariesInlineThreshold", "TarMK binaries inline threshold")
            .withOptionalArg().ofType(Integer.class)
            .defaultsTo(Segment.MEDIUM_LIMIT);
        
        cache = parser.accepts("cache", "cache size (MB)")
                .withRequiredArg().ofType(Integer.class).defaultsTo(100);
        fdsCache = parser.accepts("blobCache", "cache size (MB)")
                .withRequiredArg().ofType(Integer.class).defaultsTo(32);
        wikipedia = parser
                .accepts("wikipedia", "Wikipedia dump").withRequiredArg()
                .ofType(File.class);
        luceneIndexOnFS = parser
                .accepts("luceneIndexOnFS", "Store Lucene index on file system")
                .withOptionalArg()
                .ofType(Boolean.class).defaultsTo(false);
        metrics = parser
                .accepts("metrics", "Enable Metrics collection")
                .withOptionalArg()
                .ofType(Boolean.class).defaultsTo(false);
        withStorage = parser
                .accepts("storage", "Index storage enabled").withOptionalArg()
                .ofType(Boolean.class);
        withServer = parser
                .accepts("server", "Solr server host").withOptionalArg()
                .ofType(String.class);
        runAsAdmin = parser.accepts("runAsAdmin", "Run test using admin session")
                .withRequiredArg().ofType(Boolean.class).defaultsTo(Boolean.FALSE);
        runAsUser = parser.accepts("runAsUser", "Run test using admin, anonymous or a test user")
                .withOptionalArg().ofType(String.class).defaultsTo("admin");
        runWithToken = parser.accepts("runWithToken", "Run test using a login token vs. simplecredentials")
                .withOptionalArg().ofType(Boolean.class).defaultsTo(Boolean.FALSE);
        noIterations = parser.accepts("noIterations", "Change default 'passwordHashIterations' parameter.")
                .withOptionalArg().ofType(Integer.class).defaultsTo(AbstractLoginTest.DEFAULT_ITERATIONS);
        expiration = parser.accepts("expiration", "Expiration time (e.g. principal cache.")
                .withOptionalArg().ofType(Long.class).defaultsTo(AbstractLoginTest.NO_CACHE);
        numberOfGroups = parser.accepts("numberOfGroups", "Number of groups to create.")
                .withOptionalArg().ofType(Integer.class).defaultsTo(LoginWithMembershipTest.NUMBER_OF_GROUPS_DEFAULT);
        queryMaxCount = parser.accepts("queryMaxCount", "Max number of query results.")
                .withOptionalArg().ofType(Integer.class).defaultsTo(Integer.MAX_VALUE);
        declaredMembership = parser.accepts("declaredMembership", "Only look for declared membership.")
                .withOptionalArg().ofType(Boolean.class).defaultsTo(true);
        numberOfInitialAce = parser.accepts("numberOfInitialAce", "Number of ACE to create before running the test.")
                .withOptionalArg().ofType(Integer.class).defaultsTo(AceCreationTest.NUMBER_OF_INITIAL_ACE_DEFAULT);
        nestedGroups = parser.accepts("nestedGroups", "Use nested groups.")
                .withOptionalArg().ofType(Boolean.class).defaultsTo(false);
        entriesForEachPrincipal = parser.accepts("entriesForEachPrincipal", "Create ACEs for each principal (vs rotating).")
                .withOptionalArg().ofType(Boolean.class).defaultsTo(false);
        compositionType = parser.accepts("compositionType", "Defines composition type for benchmarks with multiple authorization models.")
                .withOptionalArg().ofType(String.class)
                .defaultsTo(CompositeAuthorizationConfiguration.CompositionType.AND.name());
        useAggregationFilter = parser.accepts("useAggregationFilter", "Run principal-based tests with 'AggregationFilter'")
                .withOptionalArg().ofType(Boolean.class)
                .defaultsTo(Boolean.FALSE);
        evalType = parser.accepts("evalType", "Allows to switch between different evaluation types within a single benchmark")
                .withOptionalArg().ofType(String.class);
        batchSize = parser.accepts("batchSize", "Batch size before persisting operations.")
                .withOptionalArg().ofType(Integer.class).defaultsTo(AddMembersTest.DEFAULT_BATCH_SIZE);
        importBehavior = parser.accepts("importBehavior", "Protected Item Import Behavior")
                .withOptionalArg().ofType(String.class).defaultsTo(ImportBehavior.NAME_BESTEFFORT);
        itemsToRead = parser.accepts("itemsToRead", "Number of items to read")
                .withRequiredArg().ofType(Integer.class).defaultsTo(DEFAULT_ITEMS_TD_READ);
        repeatedRead = parser.accepts("repeatedRead", "Number of repetitions")
                .withRequiredArg().ofType(Integer.class).defaultsTo(DEFAULT_REPEATED_READ);
        concurrency = parser.accepts("concurrency", "Number of test threads.")
                .withRequiredArg().ofType(Integer.class).withValuesSeparatedBy(',');
        report = parser.accepts("report", "Whether to output intermediate results")
                .withOptionalArg().ofType(Boolean.class)
                .defaultsTo(Boolean.FALSE);
        randomUser = parser.accepts("randomUser", "Whether to use a random user to read.")
                .withOptionalArg().ofType(Boolean.class)
                .defaultsTo(Boolean.FALSE);
        csvFile = parser.accepts("csvFile", "File to write a CSV version of the benchmark data.")
                .withOptionalArg().ofType(File.class);
        flatStructure = parser.accepts("flatStructure", "Whether the test should use a flat structure or not.")
                .withOptionalArg().ofType(Boolean.class).defaultsTo(Boolean.FALSE);
        numberOfUsers = parser.accepts("numberOfUsers")
                .withOptionalArg().ofType(Integer.class).defaultsTo(10000);
        setScope = parser.accepts("setScope", "Whether to use include setScope in the user query.")
                .withOptionalArg().ofType(Boolean.class)
                .defaultsTo(Boolean.FALSE);
        reverseOrder = parser.accepts("reverseOrder", "Invert order of configurations in composite setup.")
                .withOptionalArg().ofType(Boolean.class)
                .defaultsTo(Boolean.FALSE);
        supportedPaths = parser.accepts("supportedPaths", "Supported paths in composite setup.")
                .withOptionalArg().ofType(String.class).withValuesSeparatedBy(',');
        dynamicMembership = parser.accepts("dynamicMembership", "Enable dynamic membership handling during synchronisation of external users.")
                .withOptionalArg().ofType(Boolean.class).defaultsTo(Boolean.FALSE);
        autoMembership = parser.accepts("autoMembership", "Ids of those groups a given external identity automatically become member of.")
                .withOptionalArg().ofType(String.class).withValuesSeparatedBy(',');
        roundtripDelay = parser.accepts("roundtripDelay", "Use simplified principal name lookup from ExtIdRef by specifying roundtrip delay of value < 0.")
                .withOptionalArg().ofType(Integer.class).defaultsTo(0);
        transientWrites = parser.accepts("transient", "Do not save data.")
                .withOptionalArg().ofType(Boolean.class)
                .defaultsTo(Boolean.FALSE);
        vgcMaxAge = parser.accepts("vgcMaxAge", "Continuous DocumentNodeStore VersionGC max age in sec (RDB only)")
                .withRequiredArg().ofType(Integer.class).defaultsTo(-1);
        coldSyncInterval = parser.accepts("coldSyncInterval", "interval between sync cycles in sec (Segment-Tar-Cold only)")
                .withRequiredArg().ofType(Integer.class).defaultsTo(5);
        coldUseDataStore = parser
                .accepts("useDataStore", "Whether to use a datastore in the cold standby topology (Segment-Tar-Cold only)")
                .withOptionalArg().ofType(Boolean.class)
                .defaultsTo(Boolean.TRUE);
        coldShareDataStore = parser
                .accepts("shareDataStore", "Whether to share the datastore for primary and standby in the cold standby topology (Segment-Tar-Cold only)")
                .withOptionalArg().ofType(Boolean.class)
                .defaultsTo(Boolean.FALSE);
        coldOneShotRun = parser
                .accepts("oneShotRun", "Whether to do a continuous sync between client and server or sync only once (Segment-Tar-Cold only)")
                .withOptionalArg().ofType(Boolean.class)
                .defaultsTo(Boolean.FALSE);
        coldSecure = parser
                .accepts("secure", "Whether to enable secure communication between primary and standby in the cold standby topology (Segment-Tar-Cold only)")
                .withOptionalArg().ofType(Boolean.class)
                .defaultsTo(Boolean.FALSE);

        cacheExpiration = parser
                .accepts("cacheExpiration", "Expiration time for the cache in milliseconds")
                .withOptionalArg().ofType(Long.class)
                .defaultsTo(0L);
        numberOfLocalGroups = parser
                .accepts("numberOfLocalGroups", "Number of local groups to add dynamic membership groups.")
                .withOptionalArg().ofType(Integer.class)
                .defaultsTo(0);

        verbose = parser.accepts("verbose", "Enable verbose output");
        nonOption = parser.nonOptions();
        help = parser.acceptsAll(asList("h", "?", "help"), "show help").forHelp();
        elasticHost = parser.accepts("elasticHost", "Elastic server host").withOptionalArg()
                .ofType(String.class);
        elasticScheme = parser.accepts("elasticScheme", "Elastic scheme").withOptionalArg()
                .ofType(String.class);
        elasticPort = parser.accepts("elasticPort", "Elastic scheme").withOptionalArg()
                .ofType(Integer.class);
        elasticApiKeyId = parser.accepts("elasticApiKeyId", "Elastic unique id of the API key").withOptionalArg()
                .ofType(String.class);
        elasticApiKeySecret = parser.accepts("elasticApiKeySecret", "Elastic generated API secret").withOptionalArg()
                .ofType(String.class);
        throttlingEnabled = parser
                .accepts("throttlingEnabled", "Whether throttling for Document Store is enabled or not")
                .withOptionalArg().ofType(Boolean.class)
                .defaultsTo(Boolean.FALSE); // throttling is disabled by default
    }

}
