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
package org.apache.jackrabbit.oak.osgi;

import static org.junit.Assert.assertTrue;
import static org.ops4j.pax.exam.CoreOptions.bundle;
import static org.ops4j.pax.exam.CoreOptions.frameworkProperty;
import static org.ops4j.pax.exam.CoreOptions.junitBundles;
import static org.ops4j.pax.exam.CoreOptions.mavenBundle;
import static org.ops4j.pax.exam.CoreOptions.systemProperties;
import static org.ops4j.pax.exam.CoreOptions.vmOption;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticConnection;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexTracker;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticMetricHandler;
import org.apache.jackrabbit.oak.plugins.index.elastic.index.ElasticIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.ElasticIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.CoreOptions;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.options.DefaultCompositeOption;
import org.ops4j.pax.exam.options.SystemPropertyOption;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerClass;
import org.osgi.framework.Version;

/**
 * OSGi integration test that mirrors {@link IndexVersionSelectionTest} but runs inside a Pax Exam
 * OSGi container so that bundle classloaders naturally separate the two conflicting Lucene versions:
 * <ul>
 *   <li>{@code oak-lucene} bundle embeds Lucene 4.x (inlined classes)</li>
 *   <li>{@code oak-search-elastic} bundle embeds Lucene 9.x (via Bundle-ClassPath)</li>
 * </ul>
 * Because each bundle's classloader resolves {@code org.apache.lucene.*} from its own embedded
 * copy, there is no {@code NoClassDefFoundError} for {@code org.apache.lucene.util.ResourceLoader}.
 * This allows {@link ElasticIndexEditorProvider} to be registered normally, so the Elasticsearch
 * index is created during {@code root.commit()} and no manual index creation is needed.
 * <p>
 * Index definitions are built directly via the Oak Tree API (no {@code IndexDefinitionBuilder})
 * to avoid importing {@code oak-search} packages that are not exported by any installed bundle.
 * <p>
 * Elasticsearch is started via reflection in {@link #configuration()} (which runs in the JUnit
 * runner's classloader, before Felix is launched) and the connection URL is passed into the OSGi
 * container as a system property. This avoids any bytecode reference to test-jar or Testcontainers
 * classes in the probe bundle.
 */
@RunWith(PaxExam.class)
@ExamReactorStrategy(PerClass.class)
public class IndexVersionSelectionIT {

    private static final String ELASTIC_INDEX_PREFIX =
            "oak-it-" + Long.toHexString(System.currentTimeMillis());

    private ElasticConnection elasticConnection;
    private ContentRepository repo;
    private ContentSession session;

    @Before
    public void setUp() throws Exception {
        String connStr = System.getProperty("elasticConnectionString");
        Assume.assumeNotNull(
                "No Elasticsearch available — set elasticConnectionString or provide Docker",
                connStr);

        // Lucene 4.x SPI (SPIClassIterator) uses Thread.currentThread().getContextClassLoader()
        // (TCCL) to discover codec/format implementations. The static initializers of Codec and
        // PostingsFormat run the first time those classes are accessed — normally during
        // root.commit() / query execution, where the TCCL is not oak-lucene's classloader.
        // That causes SPIClassIterator to load Lucene40PostingsFormat from the system classpath
        // while PostingsFormat comes from oak-lucene's inlined copy → ClassCastException.
        // Fix: force Codec (and transitively PostingsFormat) static initialization here, with
        // TCCL set to oak-lucene's classloader, before Oak ever triggers it.
        ClassLoader luceneClassLoader = LuceneIndexProvider.class.getClassLoader();
        ClassLoader savedTccl = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(luceneClassLoader);
        LuceneIndexProvider luceneProvider;
        LuceneIndexEditorProvider luceneEditorProvider;
        try {
            Class.forName("org.apache.lucene.codecs.Codec", true, luceneClassLoader);
            luceneProvider = new LuceneIndexProvider();
            luceneEditorProvider = new LuceneIndexEditorProvider();
        } finally {
            Thread.currentThread().setContextClassLoader(savedTccl);
        }

        // The Elasticsearch Java client embeds jakarta.json-api and parsson in oak-search-elastic.
        // JsonpUtils.provider() uses ServiceLoader with the TCCL to find JsonProvider. When the
        // HTTP worker threads (created by RestClient) have the wrong TCCL, they find parsson on
        // the system classpath — but JsonProvider comes from the embedded JAR → "not a subtype".
        // Fix: set TCCL to oak-search-elastic's classloader while building ElasticConnection so
        // the HTTP worker threads inherit the correct TCCL, and ServiceLoader finds parsson from
        // the embedded JAR (same classloader as JsonProvider).
        ClassLoader elasticClassLoader = ElasticConnection.class.getClassLoader();
        Thread.currentThread().setContextClassLoader(elasticClassLoader);
        ElasticIndexTracker elasticTracker;
        ElasticIndexProvider elasticProvider;
        ElasticIndexEditorProvider elasticEditorProvider;
        try {
            URI uri = new URI(connStr);
            elasticConnection = ElasticConnection.newBuilder()
                    .withIndexPrefix(ELASTIC_INDEX_PREFIX)
                    .withConnectionParameters(uri.getScheme(), uri.getHost(), uri.getPort())
                    .build();
            elasticTracker = new ElasticIndexTracker(
                    elasticConnection, new ElasticMetricHandler(StatisticsProvider.NOOP));
            elasticProvider = new ElasticIndexProvider(elasticTracker);
            // ElasticIndexEditorProvider can be registered here without NoClassDefFoundError
            // because the class is loaded via oak-search-elastic's classloader which has
            // Lucene 9.x on its Bundle-ClassPath.
            elasticEditorProvider =
                    new ElasticIndexEditorProvider(elasticTracker, elasticConnection, null);
        } finally {
            Thread.currentThread().setContextClassLoader(savedTccl);
        }

        repo = new Oak(new MemoryNodeStore())
                .with(new InitialContent())
                .with(new OpenSecurityProvider())
                .with((QueryIndexProvider) luceneProvider)
                .with((Observer) luceneProvider)
                .with(luceneEditorProvider)
                .with((QueryIndexProvider) elasticProvider)
                .with((Observer) elasticTracker)
                .with(elasticEditorProvider)
                .with(new PropertyIndexEditorProvider())
                .createContentRepository();

        session = repo.login(null, null);
    }

    @After
    public void tearDown() throws Exception {
        if (session != null) {
            session.close();
        }
        if (elasticConnection != null) {
            elasticConnection.close();
        }
    }

    /**
     * Adds a fulltext index definition directly via the Oak Tree API, without using
     * {@code IndexDefinitionBuilder} or any {@code oak-search} classes.
     */
    private void addIndexDefinition(Tree oakIndex, String name, String type, boolean highCost) {
        Tree def = oakIndex.addChild(name);
        def.setProperty("jcr:primaryType", "oak:QueryIndexDefinition", Type.NAME);
        def.setProperty("type", type);
        def.setProperty("tags", Arrays.asList("myTag", type), Type.STRINGS);
        def.setProperty("selectionPolicy", "tag");

        Tree indexRules = def.addChild("indexRules");
        indexRules.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        Tree ntBase = indexRules.addChild("nt:base");
        ntBase.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        Tree properties = ntBase.addChild("properties");
        properties.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        Tree assetProp = properties.addChild("asset");
        assetProp.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        assetProp.setProperty("analyzed", true);
        assetProp.setProperty("name", "asset");

        if (highCost) {
            def.setProperty("costPerEntry", 1_000_000.0);
            def.setProperty("costPerExecution", 1_000_000.0);
        }
    }

    /**
     * Verifies that the latest index version (asset-10-custom-3, of type {@code newType}) is
     * selected even when its cost is set to a very high value (1 million), proving that
     * version-based selection takes precedence over cost. The index family is:
     * <pre>
     *   asset-10          (oldType)
     *   asset-10-custom-1 (oldType)
     *   asset-10-custom-2 (oldType)
     *   asset-10-custom-3 (newType, costPerEntry=costPerExecution=1_000_000)
     * </pre>
     * Unlike {@link IndexVersionSelectionTest#latestElasticsearchVersionIsUsedEvenWithHigherCost()},
     * this test does not manually create the Elasticsearch index: {@link ElasticIndexEditorProvider}
     * is registered in the Oak setup above and creates it automatically during {@code root.commit()}.
     */
    private void testLatestVersionIsUsedEvenWithHigherCost(String oldType, String newType)
            throws Exception {
        Root root = session.getLatestRoot();
        Tree oakIndex = root.getTree("/oak:index");

        for (String name : new String[]{"asset-10", "asset-10-custom-1", "asset-10-custom-2"}) {
            addIndexDefinition(oakIndex, name, oldType, false);
        }
        addIndexDefinition(oakIndex, "asset-10-custom-3", newType, true);

        root.getTree("/").addChild("content").setProperty("asset", "test-value");
        root.commit();

        root = session.getLatestRoot();
        Result result = root.getQueryEngine().executeQuery(
                "explain select * from [nt:base] where contains([asset], 'test-value')" +
                        " option(index tag myTag)",
                "JCR-SQL2",
                QueryEngine.NO_BINDINGS,
                QueryEngine.NO_MAPPINGS);

        String plan = result.getRows().iterator().next().getValue("plan").getValue(Type.STRING);

        // Version selection keeps only asset-10-custom-3 (the latest). The contains() constraint
        // prevents traversal, so the high-cost index must be used.
        assertTrue("Expected asset-10-custom-3 to be used, but got: " + plan,
                plan.contains("asset-10-custom-3"));
    }

    @Test
    public void latestLuceneVersionIsUsedEvenWithHigherCost() throws Exception {
        testLatestVersionIsUsedEvenWithHigherCost("elasticsearch", "lucene");
    }

    @Test
    public void latestElasticsearchVersionIsUsedEvenWithHigherCost() throws Exception {
        testLatestVersionIsUsedEvenWithHigherCost("lucene", "elasticsearch");
    }

    @Configuration
    public Option[] configuration() throws IOException, URISyntaxException {
        // This method runs in the JUnit runner's classloader, before Felix is launched.
        // We start Elasticsearch here (via reflection, to keep the probe bytecode free of
        // test-jar / Testcontainers references) and pass the URL as a system property.
        String connStr = System.getProperty("elasticConnectionString");
        if (connStr == null) {
            connStr = startElasticViaReflection();
        }
        if (connStr != null) {
            // Make the URL available to @Before (probe classloader reads System.getProperty)
            System.setProperty("elasticConnectionString", connStr);
        }

        DefaultCompositeOption options = new DefaultCompositeOption(
                junitBundles(),
                // require at least DS 1.4 supported by SCR 2.1.0+
                mavenBundle("org.apache.felix", "org.apache.felix.scr", "2.1.28"),
                // transitive deps of Felix SCR 2.1.x
                mavenBundle("org.osgi", "org.osgi.util.promise", "1.1.1"),
                mavenBundle("org.osgi", "org.osgi.util.function", "1.1.0"),
                mavenBundle("org.apache.felix", "org.apache.felix.jaas", "1.0.2"),
                mavenBundle("org.osgi", "org.osgi.dto", "1.0.0"),
                // require at least ConfigAdmin 1.6 supported by felix.configadmin 1.9.0+
                mavenBundle("org.apache.felix", "org.apache.felix.configadmin", "1.9.20"),
                mavenBundle("org.apache.felix", "org.apache.felix.fileinstall", "3.2.6"),
                mavenBundle("org.ops4j.pax.logging", "pax-logging-api", "1.7.2"),

                // Jackson dependency for object serialisation.
                // (these only need to be defined here when the versions are different from the ones
                // defined in the project -- otherwise -> "bundle symbolic name and version are not unique")
                // mavenBundle().groupId("com.fasterxml.jackson.core").artifactId("jackson-core").version("2.20.2"),
                // mavenBundle().groupId("com.fasterxml.jackson.core").artifactId("jackson-annotations").version("2.20"),
                // mavenBundle().groupId("com.fasterxml.jackson.core").artifactId("jackson-databind").version("2.20.2"),

                mavenBundle().groupId("com.github.ben-manes.caffeine").artifactId("caffeine").version("3.1.8"),

                frameworkProperty("repository.home").value("target"),

                systemProperties(new SystemPropertyOption("felix.fileinstall.dir").value(getConfigDir())),
                jarBundles(),
                jpmsOptions());
        if (connStr != null) {
            options.add(systemProperties(new SystemPropertyOption("elasticConnectionString").value(connStr)));
        }
        return CoreOptions.options(options);
    }

    /**
     * Starts Elasticsearch via reflection so that this class has no direct bytecode reference
     * to {@code ElasticTestServer} or {@code ElasticsearchContainer}. Both classes are
     * available on the Maven test classpath (test-jar and Testcontainers) but are not OSGi
     * bundles, so a direct import would prevent the probe bundle from resolving.
     *
     * @return connection URL (e.g. {@code http://localhost:9200}), or {@code null} if Docker
     *         is not available
     */
    private static String startElasticViaReflection() {
        try {
            Class<?> serverClass = Class.forName(
                    "org.apache.jackrabbit.oak.plugins.index.elastic.ElasticTestServer");
            Object container = serverClass.getMethod("getESTestServer").invoke(null);
            String host = (String) container.getClass().getMethod("getHost").invoke(container);
            int port = (Integer) container.getClass()
                    .getMethod("getMappedPort", int.class).invoke(container, 9200);
            return "http://" + host + ":" + port;
        } catch (Exception ignored) {
            return null;
        }
    }

    private String getConfigDir() {
        return new File(new File("src", "test"), "config").getAbsolutePath();
    }

    private Option jarBundles() throws MalformedURLException {
        DefaultCompositeOption composite = new DefaultCompositeOption();
        for (File f : new File("target", "test-bundles").listFiles()) {
            if (f.getName().endsWith(".jar") && f.isFile()) {
                composite.add(bundle(f.toURI().toURL().toString()));
            }
        }
        return composite;
    }

    private Option jpmsOptions() {
        DefaultCompositeOption composite = new DefaultCompositeOption();
        if (Version.parseVersion(System.getProperty("java.specification.version")).getMajor() > 1) {
            if (java.nio.file.Files.exists(java.nio.file.FileSystems
                    .getFileSystem(URI.create("jrt:/")).getPath("modules", "java.se.ee"))) {
                composite.add(vmOption("--add-modules=java.se.ee"));
            }
            composite.add(vmOption("--add-opens=java.base/jdk.internal.loader=ALL-UNNAMED"));
            composite.add(vmOption("--add-opens=java.base/java.lang=ALL-UNNAMED"));
            composite.add(vmOption("--add-opens=java.base/java.lang.invoke=ALL-UNNAMED"));
            composite.add(vmOption("--add-opens=java.base/java.io=ALL-UNNAMED"));
            composite.add(vmOption("--add-opens=java.base/java.net=ALL-UNNAMED"));
            composite.add(vmOption("--add-opens=java.base/java.nio=ALL-UNNAMED"));
            composite.add(vmOption("--add-opens=java.base/java.util=ALL-UNNAMED"));
            composite.add(vmOption("--add-opens=java.base/java.util.jar=ALL-UNNAMED"));
            composite.add(vmOption("--add-opens=java.base/java.util.regex=ALL-UNNAMED"));
            composite.add(vmOption("--add-opens=java.base/java.util.zip=ALL-UNNAMED"));
            composite.add(vmOption("--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"));
        }
        return composite;
    }
}
