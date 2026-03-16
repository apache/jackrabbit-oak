/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.osgi;

import org.junit.After;
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
import org.osgi.framework.BundleContext;
import org.osgi.framework.Version;

import javax.inject.Inject;
import javax.jcr.Node;
import javax.jcr.PropertyType;
import javax.jcr.Repository;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.query.Query;
import javax.jcr.query.QueryResult;
import javax.jcr.query.RowIterator;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.ops4j.pax.exam.CoreOptions.bundle;
import static org.ops4j.pax.exam.CoreOptions.frameworkProperty;
import static org.ops4j.pax.exam.CoreOptions.junitBundles;
import static org.ops4j.pax.exam.CoreOptions.mavenBundle;
import static org.ops4j.pax.exam.CoreOptions.systemProperties;
import static org.ops4j.pax.exam.CoreOptions.vmOption;

@RunWith(PaxExam.class)
@ExamReactorStrategy(PerClass.class)
public class LuceneNgMigrationIT {

    @Inject
    private BundleContext context;

    @Inject
    private Repository repository;

    private Session session;

    @Configuration
    public Option[] configuration() throws IOException, URISyntaxException {
        // VERBATIM COPY of OSGiIT.configuration() - update both if you change this
        return CoreOptions.options(
                junitBundles(),
                // require at least DS 1.4 supported by SCR 2.1.0+
                mavenBundle("org.apache.felix", "org.apache.felix.scr", "2.1.28"),
                // transitive deps of Felix SCR 2.1.x
                mavenBundle("org.osgi", "org.osgi.util.promise", "1.1.1"),
                mavenBundle("org.osgi", "org.osgi.util.function", "1.1.0"),
                mavenBundle("org.apache.felix", "org.apache.felix.jaas", "1.0.2"),
                mavenBundle("org.osgi", "org.osgi.dto", "1.0.0"),
                // require at least ConfigAdmin 1.6 supported by felix.configadmin 1.9.0+
                mavenBundle( "org.apache.felix", "org.apache.felix.configadmin", "1.9.20" ),
                mavenBundle( "org.apache.felix", "org.apache.felix.fileinstall", "3.2.6" ),
                mavenBundle( "org.ops4j.pax.logging", "pax-logging-api", "1.7.2" ),
                // Jackson dependency for object serialisation.
                mavenBundle().groupId("com.fasterxml.jackson.core").artifactId("jackson-core").version("2.17.2"),
                mavenBundle().groupId("com.fasterxml.jackson.core").artifactId("jackson-annotations").version("2.17.2"),
                mavenBundle().groupId("com.fasterxml.jackson.core").artifactId("jackson-databind").version("2.17.2"),

                frameworkProperty("repository.home").value("target"),
                systemProperties(new SystemPropertyOption("felix.fileinstall.dir").value(getConfigDir())),
                jarBundles(),
                jpmsOptions());
    }

    private Option jpmsOptions() {
        DefaultCompositeOption composite = new DefaultCompositeOption();
        if (Version.parseVersion(System.getProperty("java.specification.version")).getMajor() > 1) {
            if (java.nio.file.Files.exists(java.nio.file.FileSystems.getFileSystem(
                    URI.create("jrt:/")).getPath("modules", "java.se.ee"))) {
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

    private String getConfigDir() {
        return new File(new File("src", "test"), "config").getAbsolutePath();
    }

    private Option jarBundles() throws MalformedURLException {
        DefaultCompositeOption composite = new DefaultCompositeOption();
        for (File bundle : new File("target", "test-bundles").listFiles()) {
            if (bundle.getName().endsWith(".jar") && bundle.isFile()) {
                composite.add(bundle(bundle.toURI().toURL().toString()));
            }
        }
        return composite;
    }

    @Before
    public void setUp() throws Exception {
        session = repository.login(new SimpleCredentials("admin", "admin".toCharArray()));
        Node content = session.getRootNode().addNode("content");
        content.addNode("page-a").setProperty("title", "Apache Jackrabbit Oak");
        content.addNode("page-b").setProperty("title", "Jackrabbit search scalable");
        content.addNode("page-c").setProperty("title", "Oak Lucene index");
        session.save();
    }

    @After
    public void tearDown() throws Exception {
        if (session != null) {
            // Clean up test content so the container can be reused (PerClass shares it)
            if (session.getRootNode().hasNode("content")) {
                session.getRootNode().getNode("content").remove();
            }
            if (session.getRootNode().hasNode("oak:index/migrationIdx")) {
                session.getRootNode().getNode("oak:index/migrationIdx").remove();
            }
            session.save();
            session.logout();
        }
    }

    @Test
    public void testMigrationFromLegacyToNg() throws Exception {
        List<String> expected = Arrays.asList("/content/page-a", "/content/page-b");

        // Step 1: type=lucene — legacy provider serves
        Node idx = session.getRootNode().getNode("oak:index").addNode("migrationIdx");
        idx.setPrimaryType("oak:QueryIndexDefinition");
        idx.setProperty("type", "lucene");
        idx.setProperty("reindex", true);
        session.save();
        waitForReindex(session, "/oak:index/migrationIdx");

        waitForPlan(session, "jackrabbit", "lucene:migrationIdx");
        assertResults(session, "jackrabbit", expected);

        // Step 2: dual-write, legacy still serves
        idx.setProperty("activeTarget", "lucene");
        idx.setProperty("storeTargets", new String[]{"lucene", "lucene9"}, PropertyType.STRING);
        idx.setProperty("reindex", true);
        session.save();
        waitForReindex(session, "/oak:index/migrationIdx");

        waitForPlan(session, "jackrabbit", "lucene:migrationIdx");
        assertResults(session, "jackrabbit", expected);

        // Step 3: flip to lucene9 — Ng serves
        idx.setProperty("activeTarget", "lucene9");
        session.save();

        waitForPlan(session, "jackrabbit", "lucene9:migrationIdx");
        assertResults(session, "jackrabbit", expected);
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private String explainQuery(Session session, String term) throws Exception {
        String sql = "EXPLAIN SELECT * FROM [nt:base] WHERE CONTAINS(title, '" + term + "')";
        QueryResult result = session.getWorkspace().getQueryManager()
                .createQuery(sql, Query.JCR_SQL2).execute();
        return result.getRows().nextRow().getValue("plan").getString();
    }

    private List<String> queryPaths(Session session, String term) throws Exception {
        String sql = "SELECT * FROM [nt:base] WHERE CONTAINS(title, '" + term + "')";
        QueryResult result = session.getWorkspace().getQueryManager()
                .createQuery(sql, Query.JCR_SQL2).execute();
        List<String> paths = new ArrayList<>();
        RowIterator rows = result.getRows();
        while (rows.hasNext()) {
            paths.add(rows.nextRow().getPath());
        }
        Collections.sort(paths);
        return paths;
    }

    private void waitForPlan(Session session, String term, String fragment) throws Exception {
        long deadline = System.currentTimeMillis() + 10_000;
        String plan = "";
        while (System.currentTimeMillis() < deadline) {
            session.refresh(true);
            plan = explainQuery(session, term);
            if (plan.contains(fragment)) return;
            Thread.sleep(200);
        }
        fail("Plan did not contain '" + fragment + "' within 10 s. Last plan: " + plan);
    }

    private void waitForReindex(Session session, String indexPath) throws Exception {
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            session.refresh(true);
            Node idx = session.getNode(indexPath);
            if (!idx.hasProperty("reindex") || !idx.getProperty("reindex").getBoolean()) {
                return;
            }
            Thread.sleep(200);
        }
        fail("Reindex did not complete within 30 s for " + indexPath);
    }

    private void assertResults(Session session, String term, List<String> expected) throws Exception {
        List<String> actual = queryPaths(session, term);
        assertEquals("Query results mismatch for term '" + term + "'", expected, actual);
    }
}
