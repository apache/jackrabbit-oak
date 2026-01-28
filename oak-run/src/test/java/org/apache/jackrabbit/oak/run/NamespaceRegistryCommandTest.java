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
package org.apache.jackrabbit.oak.run;

import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.apache.jackrabbit.oak.plugins.name.Namespaces;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixture;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixtureProvider;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.namespace.NamespaceConstants;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * Tests for the {@link NamespaceRegistryCommand}.
 */
public class NamespaceRegistryCommandTest {

    private static final NamespaceRegistryCommand CMD = new NamespaceRegistryCommand();
    private static DocumentNodeStore STORE;
    private Oak oak;

    @BeforeClass
    public static void setupStore() {
        assumeTrue(MongoUtils.isAvailable());
        try {
            NodeStoreFixture fixture = NodeStoreFixtureProvider.create(CMD.getOptions(MongoUtils.URL, "--fix", "--read-write"));
            STORE = (DocumentNodeStore) fixture.getStore();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Before
    public void before() throws Exception {
        //Make sure all revisions are stable
        Thread.sleep(1000);
        STORE.runBackgroundOperations();
        oak = new Oak(STORE)
                .with(new OpenSecurityProvider())
                .with((RepositoryInitializer) rootBuilder -> {
                    new InitialContent().initialize(rootBuilder);
                    NodeBuilder system = rootBuilder.getChildNode(JCR_SYSTEM);
                    NodeBuilder namespaces = system.getChildNode(NamespaceConstants.REP_NAMESPACES);
                    namespaces.remove();
                    Namespaces.setupNamespaces(rootBuilder.getChildNode(JCR_SYSTEM));
                    try {
                        STORE.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
                    } catch (CommitFailedException e) {
                        throw new RuntimeException(e);
                    }
                });
        oak.createContentRepository();
        STORE.runBackgroundOperations();
    }

    @Test
    public void analyse() throws Exception {
        testCmd(new String[] { MongoUtils.URL, "--analyse" }, new String[] { "This namespace registry model is consistent, containing the following mappings from prefixes to namespace uris:" });
    }

    @Test
    public void fix() throws Exception {
        testCmd(new String[] { MongoUtils.URL, "--fix" }, new String[] { "The namespace registry is already consistent. No action is required." });
    }

    @Test
    public void breakAndFixNoReverseMapping() throws Exception {
        try (ContentSession contentSession = oak.createContentSession()) {
            Root root = contentSession.getLatestRoot();
            Tree namespaces = root.getTree(NamespaceConstants.NAMESPACES_PATH);
            //inconsistent mapping: no reverse mapping
            namespaces.setProperty("foo", "urn:foo");
            root.commit();
        }
        STORE.runBackgroundOperations();
        //complete information: automatic fix
        testCmd(new String[] { MongoUtils.URL, "--analyse" }, new String[] { "This namespace registry model is inconsistent. The inconsistency can be fixed.", "The repaired registry would contain the following mappings:", "foo -> urn:foo" });
        testCmd(new String[] { MongoUtils.URL, "--fix", "--read-write" }, new String[] { "This namespace registry model is consistent, containing the following mappings from prefixes to namespace uris:", "foo -> urn:foo" });
    }

    @Test
    public void breakAndFixPrefixAmbiguity() throws Exception {
        try (ContentSession contentSession = oak.createContentSession()) {
            Root root = contentSession.getLatestRoot();
            Tree namespaces = root.getTree(NamespaceConstants.NAMESPACES_PATH);
            Tree nsdata = namespaces.getChild(NamespaceConstants.REP_NSDATA);
            //inconsistent mapping: one URI, two prefixes
            namespaces.setProperty("foo", "urn:foo");
            nsdata.setProperty(Namespaces.encodeUri("urn:foo"), "bar");
            root.commit();
        }
        STORE.runBackgroundOperations();
        //ambiguous information: no automatic fix
        testCmd(new String[] { MongoUtils.URL, "--analyse" }, new String[] { "This namespace registry model is inconsistent. The inconsistency can NOT be fixed." });
        //consistent with supplied specific mapping
        testCmd(new String[] { MongoUtils.URL, "--analyse", "--mappings", "foo=urn:foo" }, new String[] { "This namespace registry model is consistent, containing the following mappings from prefixes to namespace uris:", "foo -> urn:foo" });
        testCmd(new String[] { MongoUtils.URL, "--fix", "--read-write", "--mappings", "foo=urn:foo" }, new String[] { "This namespace registry model is consistent, containing the following mappings from prefixes to namespace uris:", "foo -> urn:foo" });
    }

    @Test
    public void breakAndFixUriAmbiguity() throws Exception {
        try (ContentSession contentSession = oak.createContentSession()) {
            Root root = contentSession.getLatestRoot();
            Tree namespaces = root.getTree(NamespaceConstants.NAMESPACES_PATH);
            Tree nsdata = namespaces.getChild(NamespaceConstants.REP_NSDATA);
            //inconsistent mapping: one prefix, two URIs
            namespaces.setProperty("foo", "urn:foo");
            nsdata.setProperty(Namespaces.encodeUri("urn:bar"), "foo");
            root.commit();
        }
        STORE.runBackgroundOperations();
        //ambiguous information: no automatic fix
        testCmd(new String[] { MongoUtils.URL, "--analyse" }, new String[] { "This namespace registry model is inconsistent. The inconsistency can NOT be fixed." });
        //consistent with supplied specific mapping
        testCmd(new String[] { MongoUtils.URL, "--analyse", "--mappings", "foo=urn:foo" }, new String[] { "This namespace registry model is consistent, containing the following mappings from prefixes to namespace uris:", "foo -> urn:foo" });
        testCmd(new String[] { MongoUtils.URL, "--fix", "--read-write", "--mappings", "foo=urn:foo" }, new String[] { "This namespace registry model is consistent, containing the following mappings from prefixes to namespace uris:", "foo -> urn:foo" });
    }

    @Test
    public void breakAndFixDanglingPrefix() throws Exception {
        try (ContentSession contentSession = oak.createContentSession()) {
            Root root = contentSession.getLatestRoot();
            Tree namespaces = root.getTree(NamespaceConstants.NAMESPACES_PATH);
            Tree nsdata = namespaces.getChild(NamespaceConstants.REP_NSDATA);
            //adding a prefix without any mapping to an URI
            Iterable<String> prefixes = Objects.requireNonNull(nsdata.getProperty(NamespaceConstants.REP_PREFIXES)).getValue(STRINGS);
            List<String> newValue = new ArrayList<>();
            prefixes.forEach(newValue::add);
            newValue.add("foo");
            nsdata.setProperty(NamespaceConstants.REP_PREFIXES, newValue, STRINGS);
            root.commit();
        }
        STORE.runBackgroundOperations();
        //missing information: no automatic fix
        testCmd(new String[] { MongoUtils.URL, "--analyse" }, new String[] { "This namespace registry model is inconsistent. The inconsistency can NOT be fixed." });
        //consistent after removal of unmapped data.
        testCmd(new String[] { MongoUtils.URL, "--analyse", "--prune" }, new String[] { "This namespace registry model is consistent" });
        //consistent with supplied complete information.
        testCmd(new String[] { MongoUtils.URL, "--analyse", "--mappings",  "foo=urn:foo" }, new String[] { "This namespace registry model is consistent, containing the following mappings from prefixes to namespace uris:", "foo -> urn:foo" });
        testCmd(new String[] { MongoUtils.URL, "--fix", "--read-write" }, new String[] { "This namespace registry model is inconsistent. The inconsistency can NOT be fixed." });
        testCmd(new String[] { MongoUtils.URL, "--fix", "--read-write", "--prune" }, new String[] { "This namespace registry model is consistent" });
    }

    @Test
    public void breakAndFixDanglingUri() throws Exception {
        try (ContentSession contentSession = oak.createContentSession()) {
            Root root = contentSession.getLatestRoot();
            Tree namespaces = root.getTree(NamespaceConstants.NAMESPACES_PATH);
            Tree nsdata = namespaces.getChild(NamespaceConstants.REP_NSDATA);
            //adding an URI without any mapping to a prefix
            Iterable<String> prefixes = Objects.requireNonNull(nsdata.getProperty(NamespaceConstants.REP_URIS)).getValue(STRINGS);
            List<String> newValue = new ArrayList<>();
            prefixes.forEach(newValue::add);
            newValue.add("urn:foo");
            nsdata.setProperty(NamespaceConstants.REP_URIS, newValue, STRINGS);
            root.commit();
        }
        STORE.runBackgroundOperations();
        //missing information: no automatic fix
        testCmd(new String[] { MongoUtils.URL, "--analyse" }, new String[] { "This namespace registry model is inconsistent. The inconsistency can NOT be fixed." });
        //consistent after removal of unmapped data.
        testCmd(new String[] { MongoUtils.URL, "--analyse", "--prune" }, new String[] { "This namespace registry model is consistent" });
        //consistent with supplied complete information.
        testCmd(new String[] { MongoUtils.URL, "--analyse", "--mappings",  "foo=urn:foo" }, new String[] { "This namespace registry model is consistent, containing the following mappings from prefixes to namespace uris:", "foo -> urn:foo" });
        testCmd(new String[] { MongoUtils.URL, "--fix", "--read-write" }, new String[] { "This namespace registry model is inconsistent. The inconsistency can NOT be fixed." });
        testCmd(new String[] { MongoUtils.URL, "--fix", "--read-write", "--prune" }, new String[] { "This namespace registry model is consistent" });
    }

    @Test
    public void mappings() throws Exception {
        testCmd(new String[] { MongoUtils.URL, "--analyse" }, new String[] { "This namespace registry model is consistent"});
        testCmd(new String[] { MongoUtils.URL, "--fix", "--mappings",  "foo=urn:foo", "--read-write" }, new String[] { "This namespace registry model is consistent, containing the following mappings from prefixes to namespace uris:", "foo -> urn:foo" });
    }

    private void testCmd(String[] opts, String[] output) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try(PrintStream printStream = new PrintStream(out)) {
            System.setOut(printStream);
            CMD.execute(opts);
            printStream.flush();
            for (String expected : output) {
                String s = out.toString(StandardCharsets.UTF_8);
                assertTrue(s.contains(expected));
            }
        }
    }
}
