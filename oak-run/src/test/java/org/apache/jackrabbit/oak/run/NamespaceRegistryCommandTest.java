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

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.apache.jackrabbit.oak.plugins.name.Namespaces;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixture;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixtureProvider;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.namespace.NamespaceConstants;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * Tests for the {@link NamespaceRegistryCommand}.
 */
public class NamespaceRegistryCommandTest {

    private final NamespaceRegistryCommand cmd = new NamespaceRegistryCommand();
    private DocumentNodeStore store;

    @Before
    public void before() throws CommitFailedException {
        assumeTrue(MongoUtils.isAvailable());
        try {
            NodeStoreFixture fixture = NodeStoreFixtureProvider.create(cmd.getOptions(MongoUtils.URL, "--fix", "--read-write"));
            store = (DocumentNodeStore) fixture.getStore();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        NodeBuilder rootBuilder = store.getRoot().builder();
        new InitialContent().initialize(rootBuilder);
        NodeBuilder system = rootBuilder.getChildNode(JcrConstants.JCR_SYSTEM);
        NodeBuilder namespaces = system.getChildNode(NamespaceConstants.REP_NAMESPACES);
        namespaces.remove();
        Namespaces.setupNamespaces(rootBuilder.getChildNode(JcrConstants.JCR_SYSTEM));
        store.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        store.runBackgroundOperations();
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
    public void breakAndFix() throws Exception {
        NodeBuilder rootBuilder = store.getRoot().builder();
        NodeBuilder namespaces = rootBuilder.getChildNode(JcrConstants.JCR_SYSTEM).getChildNode(NamespaceConstants.REP_NAMESPACES);
        namespaces.setProperty("foo", "urn:foo");
        store.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        store.runBackgroundOperations();
        testCmd(new String[] { MongoUtils.URL, "--analyse" }, new String[] { "This namespace registry model is inconsistent. The inconsistency can be fixed.", "The repaired registry would contain the following mappings:", "foo -> urn:foo" });
        testCmd(new String[] { MongoUtils.URL, "--fix", "--read-write" }, new String[] { "This namespace registry model is consistent, containing the following mappings from prefixes to namespace uris:", "foo -> urn:foo" });
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
            cmd.execute(opts);
            printStream.flush();
            for (String expected : output) {
                String s = out.toString(StandardCharsets.UTF_8);
                assertTrue(s.contains(expected));
            }
        }
    }
}
