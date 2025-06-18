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

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.apache.jackrabbit.oak.plugins.name.Namespaces;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixtureProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

public class NamespaceRegistryCommandTest {

    private final NamespaceRegistryCommand cmd = new NamespaceRegistryCommand();

    @BeforeClass
    public static void assumeMongoDB() {
        assumeTrue(MongoUtils.isAvailable());
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
        Root root = getRoot(MongoUtils.URL, "--fix", "--read-write");
        Tree namespacesTree = root.getTree(Namespaces.NAMESPACES_PATH);
        assertTrue(namespacesTree.exists());
        namespacesTree.setProperty("foo", "urn:foo");
        root.commit();
        testCmd(new String[] { MongoUtils.URL, "--analyse" }, new String[] { "This namespace registry model is inconsistent. The inconsistency can be fixed.", "The repaired registry would contain the following mappings:", "foo -> urn:foo" });
        testCmd(new String[] { MongoUtils.URL, "--fix", "--read-write" }, new String[] { "This namespace registry model is consistent, containing the following mappings from prefixes to namespace uris:", "foo -> urn:foo" });
    }

    @Test
    public void mappings() throws Exception {
        testCmd(new String[] { MongoUtils.URL, "--analyse" }, new String[] { "This namespace registry model is consistent"});
        testCmd(new String[] { MongoUtils.URL, "--fix", "--mappings",  "foo=urn:foo", "--read-write" }, new String[] { "This namespace registry model is consistent, containing the following mappings from prefixes to namespace uris:", "foo -> urn:foo" });
    }

    private Root getRoot(String... opts) throws Exception {
        return new Oak(NodeStoreFixtureProvider.create(cmd.getOptions(opts)).getStore())
                .with(new OpenSecurityProvider())
                .createContentSession().getLatestRoot();
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
