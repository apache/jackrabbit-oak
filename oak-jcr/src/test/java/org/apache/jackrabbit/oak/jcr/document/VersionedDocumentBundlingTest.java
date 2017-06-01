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

package org.apache.jackrabbit.oak.jcr.document;

import java.io.IOException;
import java.io.StringReader;

import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.version.Version;
import javax.jcr.version.VersionManager;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.commons.JcrUtils;
import org.apache.jackrabbit.commons.cnd.CndImporter;
import org.apache.jackrabbit.commons.cnd.ParseException;
import org.apache.jackrabbit.oak.NodeStoreFixtures;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeState;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.bundlor.BundlingConfigHandler;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.jackrabbit.oak.commons.PathUtils.concat;
import static org.apache.jackrabbit.oak.jcr.AbstractRepositoryTest.dispose;
import static org.apache.jackrabbit.oak.plugins.document.bundlor.DocumentBundlor.PROP_PATTERN;
import static org.junit.Assert.assertNull;

public class VersionedDocumentBundlingTest {
    public static final String TEST_NODE_TYPE = "[oak:Asset]\n" +
            " - * (UNDEFINED) multiple\n" +
            " - * (UNDEFINED)\n" +
            " + * (nt:base) = oak:TestNode VERSION";

    private NodeStoreFixture fixture;
    private NodeStore ns;
    private Repository repository;
    private Session s;
    private DocumentStore ds;

    @Before
    public void setUp() throws Exception {
        Assume.assumeTrue(NodeStoreFixtures.DOCUMENT_NS.isAvailable());
        fixture = NodeStoreFixtures.DOCUMENT_MEM;

        ns = fixture.createNodeStore();
        if (ns == null){
            return;
        }
        ds = ((DocumentNodeStore)ns).getDocumentStore();
        Oak oak = new Oak(ns);
        repository  = new Jcr(oak).createRepository();
        s = repository.login(new SimpleCredentials("admin", "admin".toCharArray()));
        configureBundling();
    }

    @After
    public void tearDown(){
        if (s != null){
            s.logout();
        }

        dispose(repository);
        if (ns != null){
            fixture.dispose(ns);
        }
    }

    @Test
    public void createVersionedNode() throws Exception{
        Node asset = JcrUtils.getOrCreateByPath("/bundlingtest/foo.png", "oak:Unstructured", "oak:Asset", s, false);
        asset.addMixin(JcrConstants.MIX_VERSIONABLE);
        Node content = asset.addNode("jcr:content", "oak:Unstructured");
        content.addNode("metadata", "oak:Unstructured");
        s.save();

        VersionManager vm = s.getWorkspace().getVersionManager();
        String assetPath = asset.getPath();
        vm.checkin(assetPath);

        String versionedPath = vm.getBaseVersion(assetPath).getNode("jcr:frozenNode").getPath();

        //Both normal node and versioned nodes should be bundled
        assertNull(getNodeDocument(concat(assetPath, "jcr:content")));
        assertNull(getNodeDocument(concat(versionedPath, "jcr:content")));
    }

    @Test
    public void restoreVersionedNode() throws Exception{
        String assetParentPath = "/bundlingtest/par";
        Node asset = JcrUtils.getOrCreateByPath(assetParentPath + "/foo.png", "oak:Unstructured", "oak:Asset", s, false);
        Node assetParent = s.getNode(assetParentPath);
        assetParent.addMixin(JcrConstants.MIX_VERSIONABLE);
        asset.addNode("jcr:content", "oak:Unstructured");
        s.save();

        VersionManager vm = s.getWorkspace().getVersionManager();
        Version version = vm.checkin(assetParentPath);
        vm.checkout(assetParentPath);

        asset.getNode("jcr:content").setProperty("foo1", "bar1");
        s.save();

        vm.restore(version, true);
    }

    private void configureBundling() throws ParseException, RepositoryException, IOException {
        CndImporter.registerNodeTypes(new StringReader(TEST_NODE_TYPE), s);
        Node bundlor = JcrUtils.getOrCreateByPath(BundlingConfigHandler.CONFIG_PATH, "oak:Unstructured", s);
        Node asset = bundlor.addNode("oak:Asset");
        asset.setProperty(PROP_PATTERN, new String[]{
                "jcr:content",
                "jcr:content/metadata",
                "jcr:content/renditions"
        });

        s.save();
    }

    private NodeDocument getNodeDocument(String path) {
        return ds.find(Collection.NODES, Utils.getIdFromPath(path));
    }
}
