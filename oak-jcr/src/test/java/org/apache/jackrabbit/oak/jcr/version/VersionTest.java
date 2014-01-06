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
package org.apache.jackrabbit.oak.jcr.version;

import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.Property;
import javax.jcr.RepositoryException;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;
import javax.jcr.query.RowIterator;
import javax.jcr.version.Version;
import javax.jcr.version.VersionManager;

import org.apache.jackrabbit.test.AbstractJCRTest;
import org.apache.jackrabbit.test.NotExecutableException;

/**
 * <code>VersionTest</code> performs tests on JCR Version nodes.
 */
public class VersionTest extends AbstractJCRTest {

    public void testGetNodeByIdentifier() throws RepositoryException {
        Node n = testRootNode.addNode(nodeName1, testNodeType);
        n.addMixin(mixVersionable);
        superuser.save();
        VersionManager vMgr = superuser.getWorkspace().getVersionManager();
        String id = vMgr.getBaseVersion(n.getPath()).getIdentifier();
        assertTrue("Session.getNodeByIdentifier() did not return Version object for a nt:version node.",
                superuser.getNodeByIdentifier(id) instanceof Version);
    }

    @SuppressWarnings("deprecation")
    public void testGetNodeByUUID() throws RepositoryException {
        Node n = testRootNode.addNode(nodeName1, testNodeType);
        n.addMixin(mixVersionable);
        superuser.save();
        VersionManager vMgr = superuser.getWorkspace().getVersionManager();
        String uuid = vMgr.getBaseVersion(n.getPath()).getUUID();
        assertTrue("Session.getNodeByUUID() did not return Version object for a nt:version node.",
                superuser.getNodeByUUID(uuid) instanceof Version);
    }

    public void testVersionFromQuery()
            throws RepositoryException, NotExecutableException {
        Node n = testRootNode.addNode(nodeName1, testNodeType);
        n.addMixin(mixVersionable);
        superuser.save();
        VersionManager vMgr = superuser.getWorkspace().getVersionManager();
        vMgr.checkpoint(n.getPath());
        QueryManager qm = superuser.getWorkspace().getQueryManager();
        Version v = vMgr.getBaseVersion(n.getPath());
        Query q = qm.createQuery("//element(*, nt:version)[@jcr:uuid = '" +
                v.getIdentifier() + "']", Query.XPATH);
        NodeIterator nodes = q.execute().getNodes();
        assertTrue(nodes.hasNext());
        assertTrue(nodes.nextNode() instanceof Version);
        RowIterator rows = q.execute().getRows();
        assertTrue(rows.hasNext());
        assertTrue(rows.nextRow().getNode() instanceof Version);
    }

    public void testFrozenNode() throws RepositoryException {
        Node n = testRootNode.addNode(nodeName1, testNodeType);
        n.addMixin(mixVersionable);
        Node child = n.addNode(nodeName2, ntUnstructured);
        superuser.save();
        VersionManager vMgr = superuser.getWorkspace().getVersionManager();
        vMgr.checkpoint(n.getPath());
        Version v = vMgr.getBaseVersion(n.getPath());
        Node frozenChild = v.getFrozenNode().getNode(child.getName());
        assertEquals(ntFrozenNode, frozenChild.getPrimaryNodeType().getName());
    }

    // OAK-1009
    public void testFrozenUUID() throws RepositoryException {
        Node n = testRootNode.addNode(nodeName1, testNodeType);
        n.addMixin(mixVersionable);
        Node child = n.addNode(nodeName2, ntUnstructured);
        superuser.save();
        VersionManager vMgr = superuser.getWorkspace().getVersionManager();
        vMgr.checkpoint(n.getPath());
        Version v = vMgr.getBaseVersion(n.getPath());
        Node frozenChild = v.getFrozenNode().getNode(child.getName());
        assertEquals(child.getIdentifier(),
                frozenChild.getProperty(Property.JCR_FROZEN_UUID).getString());
    }
}
