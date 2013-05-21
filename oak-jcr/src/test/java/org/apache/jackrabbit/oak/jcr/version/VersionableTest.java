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
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.version.VersionException;
import javax.jcr.version.VersionManager;

import org.apache.jackrabbit.test.AbstractJCRTest;

/**
 * <code>VersionableTest</code> contains tests for method relevant to
 * versionable nodes.
 */
public class VersionableTest extends AbstractJCRTest {

    public void testGetTypeOfPredecessors() throws RepositoryException {
        Node node = testRootNode.addNode(nodeName1, testNodeType);
        node.addMixin(mixVersionable);
        superuser.save();
        VersionManager vMgr = superuser.getWorkspace().getVersionManager();
        vMgr.checkin(node.getPath());
        assertEquals(PropertyType.nameFromValue(PropertyType.REFERENCE),
                PropertyType.nameFromValue(node.getProperty(jcrPredecessors).getType()));
    }

    public void testReadOnlyAfterCheckin() throws RepositoryException {
        Node node = testRootNode.addNode(nodeName1, testNodeType);
        node.addMixin(mixVersionable);
        superuser.save();
        VersionManager vMgr = superuser.getWorkspace().getVersionManager();
        vMgr.checkin(node.getPath());
        try {
            node.setProperty(propertyName1, "value");
            fail("setProperty() must fail on a checked-in node");
        } catch (VersionException e) {
            // expected
        }
    }

    public void testReferenceableChild() throws RepositoryException {
        Node node = testRootNode.addNode(nodeName1, ntUnstructured);
        node.addMixin(mixVersionable);
        Node child = node.addNode(nodeName2, ntUnstructured);
        child.addMixin(mixReferenceable);
        superuser.save();
        VersionManager vMgr = superuser.getWorkspace().getVersionManager();
        vMgr.checkin(node.getPath());
    }
}
