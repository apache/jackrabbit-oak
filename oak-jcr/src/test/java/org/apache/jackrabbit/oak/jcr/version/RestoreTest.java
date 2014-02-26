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
import javax.jcr.Property;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.nodetype.PropertyDefinition;
import javax.jcr.version.Version;
import javax.jcr.version.VersionManager;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.test.AbstractJCRTest;

/**
 * <code>RestoreTest</code>...
 */
public class RestoreTest extends AbstractJCRTest {

    public void testSimpleRestore() throws RepositoryException {
        VersionManager vMgr = superuser.getWorkspace().getVersionManager();
        Node n = testRootNode.addNode(nodeName1, testNodeType);
        n.addMixin(mixVersionable);
        n.setProperty("prop", "a");
        superuser.save();
        String path = n.getPath();
        Version v = vMgr.checkpoint(path); // 1.0
        n.setProperty("prop", "b");
        superuser.save();
        vMgr.checkpoint(path); // 1.1
        n.remove();
        superuser.save();
        vMgr.restore(path, v, true);
        assertTrue(superuser.nodeExists(path));
        n = superuser.getNode(path);
        assertEquals("Property not restored", "a", n.getProperty("prop").getString());
        Property vhProp = n.getProperty(jcrVersionHistory);
        assertEquals(PropertyType.REFERENCE, vhProp.getType());
        PropertyDefinition def = vhProp.getDefinition();
        assertEquals(PropertyType.REFERENCE, def.getRequiredType());
    }

    public void testRestoreReferenceableChild() throws RepositoryException {
        VersionManager vMgr = superuser.getWorkspace().getVersionManager();
        Node n = testRootNode.addNode(nodeName1, "test:versionable");
        n.addMixin(mixVersionable);
        n.setProperty("prop", "a");
        Node child = n.addNode("test:copyOnParentVersion", ntUnstructured);
        child.addMixin(mixReferenceable);
        child.setProperty("prop", "a");
        superuser.save();
        String path = n.getPath();
        Version v = vMgr.checkpoint(path); // 1.0
        n.setProperty("prop", "b");
        child.setProperty("prop", "b");
        superuser.save();
        vMgr.checkpoint(path); // 1.1
        vMgr.restore(v, true);
        assertEquals("Property not restored", "a", n.getProperty("prop").getString());
        assertEquals("Property not restored", "a", child.getProperty("prop").getString());
        assertFalse("Restored node must not have jcr:frozenPrimaryType property",
                child.hasProperty(JcrConstants.JCR_FROZENPRIMARYTYPE));
    }
}
