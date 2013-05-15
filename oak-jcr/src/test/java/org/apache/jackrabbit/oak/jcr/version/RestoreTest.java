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
import javax.jcr.RepositoryException;
import javax.jcr.version.Version;
import javax.jcr.version.VersionManager;

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
    }
}
