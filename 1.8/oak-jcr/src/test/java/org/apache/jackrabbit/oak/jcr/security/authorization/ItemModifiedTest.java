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
package org.apache.jackrabbit.oak.jcr.security.authorization;

import javax.jcr.Node;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.junit.Test;

/**
 * Tests for <a href="https://issues.apache.org/jira/browse/OAK-1177">OAK-1177</a>
 */
public class ItemModifiedTest extends AbstractEvaluationTest {

    @Test
    public void testModified() throws Exception {
        Node child = superuser.getNode(path).addNode("child");
        superuser.save();

        allow(path, privilegesFromName(PrivilegeConstants.JCR_READ));

        Node n = testSession.getNode(path);
        assertFalse(n.isModified());
    }

    @Test
    public void testModified2() throws Exception {
        Node child = superuser.getNode(path).addNode("child");
        superuser.save();

        //Deny access to one of the child node
        deny(child.getPath(), privilegesFromName(PrivilegeConstants.JCR_READ));

        Node n = testSession.getNode(path);
        assertFalse(n.isModified());
    }

    @Test
    public void testModified3() throws Exception {
        Node child = superuser.getNode(path).addNode("child", JcrConstants.NT_UNSTRUCTURED);
        child.addNode("a");
        child.addNode("b");
        superuser.save();

        Node n = testSession.getNode(child.getPath());
        assertFalse(n.isModified());
    }

    @Test
    public void testModified4() throws Exception {
        //Deny access to properties
        deny(path, privilegesFromName(PrivilegeConstants.REP_READ_PROPERTIES));

        Node n = testSession.getNode(childNPath);
        assertFalse(n.isModified());
    }
}