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

import javax.jcr.AccessDeniedException;
import javax.jcr.Node;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.api.JackrabbitNode;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RenameTest... TODO
 */
@Ignore("OAK-770 : NodeImpl doesn't implement JackrabbitNode#rename")
public class RenameTest extends AbstractEvaluationTest {

    @Test
    public void testRename() throws Exception {
        Node child = testSession.getNode(childNPath);
        try {
            ((JackrabbitNode) child).rename("rename");
            testSession.save();
            fail("test session must not be allowed to rename nodes.");
        } catch (AccessDeniedException e) {
            // success.
        }

        // give 'add_child_nodes' and 'nt-management' privilege
        // -> not sufficient privileges for a renaming of the child
        allow(path, privilegesFromNames(new String[] {Privilege.JCR_ADD_CHILD_NODES, Privilege.JCR_NODE_TYPE_MANAGEMENT}));
        try {
            ((JackrabbitNode) child).rename("rename");
            testSession.save();
            fail("test session must not be allowed to rename nodes.");
        } catch (AccessDeniedException e) {
            // success.
        }

        // add 'remove_child_nodes' at 'path
        // -> rename of child must now succeed
        allow(path, privilegesFromName(Privilege.JCR_REMOVE_CHILD_NODES));
        ((JackrabbitNode) child).rename("rename");
        testSession.save();
    }
}