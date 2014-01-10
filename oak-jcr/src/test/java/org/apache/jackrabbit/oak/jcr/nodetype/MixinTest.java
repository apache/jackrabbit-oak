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
package org.apache.jackrabbit.oak.jcr.nodetype;

import javax.jcr.Node;
import javax.jcr.nodetype.NoSuchNodeTypeException;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.test.AbstractJCRTest;
import org.apache.jackrabbit.test.NotExecutableException;

import static org.apache.jackrabbit.JcrConstants.NT_UNSTRUCTURED;

/**
 *
 */
public class MixinTest extends AbstractJCRTest {

    public void testRemoveMixinWithoutMixinProperty() throws Exception {
        Node node = testRootNode.addNode(
                "testRemoveMixinWithoutMixinProperty", NT_UNSTRUCTURED);
        superuser.save();
        try {
            node.removeMixin(JcrConstants.MIX_REFERENCEABLE);
            fail();
        } catch (NoSuchNodeTypeException e) {
            // success
        } finally {
            node.remove();
            superuser.save();
        }

    }

    public void testRemoveInheritedMixin() throws Exception {
        Node node = testRootNode.addNode(
                "testRemoveInheritedMixin", NT_UNSTRUCTURED);
        node.addMixin(JcrConstants.MIX_VERSIONABLE);
        superuser.save();

        try {
            node.removeMixin(JcrConstants.MIX_REFERENCEABLE);
            fail();
        } catch (NoSuchNodeTypeException e) {
            // success
        } finally {
            node.remove();
            superuser.save();
        }
    }

    public void testRemoveInheritedMixin2() throws Exception {
        try {
            Authorizable user = ((JackrabbitSession) superuser).getUserManager().getAuthorizable("admin");
            if (user == null) {
                throw new NotExecutableException();
            }

            Node node = superuser.getNode(user.getPath());
            assertTrue(node.isNodeType(JcrConstants.MIX_REFERENCEABLE));
            node.removeMixin(JcrConstants.MIX_REFERENCEABLE);
        }  catch (NoSuchNodeTypeException e) {
            // success
        } finally {
            superuser.refresh(false);
        }
    }

    public void testRemoveMixVersionable() throws Exception {
        Node node = testRootNode.addNode(nodeName1);
        node.addMixin(mixVersionable);
        superuser.save();

        node.removeMixin(mixVersionable);
        superuser.save();
    }

    public void testRemoveMixVersionable1() throws Exception {
        Node node = testRootNode.addNode(nodeName1);
        node.addMixin(mixReferenceable);
        node.addMixin(mixVersionable);
        superuser.save();

        node.removeMixin(mixVersionable);
        superuser.save();
    }

    public void testRemoveAddMixVersionable() throws Exception {
        Node node = testRootNode.addNode(nodeName1);
        node.addMixin(mixVersionable);
        superuser.save();
        String vhId = node.getVersionHistory().getUUID();

        node.removeMixin(mixVersionable);
        node.addMixin(mixVersionable);
        superuser.save();

        assertFalse(vhId.equals(node.getVersionHistory().getUUID()));
    }

    public void testRemoveAddMixVersionable1() throws Exception {
        Node node = testRootNode.addNode(nodeName1);
        node.addMixin(mixReferenceable);
        node.addMixin(mixVersionable);
        superuser.save();
        String vhId = node.getVersionHistory().getUUID();

        node.removeMixin(mixVersionable);
        node.addMixin(mixVersionable);
        superuser.save();

        assertEquals(vhId, node.getVersionHistory().getUUID());
    }
}