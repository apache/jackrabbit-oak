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
import javax.jcr.version.OnParentVersionAction;
import javax.jcr.version.VersionManager;

import org.apache.jackrabbit.oak.spi.version.VersionConstants;
import org.apache.jackrabbit.test.AbstractJCRTest;

/**
 * Test OPV COPY
 */
public class OpvCopyTest extends AbstractJCRTest implements VersionConstants {

    private VersionManager versionManager;
    private Node frozen;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        Node n1 = testRootNode.addNode(nodeName1, NT_FILE);
        Node content = n1.addNode(JCR_CONTENT, NT_RESOURCE);
        content.setProperty(JCR_DATA, "val");

        assertEquals(OnParentVersionAction.VERSION, n1.getDefinition().getOnParentVersion());
        assertEquals(OnParentVersionAction.COPY, content.getDefinition().getOnParentVersion());

        testRootNode.addMixin(MIX_VERSIONABLE);
        superuser.save();

        versionManager = superuser.getWorkspace().getVersionManager();
        frozen = versionManager.checkpoint(testRoot).getFrozenNode();
    }

    public void testDirectChild() throws Exception {
        Node n1 = testRootNode.getNode(nodeName1);
        n1.addMixin(MIX_VERSIONABLE);
        superuser.save();

        Node frozedFile = versionManager.checkpoint(n1.getPath()).getFrozenNode();
        Node frozenContent = frozedFile.getNode(JCR_CONTENT);
        assertEquals(NT_FROZENNODE, frozenContent.getPrimaryNodeType().getName());
    }

    public void testChildInSubTree() throws Exception {
        Node frozenFile = frozen.getNode(nodeName1);

        Node frozenContent = frozenFile.getNode(JCR_CONTENT);
        assertEquals(NT_FROZENNODE, frozenContent.getPrimaryNodeType().getName());
    }
}