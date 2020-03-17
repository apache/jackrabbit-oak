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
package org.apache.jackrabbit.oak.upgrade;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.commons.cnd.CndImporter;
import org.junit.Test;

import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.nodetype.NodeDefinition;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.NodeTypeManager;
import javax.jcr.nodetype.PropertyDefinition;
import java.io.InputStreamReader;
import java.io.Reader;

import static org.junit.Assert.assertEquals;

public class CopyNodeTypesUpgradeTest extends AbstractRepositoryUpgradeTest {

    @Override
    protected void createSourceContent(Session session) throws Exception {
        final Reader cnd = new InputStreamReader(getClass().getResourceAsStream("/test-nodetypes.cnd"));
        CndImporter.registerNodeTypes(cnd, session);
    }

    @Test
    public void customNodeTypesAreRegistered() throws RepositoryException {
        final JackrabbitSession adminSession = createAdminSession();
        final NodeTypeManager nodeTypeManager = adminSession.getWorkspace().getNodeTypeManager();
        final NodeType testFolderNodeType = nodeTypeManager.getNodeType("test:Folder");
        final NodeDefinition[] cnd = testFolderNodeType.getChildNodeDefinitions();
        final PropertyDefinition[] pd = testFolderNodeType.getPropertyDefinitions();
        assertEquals("More than one child node definition", 1, cnd.length);
        assertEquals("Incorrect default primary type", "test:Folder", cnd[0].getDefaultPrimaryTypeName());
        assertEquals("More than two property definitions", 4, pd.length);
        adminSession.logout();
    }
}
