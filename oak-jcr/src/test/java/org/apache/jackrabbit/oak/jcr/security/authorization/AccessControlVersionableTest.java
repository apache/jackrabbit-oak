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

import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.Node;
import javax.jcr.nodetype.NodeType;
import javax.jcr.version.VersionManager;
import java.util.Collections;

import static org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants.MIX_REP_ACCESS_CONTROLLABLE;
import static org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants.REP_GLOB;

public class AccessControlVersionableTest extends AbstractEvaluationTest {

    private String path;
    private VersionManager vMgr;

    @Before
    @Override
    protected void setUp() throws Exception {
        super.setUp();

        Node n = superuser.getNode(childNPath).addNode(nodeName3);
        n.addMixin(MIX_REP_ACCESS_CONTROLLABLE);
        n.addMixin(NodeType.MIX_VERSIONABLE);
        path = n.getPath();
        superuser.save();

        vMgr = superuser.getWorkspace().getVersionManager();
        vMgr.checkin(path);
    }

    @After
    @Override
    protected void tearDown() throws Exception {
        try {
            superuser.refresh(false);
            vMgr.checkout(path);
        } finally {
            super.tearDown();
        }
    }

    @Test
    public void testAddEntryOnCheckedIn() throws Exception {
        modify(path, EveryonePrincipal.getInstance(), readPrivileges, true, Collections.emptyMap());
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/OAK-8945">OAK-8945</a>
     */
    @Test
    public void testAddRestrictionOnCheckedIn() throws Exception {
        modify(path, EveryonePrincipal.getInstance(), readPrivileges, false, Collections.singletonMap(REP_GLOB, superuser.getValueFactory().createValue("/*")));
    }
}