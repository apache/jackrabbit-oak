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
package org.apache.jackrabbit.oak.security.authorization.evaluation;

import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.junit.Test;

/**
 * @see <a href="https://issues.apache.org/jira/browse/OAK-2933">OAK-2933</a>
 */
public class MoveWithoutEntryCacheTest extends AbstractOakCoreTest {
    @Override
    public void before() throws Exception {
        super.before();

        setupPermission("/", testPrincipal, true, PrivilegeConstants.JCR_READ);
    }

    @Override
    public void after() throws Exception {
        super.after();
    }

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        return ConfigurationParameters.of(AuthorizationConfiguration.NAME, ConfigurationParameters.of("eagerCacheSize", 0));
    }

    /**
     * Similar to {@code org.apache.jackrabbit.oak.jcr.security.authorization.SessionMoveTest.testMoveAndAddProperty2()}
     * without having a permission-entry cache.
     *
     * @throws Exception
     */
    @Test
    public void testMoveAndAddProperty2() throws Exception {
        setupPermission("/a/b", testPrincipal, true,
                PrivilegeConstants.JCR_REMOVE_NODE,
                PrivilegeConstants.JCR_REMOVE_CHILD_NODES,
                PrivilegeConstants.REP_ADD_PROPERTIES);
        setupPermission("/a/bb", testPrincipal, true,
                PrivilegeConstants.JCR_ADD_CHILD_NODES,
                PrivilegeConstants.JCR_NODE_TYPE_MANAGEMENT);

        String siblingDestPath = "/a/bb/destination";

        Root testRoot = getTestRoot();
        testRoot.move("/a/b/c", siblingDestPath);

        Tree destTree = testRoot.getTree(siblingDestPath);
        destTree.setProperty("newProp", "val");
        testRoot.commit();
    }

    /**
     * Same as {@code org.apache.jackrabbit.oak.jcr.security.authorization.SessionMoveTest.testMoveAndRemoveProperty2()}
     * without having a permission-entry cache.
     *
     * @throws Exception
     */
    @Test
    public void testMoveAndRemoveProperty2() throws Exception {
        setupPermission("/a/b", testPrincipal, true,
                PrivilegeConstants.JCR_REMOVE_NODE,
                PrivilegeConstants.JCR_REMOVE_CHILD_NODES,
                PrivilegeConstants.REP_REMOVE_PROPERTIES);
        setupPermission("/a/bb", testPrincipal, true,
                PrivilegeConstants.JCR_ADD_CHILD_NODES,
                PrivilegeConstants.JCR_NODE_TYPE_MANAGEMENT);

        String siblingDestPath = "/a/bb/destination";

        Root testRoot = getTestRoot();
        testRoot.move("/a/b/c", siblingDestPath);

        Tree destTree = testRoot.getTree(siblingDestPath);
        destTree.removeProperty("cProp");
        testRoot.commit();
    }
}