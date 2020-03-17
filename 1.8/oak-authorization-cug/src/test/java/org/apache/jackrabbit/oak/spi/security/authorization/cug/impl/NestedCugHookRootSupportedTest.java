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
package org.apache.jackrabbit.oak.spi.security.authorization.cug.impl;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.junit.Test;

import static org.apache.jackrabbit.oak.commons.PathUtils.ROOT_PATH;
import static org.junit.Assert.assertTrue;

public class NestedCugHookRootSupportedTest extends NestedCugHookTest {

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        return ConfigurationParameters.of(AuthorizationConfiguration.NAME, ConfigurationParameters.of(
                CugConstants.PARAM_CUG_SUPPORTED_PATHS, new String[] {PathUtils.ROOT_PATH},
                CugConstants.PARAM_CUG_ENABLED, true));
    }

    @Test
    public void testAddAtRoot() throws Exception {
        createCug(ROOT_PATH, EveryonePrincipal.getInstance());
        root.commit();
        assertTrue(root.getTree("/").hasChild(REP_CUG_POLICY));

        createCug("/content", getTestGroupPrincipal());
        createCug("/content2", EveryonePrincipal.getInstance());
        root.commit();

        assertNestedCugs(root, getRootProvider(), ROOT_PATH, true, "/content", "/content2");
    }

    @Test
    public void testAddAtRoot2() throws Exception {
        createCug("/content", getTestGroupPrincipal());
        createCug("/content2", EveryonePrincipal.getInstance());
        root.commit();

        assertNestedCugs(root, getRootProvider(), ROOT_PATH, false, "/content", "/content2");

        createCug(ROOT_PATH, EveryonePrincipal.getInstance());
        root.commit();

        assertNestedCugs(root, getRootProvider(), ROOT_PATH, false);
        assertNestedCugs(root, getRootProvider(), ROOT_PATH, true, "/content", "/content2");
    }

    @Test
    public void testAddAtRoot3() throws Exception {
        createCug(ROOT_PATH, EveryonePrincipal.getInstance());
        createCug("/content", getTestGroupPrincipal());
        createCug("/content2", EveryonePrincipal.getInstance());
        root.commit();

        assertNestedCugs(root, getRootProvider(), ROOT_PATH, true, "/content", "/content2");
    }

    @Test
    public void testRemoveRootCug() throws Exception {
        // add cug at /
        createCug(ROOT_PATH, EveryonePrincipal.getInstance());
        createCug("/content", getTestGroupPrincipal());
        createCug("/content2", EveryonePrincipal.getInstance());
        root.commit();

        assertTrue(removeCug(ROOT_PATH, true));
        assertNestedCugs(root, getRootProvider(), ROOT_PATH, false, "/content", "/content2");

        assertTrue(removeCug("/content", true));
        assertNestedCugs(root, getRootProvider(), ROOT_PATH, false, "/content2");

        assertTrue(removeCug("/content2", true));
        assertNestedCugs(root, getRootProvider(), ROOT_PATH, false);
    }

    @Test
    public void testRemoveRootCug2() throws Exception {
        // add cug at /
        createCug(ROOT_PATH, EveryonePrincipal.getInstance());
        createCug("/content", getTestGroupPrincipal());
        createCug("/content2", EveryonePrincipal.getInstance());
        root.commit();

        removeCug("/content", true);
        assertNestedCugs(root, getRootProvider(), ROOT_PATH, true, "/content2");

        removeCug("/", true);
        assertNestedCugs(root, getRootProvider(), ROOT_PATH, false, "/content2");

        removeCug("/content2", true);
        assertNestedCugs(root, getRootProvider(), ROOT_PATH, false);
    }

    @Test
    public void testRemoveRootCug3() throws Exception {
        // add cug at /
        createCug(ROOT_PATH, EveryonePrincipal.getInstance());
        createCug("/content", getTestGroupPrincipal());
        createCug("/content2", EveryonePrincipal.getInstance());
        root.commit();

        assertTrue(removeCug("/content", true));
        assertNestedCugs(root, getRootProvider(), ROOT_PATH, true, "/content2");

        assertTrue(removeCug("/content2", true));
        assertNestedCugs(root, getRootProvider(), ROOT_PATH, true);

        assertTrue(removeCug(ROOT_PATH, true));
        assertNestedCugs(root, getRootProvider(), ROOT_PATH, false);
    }

    @Test
    public void testRemoveRootCug4() throws Exception {
        // add cug at /
        createCug("/", EveryonePrincipal.getInstance());
        createCug("/content", getTestGroupPrincipal());
        createCug("/content2", EveryonePrincipal.getInstance());
        root.commit();

        assertTrue(removeCug("/content", false));
        assertTrue(removeCug("/content2", false));
        assertTrue(removeCug("/", false));
        root.commit();

        assertNestedCugs(root, getRootProvider(), "/", false);
    }
}