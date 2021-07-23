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
package org.apache.jackrabbit.oak.security.user;

import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.junit.Test;

import javax.jcr.RepositoryException;
import javax.jcr.nodetype.ConstraintViolationException;

import static org.apache.jackrabbit.oak.spi.security.user.UserConstants.DEFAULT_SYSTEM_RELATIVE_PATH;
import static org.apache.jackrabbit.oak.spi.security.user.UserConstants.PARAM_SYSTEM_RELATIVE_PATH;
import static org.junit.Assert.assertTrue;

public class SystemRelativePathTest extends AbstractSecurityTest {

    private static final String REL_PATH = DEFAULT_SYSTEM_RELATIVE_PATH+"/subtree";

    @Override
    public void after() throws Exception {
        try {
            root.refresh();
        } finally {
            super.after();
        }
    }

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        return ConfigurationParameters.of(
                UserConfiguration.NAME,
                ConfigurationParameters.of(PARAM_SYSTEM_RELATIVE_PATH, REL_PATH));
    }

    @Test(expected = ConstraintViolationException.class)
    public void testDefaultRelPath() throws RepositoryException {
        try {
            getUserManager(root).createSystemUser("testDefaultRelPath", DEFAULT_SYSTEM_RELATIVE_PATH);
        } catch (RepositoryException e) {
            assertTrue(e.getMessage().contains(REL_PATH));
            throw e;
        }
    }

    @Test
    public void testRelPath() throws RepositoryException {
        User u = getUserManager(root).createSystemUser("testRelPath", REL_PATH);
        assertTrue(u.getPath().contains("/"+REL_PATH+"/"));
    }

    @Test
    public void testBelowRelPath() throws RepositoryException {
        User u = getUserManager(root).createSystemUser("testBelowRelPath", REL_PATH+"/subtree");
        assertTrue(u.getPath().contains("/"+REL_PATH+"/subtree/"));
    }
}