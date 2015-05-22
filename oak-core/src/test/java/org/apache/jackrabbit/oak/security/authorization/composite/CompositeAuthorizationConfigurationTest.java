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
package org.apache.jackrabbit.oak.security.authorization.composite;

import java.security.Principal;
import java.util.Collections;

import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.junit.Test;

import static org.junit.Assert.assertSame;

public class CompositeAuthorizationConfigurationTest extends AbstractSecurityTest {

    private CompositeAuthorizationConfiguration compositeConfiguration;

    @Override
    public void before() throws Exception {
        super.before();
        compositeConfiguration = new CompositeAuthorizationConfiguration(getSecurityProvider());
    }

    @Test(expected = IllegalStateException.class)
    public void testEmptyGetAccessControlManager() {
        compositeConfiguration.getAccessControlManager(root, NamePathMapper.DEFAULT);
    }

    @Test(expected = IllegalStateException.class)
    public void testEmptyGetPermissionProvider() {
        compositeConfiguration.getPermissionProvider(root, adminSession.getWorkspaceName(), Collections.<Principal>emptySet());
    }

    @Test
    public void testEmptyGetRestrictionProvider() {
        assertSame(RestrictionProvider.EMPTY, compositeConfiguration.getRestrictionProvider());
    }
}