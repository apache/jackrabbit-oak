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

import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.security.user.monitor.UserMonitor;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.whiteboard.DefaultWhiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardAware;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;

import static org.mockito.Mockito.mock;

public abstract class AbstractUserTest extends AbstractSecurityTest {

    final UserMonitor monitor = mock(UserMonitor.class);
    final DynamicMembershipTracker dynamicMembershipService = new DynamicMembershipTracker();

    @Before
    public void before() throws Exception {
        super.before();
        SecurityProvider sp = getSecurityProvider();
        Whiteboard wb = null;
        if (sp instanceof WhiteboardAware) {
            wb = ((WhiteboardAware)sp).getWhiteboard();
        }
        if (wb == null) {
            wb = new DefaultWhiteboard();
        }
        dynamicMembershipService.start(wb);
    }
    
    @After
    public void after() throws Exception {
        try {
            dynamicMembershipService.stop();
        } finally {
            super.after();
        }
    }
    
    protected UserMonitor getUserMonitor() {
        return monitor;
    }
    
    protected UserManagerImpl createUserManagerImpl(@NotNull Root root) {
        return new UserManagerImpl(root, getPartialValueFactory(), getSecurityProvider(), getUserMonitor(), dynamicMembershipService);
    }
    
}