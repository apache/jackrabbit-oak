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
package org.apache.jackrabbit.oak.jcr.delegate;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.AuthInfo;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.jcr.session.RefreshStrategy;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionAware;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.whiteboard.DefaultWhiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.stats.Clock;
import org.apache.jackrabbit.oak.stats.StatisticManager;
import org.jetbrains.annotations.NotNull;
import org.mockito.MockSettings;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

public abstract class AbstractDelegatorTest {

    private static final Logger log = LoggerFactory.getLogger(AbstractDelegatorTest.class);

    @NotNull
    static SessionDelegate mockSessionDelegate() {
        PermissionProvider pp = mock(PermissionProvider.class);
        return mockSessionDelegate(mockRoot(pp, true), pp);
    }
    
    @NotNull
    static SessionDelegate mockSessionDelegate(@NotNull Root root, @NotNull PermissionProvider pp) {
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        Whiteboard wb = new DefaultWhiteboard();
        StatisticManager statisticManager = new StatisticManager(wb, executorService);
        return spy(new SessionDelegate(mockContentSession(root), mockSecurityProvider(root, pp),
                RefreshStrategy.Composite.create(), new ThreadLocal<>(), statisticManager, new Clock.Virtual()));
    }

    @NotNull
    static Root mockRoot(@NotNull PermissionProvider pp, boolean permissionAware) {
        MockSettings s = withSettings();
        if (permissionAware) {
            s.extraInterfaces(PermissionAware.class);
        }
        Root r = mock(Root.class, s);
        Answer answer = invocationOnMock -> {
            pp.refresh();
            return invocationOnMock;
        };
        doAnswer(answer).when(r).refresh();
        doAnswer(answer).when(r).rebase();

        Tree t = mock(Tree.class);
        when(t.getChild(anyString())).thenReturn(t);
        when(r.getTree(anyString())).thenReturn(t);

        if (permissionAware) {
            when(((PermissionAware) r).getPermissionProvider()).thenReturn(pp);
        }
        return r;
    }

    @NotNull
    private static SecurityProvider mockSecurityProvider(@NotNull Root root, @NotNull PermissionProvider pp) {
        AuthorizationConfiguration authorizationConfiguration = mock(AuthorizationConfiguration.class);
        when(authorizationConfiguration.getPermissionProvider(root, Oak.DEFAULT_WORKSPACE_NAME, Collections.emptySet())).thenReturn(pp);

        SecurityProvider securityProvider = mock(SecurityProvider.class);
        when(securityProvider.getConfiguration(AuthorizationConfiguration.class)).thenReturn(authorizationConfiguration);
        return securityProvider;
    }

    @NotNull
    private static ContentSession mockContentSession(@NotNull Root root) {
        ContentSession cs = when(mock(ContentSession.class).getAuthInfo()).thenReturn(AuthInfo.EMPTY).getMock();
        when(cs.getWorkspaceName()).thenReturn(Oak.DEFAULT_WORKSPACE_NAME);
        when(cs.getLatestRoot()).thenReturn(root);
        when(root.getContentSession()).thenReturn(cs);
        return cs;
    }
}