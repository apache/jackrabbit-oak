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
package org.apache.jackrabbit.oak.core;

import java.security.Principal;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.security.auth.Subject;

import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.AuthInfoImpl;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.OpenAuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class MutableRootTest {

    private final NodeStore store = new MemoryNodeStore();
    private final TestPermissionProvider permissionProvider = new TestPermissionProvider();

    private MutableRoot root;

    @Before
    public void before() {
        SecurityProvider sp = new OpenSecurityProvider() {
            @Nonnull
            @Override
            public <T> T getConfiguration(@Nonnull Class<T> configClass) {
                if (AuthorizationConfiguration.class == configClass) {
                    return (T) new OpenAuthorizationConfiguration() {
                        @Nonnull
                        @Override
                        public PermissionProvider getPermissionProvider(@Nonnull Root root, @Nonnull String workspaceName, @Nonnull Set<Principal> principals) {
                            return permissionProvider;
                        }
                    };
                } else {
                    return super.getConfiguration(configClass);
                }
            }
        };
        ContentSessionImpl cs = Mockito.mock(ContentSessionImpl.class);
        when(cs.toString()).thenReturn("contentSession");
        when(cs.getAuthInfo()).thenReturn(AuthInfoImpl.EMPTY);
        when(cs.getWorkspaceName()).thenReturn("default");
        root = new MutableRoot(store, new EmptyHook(), "default", new Subject(), sp, null, null, cs);
    }

    /**
     * @see <a href"https://issues.apache.org/jira/browse/OAK-5355">OAK-5355</a>
     */
    @Test
    public void testCommit() throws Exception {
        MutableTree t = root.getTree(PathUtils.ROOT_PATH);
        NodeBuilder nb = t.getNodeBuilder();
        assertTrue(nb instanceof SecureNodeBuilder);

        assertEquals(canReadRootTree(t), nb.exists());

        // commit resets the permissionprovider, which in our test scenario alters
        // the 'denyAll' flag.
        root.commit();

        assertEquals(canReadRootTree(t), nb.exists());

        MutableTree t2 = root.getTree(PathUtils.ROOT_PATH);
        NodeBuilder nb2 = t.getNodeBuilder();

        assertEquals(canReadRootTree(t2), nb.exists());
        assertEquals(nb2.exists(), nb.exists());
    }

    private boolean canReadRootTree(@Nonnull Tree t) {
        return permissionProvider.getTreePermission(t, TreePermission.EMPTY).canRead();
    }
}