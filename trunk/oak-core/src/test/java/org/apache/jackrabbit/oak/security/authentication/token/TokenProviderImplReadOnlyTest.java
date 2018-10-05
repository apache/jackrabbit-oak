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
package org.apache.jackrabbit.oak.security.authentication.token;

import javax.jcr.GuestCredentials;
import javax.jcr.security.AccessControlManager;

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenInfo;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenProvider;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TokenProviderImplReadOnlyTest extends AbstractTokenTest {

    private ContentSession cs;
    private Root readOnlyRoot;
    private TokenProvider readOnlyTp;

    @Override
    public void before() throws Exception {
        super.before();

        AccessControlManager acMgr = getAccessControlManager(root);
        String userPath = getTestUser().getPath();
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, userPath);
        acl.addAccessControlEntry(EveryonePrincipal.getInstance(), privilegesFromNames(PrivilegeConstants.JCR_READ));
        acMgr.setPolicy(userPath, acl);
        root.commit();

        cs = login(new GuestCredentials());
        readOnlyRoot = cs.getLatestRoot();
        readOnlyTp = new TokenProviderImpl(readOnlyRoot, getTokenConfig(), getUserConfiguration());
    }

    private String generateToken() throws Exception {
        TokenInfo info = tokenProvider.createToken(getTestUser().getID(), ImmutableMap.<String, Object>of());
        String token = info.getToken();
        readOnlyRoot.refresh();
        return token;
    }

    @Test
    public void testCreateToken() throws Exception {
        String userId = getTestUser().getID();
        readOnlyRoot.refresh();

        assertNull(readOnlyTp.createToken(userId, ImmutableMap.<String, Object>of()));
    }

    @Test
    public void testCreateToken2() throws Exception {
        // make sure user already has a token-parent node.
        generateToken();
        // now generate a new token with the read-only root
        assertNull(readOnlyTp.createToken(getTestUser().getID(), ImmutableMap.<String, Object>of()));
    }

    @Test
    public void testGetTokenInfo() throws Exception {
        TokenInfo readOnlyInfo = readOnlyTp.getTokenInfo(generateToken());
        assertNotNull(readOnlyInfo);
    }

    @Test
    public void testRefreshToken() throws Exception {
        TokenInfo readOnlyInfo = readOnlyTp.getTokenInfo(generateToken());
        assertFalse(readOnlyInfo.resetExpiration(System.currentTimeMillis() + TokenProviderImpl.DEFAULT_TOKEN_EXPIRATION - 100));
    }

    @Test
    public void testRemoveToken() throws Exception {
        TokenInfo readOnlyInfo = readOnlyTp.getTokenInfo(generateToken());
        assertFalse(readOnlyInfo.remove());
    }
}