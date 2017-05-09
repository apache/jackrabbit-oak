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
package org.apache.jackrabbit.oak.spi.security.authentication.external.basic;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.security.authentication.external.AbstractExternalAuthTest;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncResult;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncedIdentity;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RepExternalIdTest extends AbstractExternalAuthTest {

    private Root r;
    private UserManager userManager;
    private DefaultSyncContext syncCtx;

    @Before
    public void before() throws Exception {
        super.before();

        r = getSystemRoot();
        userManager = getUserManager(r);
        syncCtx = new DefaultSyncContext(syncConfig, idp, getUserManager(r), getValueFactory(r));
    }

    @Override
    public void after() throws Exception {
        try {
            syncCtx.close();
        } finally {
            super.after();
        }
    }

    private void assertRepExternalId(@Nonnull SyncResult result) throws Exception {
        assertSame(SyncResult.Status.ADD, result.getStatus());
        SyncedIdentity si = result.getIdentity();
        assertNotNull(si);


        Authorizable authorizable = userManager.getAuthorizable(si.getId());
        assertNotNull(authorizable);

        Tree userTree = r.getTree(authorizable.getPath());
        assertTrue(userTree.hasProperty(DefaultSyncContext.REP_EXTERNAL_ID));

        PropertyState ps = userTree.getProperty(DefaultSyncContext.REP_EXTERNAL_ID);
        assertNotNull(ps);
        assertFalse(ps.isArray());
        assertSame(Type.STRING, ps.getType());
        assertEquals(si.getExternalIdRef(), ExternalIdentityRef.fromString(ps.getValue(Type.STRING)));
    }

    @Test
    public void syncExternalUser() throws Exception {
        SyncResult res = syncCtx.sync(idp.getUser(USER_ID));

        assertRepExternalId(res);
    }

    @Test
    public void syncExternalGroup() throws Exception {
        SyncResult res = syncCtx.sync(idp.listGroups().next());

        assertRepExternalId(res);
    }

    @Test
    public void testUniqueConstraint() throws Exception {
        SyncResult res = syncCtx.sync(idp.getUser(USER_ID));

        try {
            Tree t = r.getTree(getTestUser().getPath());
            t.setProperty(DefaultSyncContext.REP_EXTERNAL_ID, res.getIdentity().getExternalIdRef().getString());
            r.commit();
            fail("Duplicate value for rep:externalId must be detected in the default setup.");
        } catch (CommitFailedException e) {
            // success: verify nature of the exception
            assertTrue(e.isConstraintViolation());
            assertEquals(30, e.getCode());
        } finally {
            r.refresh();
        }
    }

    @Test
    public void testUniqueConstraintSubsequentCommit() throws Exception {
        SyncResult res = syncCtx.sync(idp.getUser(USER_ID));
        r.commit();

        try {
            Tree t = r.getTree(getTestUser().getPath());
            t.setProperty(DefaultSyncContext.REP_EXTERNAL_ID, res.getIdentity().getExternalIdRef().getString());
            r.commit();
            fail("Duplicate value for rep:externalId must be detected in the default setup.");
        } catch (CommitFailedException e) {
            // success: verify nature of the exception
            assertTrue(e.isConstraintViolation());
            assertEquals(30, e.getCode());
        } finally {
            r.refresh();
        }
    }

    @Test
    public void testUniqueConstraintNonUserNode() throws Exception {
        try {
            SyncResult res = syncCtx.sync(idp.getUser(USER_ID));
            Tree nonUserTree = r.getTree("/");
            nonUserTree.setProperty(DefaultSyncContext.REP_EXTERNAL_ID, res.getIdentity().getExternalIdRef().getString());
            r.commit();

            fail("Duplicate value for rep:externalId must be detected in the default setup.");
        } catch (CommitFailedException e) {
            // success: verify nature of the exception
            assertTrue(e.isConstraintViolation());
            assertEquals(30, e.getCode());
        } finally {
            r.refresh();
        }
    }
}