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
package org.apache.jackrabbit.oak.spi.security.authentication.external.impl.jmx;

import java.util.Map;
import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.spi.security.authentication.external.AbstractExternalAuthTest;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentity;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalUser;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncContext;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncResult;
import org.apache.jackrabbit.oak.spi.security.authentication.external.TestIdentityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncContext;
import org.junit.Before;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public abstract class AbstractJmxTest extends AbstractExternalAuthTest {

    ExternalIdentityProvider foreignIDP;

    @Before
    public void before() throws Exception {
        super.before();

        foreignIDP = new TestIdentityProvider("anotherIDP");
    }

    static void assertResultMessages(@Nonnull String[] resultMessages, String uid, @Nonnull String expectedOperation) {
        assertResultMessages(resultMessages, ImmutableMap.of(uid, expectedOperation));
    }

    static void assertResultMessages(@Nonnull String[] resultMessages, @Nonnull Map<String, String> expected) {
        assertEquals(expected.size(), resultMessages.length);
        for (int i = 0; i < resultMessages.length; i++) {
            String rm = resultMessages[i];
            String op = rm.substring(rm.indexOf(":") + 2, rm.indexOf("\","));

            int index = rm.indexOf("uid:\"") + 5;
            String uid = rm.substring(index, rm.indexOf("\",", index));

            assertTrue(expected.containsKey(uid));
            assertEquals(expected.get(uid), op);
        }
    }

    static void assertSync(@Nonnull ExternalIdentity ei, @Nonnull UserManager userManager) throws Exception {
        Authorizable authorizable;
        if (ei instanceof ExternalUser) {
            authorizable = userManager.getAuthorizable(ei.getId(), User.class);
        } else {
            authorizable = userManager.getAuthorizable(ei.getId(), Group.class);
        }
        assertNotNull(ei.getId(), authorizable);
        assertEquals(ei.getId(), authorizable.getID());
        assertEquals(ei.getExternalId(), ExternalIdentityRef.fromString(authorizable.getProperty(DefaultSyncContext.REP_EXTERNAL_ID)[0].getString()));
    }

    SyncResult sync(@Nonnull ExternalIdentityProvider idp, @Nonnull String id, boolean isGroup) throws Exception {
        return sync((isGroup) ? idp.getGroup(id) : idp.getUser(id), idp);
    }

    SyncResult sync(@Nonnull ExternalIdentity externalIdentity, @Nonnull ExternalIdentityProvider idp) throws Exception {
        SyncContext ctx = new DefaultSyncContext(syncConfig, idp, getUserManager(root), getValueFactory(root));
        SyncResult res = ctx.sync(externalIdentity);
        root.commit();
        return res;
    }

    UserManager getUserManager() {
        root.refresh();
        return getUserManager(root);
    }
}