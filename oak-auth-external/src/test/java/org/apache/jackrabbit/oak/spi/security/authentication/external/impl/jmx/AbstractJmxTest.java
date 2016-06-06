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

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentity;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalUser;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncContext;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncResult;
import org.apache.jackrabbit.oak.spi.security.authentication.external.TestIdentityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncConfig;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncContext;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public abstract class AbstractJmxTest {

    static Repository REPOSITORY;

    Session session;
    UserManager userManager;

    DefaultSyncConfig syncConfig;

    ExternalIdentityProvider idp;
    ExternalIdentityProvider foreignIDP;

    private Set<String> ids;

    @BeforeClass
    public static void beforeClass() {
        REPOSITORY = new Jcr().createRepository();
    }

    @Before
    public void before() throws Exception {
        session = REPOSITORY.login(new SimpleCredentials("admin", "admin".toCharArray()));
        if (!(session instanceof JackrabbitSession)) {
            throw new IllegalStateException();
        } else {
            userManager = ((JackrabbitSession) session).getUserManager();
        }
        ids = Sets.newHashSet(getAllAuthorizableIds(userManager));

        syncConfig = new DefaultSyncConfig();
        syncConfig.user().setMembershipNestingDepth(1);

        idp = new TestIdentityProvider();
        foreignIDP = new TestIdentityProvider("anotherIDP");
    }

    @After
    public void after() throws Exception {
        try {
            session.refresh(false);
            Iterator<String> iter = getAllAuthorizableIds(userManager);
            while (iter.hasNext()) {
                String id = iter.next();
                if (!ids.remove(id)) {
                    Authorizable a = userManager.getAuthorizable(id);
                    if (a != null) {
                        a.remove();
                    }
                }
            }
            session.save();
        } finally {
            session.logout();
        }
    }

    private static Iterator<String> getAllAuthorizableIds(@Nonnull UserManager userManager) throws Exception {
        Iterator<Authorizable> it = userManager.findAuthorizables("jcr:primaryType", null);
        return Iterators.filter(Iterators.transform(it, new Function<Authorizable, String>() {
            @Nullable
            @Override
            public String apply(Authorizable input) {
                try {
                    if (input != null) {
                        return input.getID();
                    }
                } catch (RepositoryException e) {
                    // failed to retrieve ID
                }
                return null;
            }
        }), Predicates.notNull());
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
        SyncContext ctx = new DefaultSyncContext(syncConfig, idp, userManager, session.getValueFactory());
        SyncResult res = ctx.sync(externalIdentity);
        session.save();
        return res;
    }
}