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

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.Repository;

import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityProviderManager;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncHandler;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncManager;
import org.apache.jackrabbit.oak.spi.security.authentication.external.TestIdentityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncConfig;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncHandler;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class SyncMBeanImplTest {

    private static final String SYNC_NAME = "testSyncName";

    private static Repository REPOSITORY;

    private ExternalIdentityProvider idp;
    private SyncManager syncMgr;
    ExternalIdentityProviderManager idpMgr;

    private SyncMBeanImpl syncMBean;

    @BeforeClass
    public static void beforeClass() {
        REPOSITORY = new Jcr().createRepository();
    }

    @Before
    public void before() {
        // TODO : proper setup
        idp = new TestIdentityProvider();
        syncMgr = new SyncManager() {
            @CheckForNull
            @Override
            public SyncHandler getSyncHandler(@Nonnull String name) {
                if (SYNC_NAME.equals(name)) {
                    return new DefaultSyncHandler(new DefaultSyncConfig());
                } else {
                    return null;
                }
            }
        };
        idpMgr = new ExternalIdentityProviderManager() {
            @CheckForNull
            @Override
            public ExternalIdentityProvider getProvider(@Nonnull String name) {
                if (name.equals(idp.getName())) {
                    return idp;
                } else {
                    return null;
                }
            }
        };
        syncMBean = new SyncMBeanImpl(REPOSITORY, syncMgr, SYNC_NAME, idpMgr, idp.getName());
    }

    @Test
    public void testGetSyncHandlerName() {
        assertEquals(SYNC_NAME, syncMBean.getSyncHandlerName());
    }

    @Test
    public void testInvalidSyncHandlerName() {
        SyncMBeanImpl syncMBean = new SyncMBeanImpl(REPOSITORY, syncMgr, "invalid", idpMgr, idp.getName());
        assertEquals("invalid", syncMBean.getSyncHandlerName());

        // calling any sync-operation must fail due to the invalid configuration
        try {
            syncMBean.syncAllExternalUsers();
            fail("syncAllExternalUsers with invalid SyncHandlerName must fail");
        } catch (IllegalArgumentException e) {
            //success
        }
    }

    @Test
    public void testGetIDPName() {
        assertEquals(idp.getName(), syncMBean.getIDPName());
    }

    @Test
    public void testInvalidIDPName() {
        SyncMBeanImpl syncMBean = new SyncMBeanImpl(REPOSITORY, syncMgr, SYNC_NAME, idpMgr, "invalid");
        assertEquals("invalid", syncMBean.getIDPName());

        // calling any sync-operation must fail due to the invalid configuration
        try {
            syncMBean.syncAllExternalUsers();
            fail("syncAllExternalUsers with invalid IDP name must fail");
        } catch (IllegalArgumentException e) {
            //success
        }
    }

    @Test
    public void testSyncUsers() {
        // TODO
    }

    @Test
    public void testSyncAllUsers() {
        // TODO
    }

    @Test
    public void testSyncExternalUsers() {
        // TODO
    }

    @Test
    public void testSyncAllExternalUsers() {
        // TODO
    }

    @Test
    public void testListOrphanedUsers() {
        // TODO
    }

    @Test
    public void testPurgeOrphanedUsers() {
        // TODO
    }
}