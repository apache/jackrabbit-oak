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
package org.apache.jackrabbit.oak.fixture;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.spi.audit.AuditDispatch;
import org.apache.jackrabbit.oak.spi.security.audit.SecurityAuditDomain;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.junit.After;
import org.junit.Test;

/**
 * Sanity coverage for {@link OakFixture#getMemoryNSWithAudit(long)}.
 * <p>
 * Phase 3 of the audit-SPI work measured the wrong shape because the
 * default {@code Oak-MemoryNS} fixture does not wire the audit pipeline
 * — capture sites silently routed to {@code AuditDispatch.NOOP}. This test
 * fails the build if the audit-enabled fixture ever regresses into the
 * same silent-audit-OFF state.
 */
public class MemoryNSWithAuditFixtureTest {

    private OakFixture fixture;

    @After
    public void tearDown() {
        if (fixture != null) {
            fixture.tearDownCluster();
            fixture = null;
        }
    }

    @Test
    public void getOakWiresAuditPipelineOn() throws Exception {
        fixture = OakFixture.getMemoryNSWithAudit(0);
        Oak oak = fixture.getOak(0);
        assertNotNull(oak);

        assertTrue("FT_OAK-12331 toggle + a 'security'-domain listener must be live; "
                        + "AuditDispatch.isEnabled() must return true",
                AuditDispatch.isEnabled());
        assertTrue("Capture sites in UserManagerImpl gate on isEnabledFor('security'); "
                        + "must return true so audit-ON capture exercise the buffer path",
                AuditDispatch.isEnabledFor(SecurityAuditDomain.DOMAIN));
    }

    @Test
    public void setUpClusterAlsoWiresAuditPipeline() throws Exception {
        fixture = OakFixture.getMemoryNSWithAudit(0);
        Oak[] cluster = fixture.setUpCluster(2, StatisticsProvider.NOOP);
        assertNotNull(cluster);
        assertTrue("cluster must contain the requested number of Oak instances",
                cluster.length == 2);

        assertTrue(AuditDispatch.isEnabled());
        assertTrue(AuditDispatch.isEnabledFor(SecurityAuditDomain.DOMAIN));
    }

    @Test
    public void tearDownClusterDisposesAuditPipeline() throws Exception {
        fixture = OakFixture.getMemoryNSWithAudit(0);
        fixture.getOak(0);
        assertTrue(AuditDispatch.isEnabled());

        fixture.tearDownCluster();
        fixture = null;

        assertFalse("After tearDownCluster, AuditDispatch must route to NOOP",
                AuditDispatch.isEnabled());
        assertFalse("Domain-scoped probe must also revert to NOOP",
                AuditDispatch.isEnabledFor(SecurityAuditDomain.DOMAIN));
    }

    @Test
    public void fixtureNameIsStable() {
        fixture = OakFixture.getMemoryNSWithAudit(0);
        assertTrue("fixture name must match OAK_MEMORY_NS_AUDIT constant",
                OakFixture.OAK_MEMORY_NS_AUDIT.equals(fixture.toString()));
    }
}
