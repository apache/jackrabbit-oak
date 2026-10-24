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
import org.apache.jackrabbit.oak.spi.audit.AuditBufferLifecycle;
import org.apache.jackrabbit.oak.spi.audit.AuditDispatch;
import org.apache.jackrabbit.oak.spi.security.audit.SecurityAuditDomain;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Coverage for the {@code -Doak.audit.enabled=true} opt-in on
 * {@link OakFixture#getMemoryNS(long)} (and, transitively, the shared
 * {@code getMemory(name, cacheSize)} entry point). The dedicated
 * {@link OakFixture#getMemoryNSWithAudit(long)} entry point is covered by
 * {@link MemoryNSWithAuditFixtureTest}; this class pins the property-driven
 * branch that lets existing benchmarks opt into audit without switching
 * methods.
 * <p>
 * Both {@link AuditDispatch#install} and {@link AuditBufferLifecycle#install}
 * mutate JVM-static state, so the tests defensively reset both in
 * {@link #before()} / {@link #after()} on top of the per-test
 * {@code fixture.tearDownCluster()} call (which calls
 * {@code AuditPipeline.dispose()} — the production path that
 * NOOPs both façades). That keeps the OFF assertions honest even if a
 * prior test in the same JVM leaked state.
 */
public class OakFixturePropertyTest {

    private OakFixture fixture;
    private String originalProperty;

    @Before
    public void before() {
        originalProperty = System.getProperty(OakFixture.AUDIT_ENABLED_PROPERTY);
        System.clearProperty(OakFixture.AUDIT_ENABLED_PROPERTY);
        // Hermetic baseline: NOOP both global façades before each test,
        // regardless of any upstream test's cleanup quality.
        AuditDispatch.install(null);
        AuditBufferLifecycle.install(null);
    }

    @After
    public void after() {
        try {
            if (fixture != null) {
                fixture.tearDownCluster();
                fixture = null;
            }
        } finally {
            // Always restore the property to the JVM-startup value so a
            // run-with-property invocation doesn't bleed into other tests.
            if (originalProperty == null) {
                System.clearProperty(OakFixture.AUDIT_ENABLED_PROPERTY);
            } else {
                System.setProperty(OakFixture.AUDIT_ENABLED_PROPERTY, originalProperty);
            }
            // Belt-and-braces: even if tearDownCluster() somehow left a
            // façade installed, force both back to NOOP.
            AuditDispatch.install(null);
            AuditBufferLifecycle.install(null);
        }
    }

    /**
     * Default (no property set): {@code getMemoryNS} must keep its
     * historical audit-OFF shape. Existing consumers see no behavior
     * change unless they explicitly opt in.
     */
    @Test
    public void propertyAbsentLeavesGetMemoryNSAuditOff() throws Exception {
        // before() already cleared the property.
        fixture = OakFixture.getMemoryNS(0);
        Oak oak = fixture.getOak(0);
        assertNotNull(oak);

        assertFalse("default getMemoryNS(0) without the property must be audit-OFF",
                AuditDispatch.isEnabled());
        assertFalse("audit must remain OFF for every domain probe",
                AuditDispatch.isEnabledFor(SecurityAuditDomain.DOMAIN));
    }

    /**
     * With {@code -Doak.audit.enabled=true}, {@code getMemoryNS} must
     * wire the same audit pipeline that {@code getMemoryNSWithAudit}
     * does — FT_OAK-12331 toggle ON and a security-domain listener live.
     * If this assertion regresses, callers that pass
     * {@code -Doak.audit.enabled=true} from {@code mvn -D...} or
     * benchmark scripts will silently measure audit-OFF code paths.
     */
    @Test
    public void propertySetTrueEnablesAuditOnGetMemoryNS() throws Exception {
        System.setProperty(OakFixture.AUDIT_ENABLED_PROPERTY, "true");

        fixture = OakFixture.getMemoryNS(0);
        Oak oak = fixture.getOak(0);
        assertNotNull(oak);

        assertTrue("getMemoryNS(0) with -Doak.audit.enabled=true must wire the audit pipeline; "
                        + "AuditDispatch.isEnabled() must return true",
                AuditDispatch.isEnabled());
        assertTrue("the security-domain listener must be live so capture sites in "
                        + "UserManagerImpl actually allocate / buffer / dispatch events",
                AuditDispatch.isEnabledFor(SecurityAuditDomain.DOMAIN));
    }

    /**
     * Property explicitly set to {@code "false"} must behave identically
     * to no property set — audit OFF. Pins the {@code Boolean.getBoolean}
     * contract against a future regression that defaulted to true on any
     * property presence.
     */
    @Test
    public void propertySetFalseLeavesGetMemoryNSAuditOff() throws Exception {
        System.setProperty(OakFixture.AUDIT_ENABLED_PROPERTY, "false");

        fixture = OakFixture.getMemoryNS(0);
        Oak oak = fixture.getOak(0);
        assertNotNull(oak);

        assertFalse("getMemoryNS(0) with -Doak.audit.enabled=false must stay audit-OFF",
                AuditDispatch.isEnabled());
    }

    /**
     * The property is read at fixture-construction time, not at
     * {@code getOak} time. Mutating the property AFTER construction
     * must NOT flip the fixture's audit mode — otherwise a process that
     * toggles the property mid-run could end up with a fixture whose
     * teardown contract no longer matches its construction-time wiring.
     */
    @Test
    public void propertyIsReadAtConstructionNotAtGetOak() throws Exception {
        // Construct with the property OFF.
        System.clearProperty(OakFixture.AUDIT_ENABLED_PROPERTY);
        fixture = OakFixture.getMemoryNS(0);

        // Flip the property AFTER construction.
        System.setProperty(OakFixture.AUDIT_ENABLED_PROPERTY, "true");

        Oak oak = fixture.getOak(0);
        assertNotNull(oak);

        assertFalse("property mutation after construction must not retroactively "
                        + "enable audit on this fixture",
                AuditDispatch.isEnabled());
    }
}
