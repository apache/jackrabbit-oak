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
package org.apache.jackrabbit.oak.spi.security.user;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Set;

import org.apache.jackrabbit.oak.spi.audit.AuditType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

public class UserAuditTypesTest {

    @Test
    public void typeConstantsAreStable() {
        // Pins the wire values: listener bundles match on them.
        assertEquals("membership.added", UserAuditTypes.MEMBER_ADDED.name());
        assertEquals("membership.removed", UserAuditTypes.MEMBER_REMOVED.name());
    }

    @Test
    public void allPayloadKeysAreNonBlank() {
        for (String value : payloadKeys()) {
            assertFalse("payload key must not be blank: " + value, value.isBlank());
        }
    }

    @Test
    public void typesAreUnique() {
        List<AuditType> values = types();
        assertEquals("types must be unique",
                values.size(), Set.copyOf(values).size());
    }

    @Test
    public void payloadKeysAreUnique() {
        List<String> values = payloadKeys();
        assertEquals("payload keys must be unique",
                values.size(), Set.copyOf(values).size());
    }

    @Test
    public void privateConstructorIsReachableForCoverage() throws Exception {
        // Constants-only class: private constructor guards against
        // accidental instantiation; reflection-invoked for line coverage.
        Constructor<UserAuditTypes> ctor = UserAuditTypes.class.getDeclaredConstructor();
        ctor.setAccessible(true);
        assertNotNull(ctor.newInstance());
    }

    private static List<AuditType> types() {
        return List.of(
                UserAuditTypes.MEMBER_ADDED,
                UserAuditTypes.MEMBER_REMOVED);
    }

    private static List<String> payloadKeys() {
        return List.of(
                UserAuditTypes.PAYLOAD_GROUP_PATH,
                UserAuditTypes.PAYLOAD_MEMBER_IDS,
                UserAuditTypes.PAYLOAD_MEMBER_PATHS,
                UserAuditTypes.PAYLOAD_MEMBERSHIP_SOURCE,
                UserAuditTypes.PAYLOAD_IS_CONTENT_ID,
                UserAuditTypes.PAYLOAD_FAILED_IDS);
    }
}
