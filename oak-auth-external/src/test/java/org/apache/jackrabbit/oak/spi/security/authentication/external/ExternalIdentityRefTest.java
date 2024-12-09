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
package org.apache.jackrabbit.oak.spi.security.authentication.external;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.jackrabbit.guava.common.collect.ImmutableList;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class ExternalIdentityRefTest {

    private static final String USERID = "user%id";
    private static final String PROVIDER_NAME = "provider;Name";

    private final ExternalIdentityRef refNullProvider = new ExternalIdentityRef(USERID, null);
    private final ExternalIdentityRef refEmptyProvider = new ExternalIdentityRef(USERID, "");
    private final ExternalIdentityRef ref = new ExternalIdentityRef(USERID, PROVIDER_NAME);

    @Test
    public void testGetId() {
        assertEquals(USERID, refNullProvider.getId());
        assertEquals(USERID, refEmptyProvider.getId());
        assertEquals(USERID, ref.getId());
    }

    @Test
    public void testGetProviderName() {
        assertNull(refNullProvider.getProviderName());
        assertNull(refEmptyProvider.getProviderName());
        assertEquals(PROVIDER_NAME, ref.getProviderName());
    }

    @Test
    public void testGetString() {
        String s = refNullProvider.getString();
        assertNotNull(s);
        assertNotEquals(USERID, s);
        assertEquals("user%25id", s);

        s = refEmptyProvider.getString();
        assertNotNull(s);
        assertNotEquals(USERID, s);
        assertEquals("user%25id", s);

        s = ref.getString();
        assertNotNull(s);
        assertEquals("user%25id;provider%3bName", s);
    }

    @Test
    public void testFromString() {
        ExternalIdentityRef r = ExternalIdentityRef.fromString(refNullProvider.getString());
        assertEquals(refNullProvider, r);
        assertEquals(USERID, r.getId());
        assertEquals(refNullProvider.getString(), r.getString());
        assertNull(r.getProviderName());

        r = ExternalIdentityRef.fromString(refEmptyProvider.getString());
        assertEquals(refEmptyProvider, r);
        assertEquals(USERID, r.getId());
        assertEquals(refEmptyProvider.getString(), r.getString());
        assertNull(r.getProviderName()); // empty provider string is converted to null

        r = ExternalIdentityRef.fromString(ref.getString());
        assertEquals(ref, r);
        assertEquals(USERID, r.getId());
        assertEquals(PROVIDER_NAME, r.getProviderName());
        assertEquals(ref.getString(), r.getString());
    }

    @Test
    public void testEquals() {
        assertEquals(refNullProvider, refNullProvider);
        assertEquals(refNullProvider, new ExternalIdentityRef(USERID, refNullProvider.getProviderName()));
        assertEquals(refNullProvider, new ExternalIdentityRef(USERID, refEmptyProvider.getProviderName()));

        assertEquals(refNullProvider, refEmptyProvider);
        assertEquals(refEmptyProvider, refNullProvider);

        assertEquals(ref, ref);
        assertEquals(ref, new ExternalIdentityRef(ref.getId(), ref.getProviderName()));
        assertEquals(ref, new ExternalIdentityRef(USERID, PROVIDER_NAME));
    }

    @Test
    public void testNotEquals() {
        Map<ExternalIdentityRef, ExternalIdentityRef> notEqual = new HashMap<>();
        notEqual.put(refNullProvider, ref);
        notEqual.put(refEmptyProvider, ref);
        notEqual.put(refNullProvider, null);
        notEqual.put(refNullProvider, new ExternalIdentityRef("anotherId", null));
        notEqual.put(ref, new ExternalIdentityRef("anotherId", PROVIDER_NAME));
        notEqual.put(ref, new ExternalIdentityRef(USERID, "anotherProvider"));

        for (Map.Entry<ExternalIdentityRef, ExternalIdentityRef> entry : notEqual.entrySet()) {
            ExternalIdentityRef r1 = entry.getKey();
            ExternalIdentityRef r2 = entry.getValue();

            assertNotEquals(r1, r2);
            assertNotEquals(r2, r1);
        }
    }

    @Test
    public void testNotEqualsExternalIdentity() {
        assertFalse(ref.equals(new ExternalIdentity() {
            @NotNull
            @Override
            public ExternalIdentityRef getExternalId() {
                return ref;
            }

            @NotNull
            @Override
            public String getId() {
                return ref.getId();
            }

            @NotNull
            @Override
            public String getPrincipalName() {
                return ref.getId();
            }

            @Nullable
            @Override
            public String getIntermediatePath() {
                return null;
            }

            @NotNull
            @Override
            public Iterable<ExternalIdentityRef> getDeclaredGroups() {
                return Set.of();
            }

            @NotNull
            @Override
            public Map<String, ?> getProperties() {
                return Map.of();
            }
        }));
    }

    @Test
    public void testToString() {
        for (ExternalIdentityRef r : ImmutableList.of(ref, refEmptyProvider, refEmptyProvider)) {
            assertEquals("ExternalIdentityRef{" + "id='" + r.getId() + '\'' + ", providerName='" + r.getProviderName() + '\'' + '}', r.toString());
        }
    }
    
    @Test
    public void testDeprecatedGroupRef() {
        ExternalGroupRef ref = new ExternalGroupRef(USERID, PROVIDER_NAME);
        assertEquals(USERID, ref.getId());
        assertEquals(PROVIDER_NAME, ref.getProviderName());
    }
}
