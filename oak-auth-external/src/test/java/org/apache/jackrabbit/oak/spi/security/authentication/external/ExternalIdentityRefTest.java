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

import java.util.Map;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ExternalIdentityRefTest {

    private static final String USERID = "user%id";
    private static final String PROVIDER_NAME = "provider;Name";

    private ExternalIdentityRef refNullProvider = new ExternalIdentityRef(USERID, null);
    private ExternalIdentityRef refEmptyProvider = new ExternalIdentityRef(USERID, "");
    private ExternalIdentityRef ref = new ExternalIdentityRef(USERID, PROVIDER_NAME);

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
        assertFalse(USERID.equals(s));
        assertEquals("user%25id", s);

        s = refEmptyProvider.getString();
        assertNotNull(s);
        assertFalse(USERID.equals(s));
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
        assertTrue(refNullProvider.equals(refNullProvider));
        assertTrue(refNullProvider.equals(new ExternalIdentityRef(USERID, refNullProvider.getProviderName())));
        assertTrue(refNullProvider.equals(new ExternalIdentityRef(USERID, refEmptyProvider.getProviderName())));

        assertTrue(refNullProvider.equals(refEmptyProvider));
        assertTrue(refEmptyProvider.equals(refNullProvider));

        assertFalse(refNullProvider.equals(ref));
        assertFalse(ref.equals(refNullProvider));

        assertFalse(refNullProvider.equals(null));
        assertFalse(refNullProvider.equals(new ExternalIdentityRef("anotherId", null)));

        assertTrue(ref.equals(ref));
        assertTrue(ref.equals(new ExternalIdentityRef(ref.getId(), ref.getProviderName())));
        assertTrue(ref.equals(new ExternalIdentityRef(USERID, PROVIDER_NAME)));
        assertFalse(ref.equals(new ExternalIdentity() {
            @Nonnull
            @Override
            public ExternalIdentityRef getExternalId() {
                return ref;
            }

            @Nonnull
            @Override
            public String getId() {
                return ref.getId();
            }

            @Nonnull
            @Override
            public String getPrincipalName() {
                return ref.getId();
            }

            @CheckForNull
            @Override
            public String getIntermediatePath() {
                return null;
            }

            @Nonnull
            @Override
            public Iterable<ExternalIdentityRef> getDeclaredGroups() throws ExternalIdentityException {
                return ImmutableSet.of();
            }

            @Nonnull
            @Override
            public Map<String, ?> getProperties() {
                return ImmutableMap.of();
            }
        }));
    }
}
