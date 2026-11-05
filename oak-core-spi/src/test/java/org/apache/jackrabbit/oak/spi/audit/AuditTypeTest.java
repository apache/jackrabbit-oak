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
package org.apache.jackrabbit.oak.spi.audit;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class AuditTypeTest {

    @Test
    public void acceptsDottedIdentifier() {
        assertEquals("membership.added", AuditType.of("membership.added").name());
    }

    @Test
    public void rejectsEmpty() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> AuditType.of(""));
        assertTrue(ex.getMessage().contains("type"));
        assertTrue(ex.getMessage().contains("blank"));
    }

    @Test
    public void rejectsWhitespaceOnly() {
        assertThrows(IllegalArgumentException.class, () -> AuditType.of("   "));
    }

    @Test
    public void rejectsEmbeddedWhitespace() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> AuditType.of("membership added"));
        assertTrue(ex.getMessage().contains("whitespace"));
    }

    @Test
    public void rejectsColon() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> AuditType.of("rep:added"));
        assertTrue(ex.getMessage().contains("':'"));
    }

    @Test
    public void rejectsCharactersIllegalInAJcrName() {
        for (String bad : new String[] {"a/b", "a[b", "a]b", "a|b", "a*b", ".", ".."}) {
            assertThrows("must reject " + bad, IllegalArgumentException.class,
                    () -> AuditType.of(bad));
        }
    }

    @Test
    public void equalsAndHashCodeAreValueBased() {
        AuditType a = AuditType.of("membership.added");
        AuditType b = AuditType.of("membership.added");
        AuditType other = AuditType.of("membership.removed");

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, other);
        assertEquals(a, a);
        assertNotEquals(a, null);
        assertNotEquals("membership.added", a);
    }

    @Test
    public void usableAsAMapKey() {
        Map<AuditType, String> map = new HashMap<>();
        map.put(AuditType.of("membership.added"), "v");
        assertEquals("v", map.get(AuditType.of("membership.added")));
    }

    @Test
    public void toStringReturnsName() {
        assertEquals("membership.added", AuditType.of("membership.added").toString());
    }
}
