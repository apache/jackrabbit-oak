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

public class AuditDomainTest {

    @Test
    public void acceptsDottedIdentifier() {
        assertEquals("oak.security", AuditDomain.of("oak.security").name());
    }

    @Test
    public void acceptsHyphenAndUnderscore() {
        assertEquals("oak-security", AuditDomain.of("oak-security").name());
        assertEquals("oak_security", AuditDomain.of("oak_security").name());
    }

    @Test
    public void rejectsEmpty() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> AuditDomain.of(""));
        assertTrue(ex.getMessage().contains("domain"));
        assertTrue(ex.getMessage().contains("blank"));
    }

    @Test
    public void rejectsWhitespaceOnly() {
        // JcrNameParser accepts " ", so the blank check is what rejects it.
        assertThrows(IllegalArgumentException.class, () -> AuditDomain.of("   "));
        assertThrows(IllegalArgumentException.class, () -> AuditDomain.of(" \t "));
    }

    @Test
    public void rejectsEmbeddedWhitespace() {
        // Also accepted by JcrNameParser, hence the explicit check.
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> AuditDomain.of("oak security"));
        assertTrue(ex.getMessage().contains("whitespace"));
    }

    @Test
    public void rejectsColon() {
        // A colon is a JCR namespace prefix; meaningless for a flat
        // identifier, and JcrNameParser would happily accept it.
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> AuditDomain.of("oak:security"));
        assertTrue(ex.getMessage().contains("':'"));
    }

    @Test
    public void rejectsCharactersIllegalInAJcrName() {
        // The reason the type exists: a domain must be usable as a node name.
        for (String bad : new String[] {"a/b", "a[b", "a]b", "a|b", "a*b", ".", ".."}) {
            assertThrows("must reject " + bad, IllegalArgumentException.class,
                    () -> AuditDomain.of(bad));
        }
    }

    @Test
    public void equalsAndHashCodeAreValueBased() {
        AuditDomain a = AuditDomain.of("oak.security");
        AuditDomain b = AuditDomain.of("oak.security");
        AuditDomain other = AuditDomain.of("oak.query");

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, other);
        assertEquals(a, a);
        assertNotEquals(a, null);
        assertNotEquals("oak.security", a);
    }

    @Test
    public void usableAsAMapKey() {
        // The pipeline routes by domain, so map behaviour is load-bearing.
        Map<AuditDomain, String> map = new HashMap<>();
        map.put(AuditDomain.of("oak.security"), "v");
        assertEquals("v", map.get(AuditDomain.of("oak.security")));
    }

    @Test
    public void notEqualToASameNamedType() {
        // Distinct types on purpose: a domain is not a type.
        assertNotEquals(AuditDomain.of("x.y"), AuditType.of("x.y"));
    }

    @Test
    public void toStringReturnsName() {
        assertEquals("oak.security", AuditDomain.of("oak.security").toString());
    }
}
