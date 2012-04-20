/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.jackrabbit.oak.namepath;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class NameSpaceRegistryTest {

    @Test
    public void nameSpaceRegistry() {
        NamespaceRegistry r = new NamespaceRegistry();

        r.registerNamespace("p", "n");
        assertEquals(r.getNamespace("p"), "n");
        assertEquals(r.getJcrPrefix("n"), "p");
        String mk = r.getMkPrefixFromJcr("p");
        assertNotNull(mk);
        assertEquals("p", r.getJcrPrefixFromMk(mk));

        r.registerNamespace("p", "n2");
        assertEquals(r.getNamespace("p"), "n2");
        assertEquals(r.getJcrPrefix("n2"), "p");
        assertNull(r.getJcrPrefix("n"));
        mk = r.getMkPrefixFromJcr("p");
        assertNotNull(mk);
        assertEquals("p", r.getJcrPrefixFromMk(mk));

        r.registerNamespace("p2", "n");
        assertEquals(r.getNamespace("p2"), "n");
        assertEquals(r.getJcrPrefix("n"), "p2");
        assertEquals(r.getNamespace("p"), "n2");
        assertEquals(r.getJcrPrefix("n2"), "p");
        mk = r.getMkPrefixFromJcr("p");
        assertNotNull(mk);
        assertEquals("p", r.getJcrPrefixFromMk(mk));
        mk = r.getMkPrefixFromJcr("p2");
        assertNotNull(mk);
        assertEquals("p2", r.getJcrPrefixFromMk(mk));

        r.registerNamespace("p2", "n2");
        assertEquals(r.getNamespace("p2"), "n2");
        assertEquals(r.getJcrPrefix("n2"), "p2");
        assertNull(r.getJcrPrefix("n"));
        assertNull(r.getNamespace("p"));
        mk = r.getMkPrefixFromJcr("p");
        assertNull(mk);
        mk = r.getMkPrefixFromJcr("p2");
        assertNotNull(mk);
        assertEquals("p2", r.getJcrPrefixFromMk(mk));

        r.registerNamespace("p", "n");
        assertEquals(r.getNamespace("p2"), "n2");
        assertEquals(r.getJcrPrefix("n2"), "p2");
        assertEquals(r.getNamespace("p"), "n");
        assertEquals(r.getJcrPrefix("n"), "p");
        mk = r.getMkPrefixFromJcr("p");
        assertNotNull(mk);
        assertEquals("p", r.getJcrPrefixFromMk(mk));
        mk = r.getMkPrefixFromJcr("p2");
        assertNotNull(mk);
        assertEquals("p2", r.getJcrPrefixFromMk(mk));
    }
}
