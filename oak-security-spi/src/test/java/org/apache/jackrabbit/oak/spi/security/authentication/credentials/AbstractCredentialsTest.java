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
package org.apache.jackrabbit.oak.spi.security.authentication.credentials;

import java.util.Date;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class AbstractCredentialsTest {

    private static final String USER_ID = "userId";

    private AbstractCredentials credentials = new AbstractCredentials(USER_ID) {
    };

    @Test
    public void testGetUserId() {
        assertEquals(USER_ID, credentials.getUserId());
    }

    @Test
    public void testAttributesAreEmpty() {
        assertTrue(credentials.getAttributes().isEmpty());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAttributesAreImmutable() {
        credentials.getAttributes().put("attr", "value");
    }

    @Test
    public void testSetAttribute() {
        Object value = new Date();
        credentials.setAttribute("attr", value);

        assertEquals(value, credentials.getAttribute("attr"));

        Map<String,Object> attributes = credentials.getAttributes();
        assertTrue(attributes.containsKey("attr"));
        assertEquals(1, attributes.size());
        assertEquals(value, attributes.get("attr"));
    }

    @Test
    public void testSetNullAttributeValue() {
        credentials.setAttribute("attr", null);
        assertTrue(credentials.getAttributes().isEmpty());


        credentials.setAttribute("attr", 25);
        credentials.setAttribute("attr", null);

        assertNull(credentials.getAttribute("attr"));
        assertTrue(credentials.getAttributes().isEmpty());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetNullAttributeName() {
        credentials.setAttribute(null, "value");
    }

    @Test
    public void testSetAttributes() {
        Map<String,Object> attributes = ImmutableMap.of("attr", true);
        credentials.setAttributes(attributes);

        Map<String,Object> attr = credentials.getAttributes();
        assertFalse(attr.isEmpty());
        assertEquals(attributes, attr);
        assertNotSame(attributes, attr);
    }
}