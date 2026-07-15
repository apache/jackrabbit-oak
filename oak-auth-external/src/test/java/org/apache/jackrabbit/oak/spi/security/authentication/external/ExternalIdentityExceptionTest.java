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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public class ExternalIdentityExceptionTest {

    @Test
    public void testConstructWithMsg() {
        ExternalIdentityException ex = new ExternalIdentityException("msg");
        assertEquals("msg", ex.getMessage());
        assertNull(ex.getCause());
    }

    @Test
    public void testConstructWithMsgAndException() {
        Exception base = new IllegalArgumentException();
        ExternalIdentityException ex = new ExternalIdentityException("msg", base);
        assertEquals("msg", ex.getMessage());
        assertSame(base, ex.getCause());
    }

    @Test
    public void testConstructWithException() {
        Exception base = new IllegalArgumentException("msg");
        ExternalIdentityException ex = new ExternalIdentityException(base);
        assertEquals(base.toString(), ex.getMessage());
        assertSame(base, ex.getCause());
    }

}