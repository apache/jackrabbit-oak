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
package org.apache.jackrabbit.oak.spi.security.authentication;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.spi.security.principal.SystemPrincipal;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SystemSubjectTest {

    @Test
    public void testPrincipals() {
        assertEquals(ImmutableSet.of(SystemPrincipal.INSTANCE), SystemSubject.INSTANCE.getPrincipals());
    }

    @Test
    public void testReadOnly() {
        assertTrue(SystemSubject.INSTANCE.isReadOnly());
    }

    @Test
    public void testPublicCredentials() {
        assertTrue(SystemSubject.INSTANCE.getPublicCredentials().isEmpty());
    }

    @Test
    public void testPrivateCredentials() {
        assertTrue(SystemSubject.INSTANCE.getPrivateCredentials().isEmpty());
    }
}