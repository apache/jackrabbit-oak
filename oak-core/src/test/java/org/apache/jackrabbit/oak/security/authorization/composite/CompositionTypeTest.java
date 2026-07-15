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
package org.apache.jackrabbit.oak.security.authorization.composite;

import org.junit.Test;

import static org.apache.jackrabbit.oak.security.authorization.composite.CompositeAuthorizationConfiguration.CompositionType.AND;
import static org.apache.jackrabbit.oak.security.authorization.composite.CompositeAuthorizationConfiguration.CompositionType.OR;
import static org.junit.Assert.assertSame;

public class CompositionTypeTest {

    @Test
    public void testOrFromString() {
        String[] orNames = new String[] {OR.name(), OR.toString(), OR.name().toLowerCase(), OR.name().toUpperCase()};
        for (String s : orNames) {
            assertSame(OR, CompositeAuthorizationConfiguration.CompositionType.fromString(s));
        }
    }

    @Test
    public void testAndFromString() {
        String[] andNames = new String[] {AND.name(), AND.toString(), AND.name().toLowerCase(), AND.name().toUpperCase(), "any", ""};
        for (String s : andNames) {
            assertSame(AND, CompositeAuthorizationConfiguration.CompositionType.fromString(s));
        }
    }
}