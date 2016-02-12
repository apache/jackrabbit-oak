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
package org.apache.jackrabbit.oak.spi.security.authorization.cug.impl;

import java.security.Principal;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.spi.security.authorization.cug.CugExclude;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CugExcludeImplTest extends CugExcludeDefaultTest {

    private String[] principalNames = new String[] {"a","b","c","test"};
    private Set<Principal> principals = ImmutableSet.<Principal>of(new PrincipalImpl("test"));

    @Override
    CugExclude createInstance() {
        return new CugExcludeImpl();
    }

    private void activate(@Nonnull Map<String, Object> map) {
        ((CugExcludeImpl) exclude).activate(map);
    }

    @Test
    public void testEmpty() {
        assertFalse(exclude.isExcluded(principals));
    }

    @Test
    public void testEmpty2() {
        activate(Collections.<String, Object>emptyMap());
        assertFalse(exclude.isExcluded(principals));
    }

    @Test
    public void testExcludeTest() {
        Map<String, Object> m = ImmutableMap.<String, Object>of("principalNames", principalNames);
        activate(m);

        Set<Principal> all = new HashSet<Principal>();
        for (String name : principalNames) {
            Principal p = new PrincipalImpl(name);
            assertTrue(exclude.isExcluded(ImmutableSet.of(p)));

            all.add(p);
            assertTrue(exclude.isExcluded(all));
        }
    }

    @Test
    public void testExcludeAnother() {
        Map<String, Object> m = ImmutableMap.<String, Object>of("principalNames", principalNames);
        activate(m);
        assertFalse(exclude.isExcluded(ImmutableSet.<Principal>of(new PrincipalImpl("another"))));
    }

    @Test
    public void testModifyExclude() {
        Map<String, Object> m = ImmutableMap.<String, Object>of("principalNames", principalNames);
        activate(m);
        ((CugExcludeImpl) exclude).modified(ImmutableMap.<String, Object>of("principalNames", new String[]{"other"}));

        for (String name : principalNames) {
            Principal p = new PrincipalImpl(name);
            assertFalse(exclude.isExcluded(ImmutableSet.of(p)));
        }
        assertTrue(exclude.isExcluded(ImmutableSet.<Principal>of(new PrincipalImpl("other"))));
    }
}