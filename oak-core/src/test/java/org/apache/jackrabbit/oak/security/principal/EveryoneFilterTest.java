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
package org.apache.jackrabbit.oak.security.principal;

import org.apache.jackrabbit.guava.common.collect.Iterators;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.security.Principal;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class EveryoneFilterTest {

    @Parameterized.Parameters(name = "searchType={1}")
    public static Collection<Object[]> parameters() {
        return List.of(
                new Object[] { PrincipalManager.SEARCH_TYPE_GROUP , "Group"},
                new Object[] { PrincipalManager.SEARCH_TYPE_ALL , "All"},
                new Object[] { PrincipalManager.SEARCH_TYPE_NOT_GROUP , "Not_Group"}
                );
    }
    
    private final int searchType;
    private final Principal anotherPrincipal = new PrincipalImpl("another");
    
    public EveryoneFilterTest(int searchType, String name) {
        this.searchType = searchType;
    }
    
    private static int adjustExpectedSize (int searchType, int expectedSize) {
        // for search-type 'users only' everyone will not get injected if missing
        return (searchType != PrincipalManager.SEARCH_TYPE_NOT_GROUP) ? expectedSize+1 : expectedSize;
    }

    @Test
    public void testEveryoneAlreadyIncluded() {
        Iterator<Principal> principals = Iterators.forArray(EveryonePrincipal.getInstance(), anotherPrincipal);
        Iterator<Principal> result = EveryoneFilter.filter(principals, EveryonePrincipal.NAME, searchType, 0, Long.MAX_VALUE);
        
        assertEquals(2, Iterators.size(result));
    }

    @Test
    public void testMissingEveryoneNoRange() {
        Iterator<Principal> principals = Iterators.singletonIterator(anotherPrincipal);
        Iterator<Principal> result = EveryoneFilter.filter(principals, EveryonePrincipal.NAME, searchType, 0, Long.MAX_VALUE);

        int expectedSize = adjustExpectedSize(searchType, 1);
        assertEquals(expectedSize, Iterators.size(result));
    }

    @Test
    public void testMissingEveryoneNullHint() {
        Iterator<Principal> principals = Iterators.forArray(anotherPrincipal);
        Iterator<Principal> result = EveryoneFilter.filter(principals, null, searchType, 0, Long.MAX_VALUE);

        int expectedSize = adjustExpectedSize(searchType, 1);
        assertEquals(expectedSize, Iterators.size(result));
    }

    @Test
    public void testMissingEveryoneOffset() {
        Iterator<Principal> principals = Iterators.forArray(anotherPrincipal);
        Iterator<Principal> result = EveryoneFilter.filter(principals, EveryonePrincipal.NAME, searchType, 1, Long.MAX_VALUE);

        assertEquals(1, Iterators.size(result));
    }

    @Test
    public void testMissingEveryoneLimit() {
        Iterator<Principal> principals = Iterators.forArray();
        Iterator<Principal> result = EveryoneFilter.filter(principals, EveryonePrincipal.NAME, searchType, 0, 10);

        assertEquals(0, Iterators.size(result));
    }
    
    @Test
    public void testResultContainsNull() {
        Iterator<Principal> principals = Iterators.forArray(anotherPrincipal, null, EveryonePrincipal.getInstance());
        Iterator<Principal> result = EveryoneFilter.filter(principals, EveryonePrincipal.NAME, searchType, 0, Long.MAX_VALUE);

        assertEquals(3, Iterators.size(result));
    }
}