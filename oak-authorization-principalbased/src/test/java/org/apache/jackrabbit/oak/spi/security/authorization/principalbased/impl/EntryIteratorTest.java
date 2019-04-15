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
package org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl;

import com.google.common.base.Predicates;
import com.google.common.collect.Iterators;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class EntryIteratorTest {

    private EntryCache cache = when(mock(EntryCache.class).getEntries(anyString())).thenReturn(Iterators.singletonIterator(mock(PermissionEntry.class))).getMock();

    @Test
    public void testIterationStopsAtRootPath() {
        EntryIterator it = new EntryIterator("/some/test/path", Predicates.alwaysTrue(), cache);
        while (it.hasNext()) {
            it.next();
        }
        verify(cache, times(4)).getEntries(anyString());
        for (String path : new String[] {"/some/test/path", "/some/test", "/some", "/"}) {
            verify(cache, times(1)).getEntries(path);
        }
    }

    @Test
    public void testRespectsPredicate() {
        EntryIterator it = new EntryIterator("/some/test/path", Predicates.alwaysFalse(), cache);
        assertFalse(it.hasNext());
    }
}