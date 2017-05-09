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

package org.apache.jackrabbit.oak.remote.content;

import org.apache.jackrabbit.oak.api.Result;
import org.junit.Test;

import static com.google.common.collect.Iterables.elementsEqual;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class ContentRemoteResultsTest {

    private ContentRemoteResults createContentRemoteResults(Result result) {
        return new ContentRemoteResults(createContentRemoteBinaries(), result);
    }

    private ContentRemoteBinaries createContentRemoteBinaries() {
        return mock(ContentRemoteBinaries.class);
    }

    @Test
    public void testGetTotal() {
        Result result = mock(Result.class);
        doReturn(42L).when(result).getSize();
        assertEquals(42L, createContentRemoteResults(result).getTotal());
    }

    @Test
    public void testGetColumns() {
        String[] columns = {"a", "b"};

        Result result = mock(Result.class);
        doReturn(columns).when(result).getColumnNames();

        assertTrue(elementsEqual(asList(columns), createContentRemoteResults(result).getColumns()));
    }

    @Test
    public void testGetSelectors() {
        String[] selectors = {"a", "b"};

        Result result = mock(Result.class);
        doReturn(selectors).when(result).getSelectorNames();

        assertTrue(elementsEqual(asList(selectors), createContentRemoteResults(result).getSelectors()));
    }

}
