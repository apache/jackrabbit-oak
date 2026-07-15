/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.search;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link CompositePropertyUpdateCallback}
 */
public class CompositePropertyUpdateCallbackTest {

    @Test
    public void testNullCollectionNotAllowed() {
        try {
            new CompositePropertyUpdateCallback(null);
            fail("creating a CompositePropertyUpdateCallback with null delegates should not be allowed");
        } catch (Throwable t) {
            // ok
        }
    }

    @Test
    public void testEmptyCollection() throws CommitFailedException {
        CompositePropertyUpdateCallback compositePropertyUpdateCallback = new CompositePropertyUpdateCallback(Collections.emptyList());
        String path = "/foo";
        String relativePath = "bar";
        PropertyDefinition pd = mock(PropertyDefinition.class);
        PropertyState before = mock(PropertyState.class);
        PropertyState after = mock(PropertyState.class);
        compositePropertyUpdateCallback.propertyUpdated(path, relativePath, pd, before, after);
        compositePropertyUpdateCallback.done();
    }

    @Test
    public void propertyUpdated() {
        List<PropertyUpdateCallback> delegates = new LinkedList<>();
        delegates.add(mock(PropertyUpdateCallback.class));
        delegates.add(mock(PropertyUpdateCallback.class));
        CompositePropertyUpdateCallback compositePropertyUpdateCallback = new CompositePropertyUpdateCallback(delegates);
        String path = "/foo";
        String relativePath = "bar";
        PropertyDefinition pd = mock(PropertyDefinition.class);
        PropertyState before = mock(PropertyState.class);
        PropertyState after = mock(PropertyState.class);
        compositePropertyUpdateCallback.propertyUpdated(path, relativePath, pd, before, after);
    }

    @Test
    public void done() throws CommitFailedException {
        List<PropertyUpdateCallback> delegates = new LinkedList<>();
        delegates.add(mock(PropertyUpdateCallback.class));
        delegates.add(mock(PropertyUpdateCallback.class));
        CompositePropertyUpdateCallback compositePropertyUpdateCallback = new CompositePropertyUpdateCallback(delegates);
        compositePropertyUpdateCallback.done();
    }
}