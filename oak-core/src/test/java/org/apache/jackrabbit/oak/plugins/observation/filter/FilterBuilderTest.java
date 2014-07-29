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

package org.apache.jackrabbit.oak.plugins.observation.filter;

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.assertEquals;

import org.apache.jackrabbit.oak.plugins.observation.filter.FilterBuilder.Condition;
import org.junit.Test;

public class FilterBuilderTest {

    @Test
    public void allShortcutsExcludeAll() {
        FilterBuilder builder = new FilterBuilder();
        Condition condition = builder.all(
                builder.path("path"),
                builder.excludeAll(),
                builder.path("path"));
        assertEquals(ConstantFilter.EXCLUDE_ALL, condition.createFilter(EMPTY_NODE, EMPTY_NODE));
    }

    @Test
    public void emptyAllShortcuts() {
        FilterBuilder builder = new FilterBuilder();
        Condition condition = builder.all();
        assertEquals(ConstantFilter.INCLUDE_ALL, condition.createFilter(EMPTY_NODE, EMPTY_NODE));
    }

    @Test
    public void anyShortcutsIncludeAll() {
        FilterBuilder builder = new FilterBuilder();
        Condition condition = builder.any(
                builder.path("path"),
                builder.includeAll(),
                builder.path("path"));
        assertEquals(ConstantFilter.INCLUDE_ALL, condition.createFilter(EMPTY_NODE, EMPTY_NODE));
    }

    @Test
    public void emptyAnyShortcuts() {
        FilterBuilder builder = new FilterBuilder();
        Condition condition = builder.any();
        assertEquals(ConstantFilter.EXCLUDE_ALL, condition.createFilter(EMPTY_NODE, EMPTY_NODE));
    }

}
