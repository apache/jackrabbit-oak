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

package org.apache.jackrabbit.oak.plugins.index.lucene.property;


import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Test;

import static com.google.common.collect.ImmutableList.copyOf;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.jackrabbit.oak.plugins.index.lucene.property.HybridPropertyIndexUtil.PROP_CREATED;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.*;

public class UniqueIndexCleanerTest {
    private NodeBuilder builder = EMPTY_NODE.builder();

    @Test
    public void nothingCleaned() throws Exception{
        builder.child("a").setProperty(PROP_CREATED, 100);
        builder.child("b").setProperty(PROP_CREATED, 100);

        refresh();

        UniqueIndexCleaner cleaner = new UniqueIndexCleaner(MILLISECONDS, 1);
        cleaner.clean(builder, 10);
        assertThat(copyOf(builder.getChildNodeNames()), containsInAnyOrder("a", "b"));
    }

    @Test
    public void cleanWithMargin() throws Exception{
        builder.child("a").setProperty(PROP_CREATED, 100);
        builder.child("b").setProperty(PROP_CREATED, 200);

        refresh();
        UniqueIndexCleaner cleaner = new UniqueIndexCleaner(MILLISECONDS, 100);
        cleaner.clean(builder, 200);
        assertThat(copyOf(builder.getChildNodeNames()), containsInAnyOrder("b"));
    }

    private void refresh(){
        builder = builder.getNodeState().builder();
    }
}