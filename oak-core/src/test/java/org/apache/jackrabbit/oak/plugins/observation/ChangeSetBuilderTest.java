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

package org.apache.jackrabbit.oak.plugins.observation;

import org.junit.Test;

import static com.google.common.collect.ImmutableSet.of;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

public class ChangeSetBuilderTest {

    @Test
    public void basicMerge() throws Exception{
        ChangeSetBuilder cb1 = new ChangeSetBuilder(5, 2);
        add(cb1, "1");

        ChangeSetBuilder cb2 = new ChangeSetBuilder(5, 2);
        add(cb2, "2");

        ChangeSet cs = cb1.add(cb2.build()).build();
        assertThat(cs.getAllNodeTypes(), containsInAnyOrder("nt-1", "nt-2"));
        assertThat(cs.getParentPaths(), containsInAnyOrder("p-1", "p-2"));
        assertThat(cs.getParentNodeNames(), containsInAnyOrder("nn-1", "nn-2"));
        assertThat(cs.getParentNodeTypes(), containsInAnyOrder("pnt-1", "pnt-2"));
        assertThat(cs.getPropertyNames(), containsInAnyOrder("pn-1", "pn-2"));
    }

    @Test
    public void addedChangeSetAlreadyOverflown() throws Exception{
        ChangeSetBuilder cb1 = new ChangeSetBuilder(5, 2);
        add(cb1, "1");

        ChangeSet csAll = new ChangeSet(2, of("p-2"), of("nn-2"), of("pnt-2"), of("pn-2"), of("nt-2"));
        ChangeSet cs1 = new ChangeSet(2, null, of("nn-2"), of("nt-2"), of("pn-2"), of("nt-2"));
        ChangeSet mcs1 = cb1.add(cs1).build();
        assertNull(mcs1.getParentPaths());
        assertThat(mcs1.getAllNodeTypes(), containsInAnyOrder("nt-1", "nt-2"));
    }

    @Test
    public void overflowPath() throws Exception{
        ChangeSetBuilder cb1 = new ChangeSetBuilder(2, 2);
        add(cb1, "1");

        ChangeSet cs1 = new ChangeSet(2, null, of("nn-2"), of("pnt-2"), of("pn-2"), of("nt-2"));
        ChangeSet cs = cb1.add(cs1).build();
        assertNull(cs.getParentPaths());
        assertThat(cs.getAllNodeTypes(), containsInAnyOrder("nt-1", "nt-2"));
        assertThat(cs.getParentNodeNames(), containsInAnyOrder("nn-1", "nn-2"));
        assertThat(cs.getParentNodeTypes(), containsInAnyOrder("pnt-1", "pnt-2"));
        assertThat(cs.getPropertyNames(), containsInAnyOrder("pn-1", "pn-2"));

        ChangeSet cs2 = new ChangeSet(2, of("p-2", "p-3"), of("nn-2"), of("pnt-2"), of("pn-2"), of("nt-2"));
        cs = cb1.add(cs2).build();
        assertNull(cs.getParentPaths());
    }

    @Test
    public void overflowParentNodeName() throws Exception{
        ChangeSetBuilder cb1 = new ChangeSetBuilder(2, 2);
        add(cb1, "1");

        ChangeSet cs1 = new ChangeSet(2, of("p-2"), null, of("pnt-2"), of("pn-2"), of("nt-2"));
        ChangeSet cs = cb1.add(cs1).build();
        assertNull(cs.getParentNodeNames());
        assertThat(cs.getAllNodeTypes(), containsInAnyOrder("nt-1", "nt-2"));
        assertThat(cs.getParentNodeTypes(), containsInAnyOrder("pnt-1", "pnt-2"));
        assertThat(cs.getPropertyNames(), containsInAnyOrder("pn-1", "pn-2"));

        ChangeSet cs2 = new ChangeSet(2, of("p-2"), of("nn-2", "nn-3"), of("pnt-2"), of("pn-2"), of("nt-2"));
        cs = cb1.add(cs2).build();
        assertNull(cs.getParentNodeNames());
    }

    @Test
    public void overflowParentNodeTypes() throws Exception{
        ChangeSetBuilder cb1 = new ChangeSetBuilder(2, 2);
        add(cb1, "1");

        ChangeSet cs1 = new ChangeSet(2, of("p-2"), of("nn-2"), null, of("pn-2"), of("nt-2"));
        ChangeSet cs = cb1.add(cs1).build();
        assertNull(cs.getParentNodeTypes());
        assertThat(cs.getParentPaths(), containsInAnyOrder("p-1", "p-2"));
        assertThat(cs.getAllNodeTypes(), containsInAnyOrder("nt-1", "nt-2"));
        assertThat(cs.getParentNodeNames(), containsInAnyOrder("nn-1", "nn-2"));
        assertThat(cs.getPropertyNames(), containsInAnyOrder("pn-1", "pn-2"));
    }

    @Test
    public void overflowPropertyNames() throws Exception{
        ChangeSetBuilder cb1 = new ChangeSetBuilder(2, 2);
        add(cb1, "1");

        ChangeSet cs1 = new ChangeSet(2, of("p-2"), of("nn-2"), of("pnt-2"), null, of("nt-2"));
        ChangeSet cs = cb1.add(cs1).build();
        assertNull(cs.getPropertyNames());
        assertThat(cs.getParentPaths(), containsInAnyOrder("p-1", "p-2"));
        assertThat(cs.getAllNodeTypes(), containsInAnyOrder("nt-1", "nt-2"));
        assertThat(cs.getParentNodeNames(), containsInAnyOrder("nn-1", "nn-2"));
        assertThat(cs.getParentNodeTypes(), containsInAnyOrder("pnt-1", "pnt-2"));
    }

    @Test
    public void overflowAllNodeTypes() throws Exception{
        ChangeSetBuilder cb1 = new ChangeSetBuilder(2, 2);
        add(cb1, "1");

        ChangeSet cs1 = new ChangeSet(2, of("p-2"), of("nn-2"), of("pnt-2"), of("pn-2"), null);
        ChangeSet cs = cb1.add(cs1).build();
        assertNull(cs.getAllNodeTypes());
        assertThat(cs.getParentPaths(), containsInAnyOrder("p-1", "p-2"));
        assertThat(cs.getParentNodeNames(), containsInAnyOrder("nn-1", "nn-2"));
        assertThat(cs.getParentNodeTypes(), containsInAnyOrder("pnt-1", "pnt-2"));
        assertThat(cs.getPropertyNames(), containsInAnyOrder("pn-1", "pn-2"));
    }

    @Test
    public void pathDepth() throws Exception{
        ChangeSetBuilder cb = new ChangeSetBuilder(2, 2);
        cb.addParentPath("/a/b");
        cb.addParentPath("/x");
        cb.addParentPath("/p/q/r");

        ChangeSet cs = cb.build();
        assertThat(cs.getParentPaths(), containsInAnyOrder("/a/b", "/x"));
    }

    private static void add(ChangeSetBuilder cb, String suffix){
        cb.addNodeType("nt-"+suffix)
                .addParentPath("p-"+suffix)
                .addParentNodeName("nn-"+suffix)
                .addParentNodeType("pnt-"+suffix)
                .addPropertyName("pn-"+suffix);
    }

}