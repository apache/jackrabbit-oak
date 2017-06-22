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
package org.apache.jackrabbit.oak.segment;

import static com.google.common.collect.Sets.newHashSet;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.easymock.EasyMock.createControl;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

import java.io.IOException;
import java.util.Set;
import java.util.regex.Pattern;

import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.segment.memory.MemoryStore;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.junit.Test;

/**
 * Test case for segment node state comparisons.
 */
public class MapRecordTest {

    private final NodeStateDiff diff =
            createControl().createMock("diff", NodeStateDiff.class);

    private NodeBuilder builder;

    // TODO frm replace this with JUnit test initialization
    public MapRecordTest() throws IOException {
        MemoryStore store = new MemoryStore();
        RecordId id = store.getWriter().writeNode(EMPTY_NODE);
        SegmentNodeState node = new SegmentNodeState(store.getReader(), store.getWriter(), store.getBlobStore(), id);
        builder = node.builder();
    }

    @Test
    public void testOak1104() {
        Pattern pattern = Pattern.compile(", ");
        Set<String> beforeNames = newHashSet(pattern.split(
                "_b_Lucene41_0.doc, _b.fdx, _b.fdt, segments_34, _b_4.del,"
                + " _b_Lucene41_0.pos, _b.nvm, _b.nvd, _b.fnm, _3n.si,"
                + " _b_Lucene41_0.tip, _b_Lucene41_0.tim, _3n.cfe,"
                + " segments.gen, _3n.cfs, _b.si"));
        Set<String> afterNames = newHashSet(pattern.split(
                "_b_Lucene41_0.pos, _3k.cfs, _3j_1.del, _b.nvm, _b.nvd,"
                + " _3d.cfe, _3d.cfs, _b.fnm, _3j.si, _3h.si, _3i.cfe,"
                + " _3i.cfs, _3e_2.del, _3f.si, _b_Lucene41_0.tip,"
                + " _b_Lucene41_0.tim, segments.gen, _3e.cfe, _3e.cfs,"
                + " _b.si, _3g.si, _3l.si, _3i_1.del, _3d_3.del, _3e.si,"
                + " _3d.si, _b_Lucene41_0.doc, _3h_2.del, _3i.si, _3k_1.del,"
                + " _3j.cfe, _3j.cfs, _b.fdx, _b.fdt, _3g_1.del, _3k.si,"
                + " _3l.cfe, _3l.cfs, segments_33, _3f_1.del, _3h.cfe,"
                + " _3h.cfs, _b_4.del, _3f.cfe, _3f.cfs, _3g.cfe, _3g.cfs"));

        for (String name : beforeNames) {
            builder.setChildNode(name);
        }
        NodeState before = builder.getNodeState();

        for (String name : Sets.difference(beforeNames, afterNames)) {
            builder.getChildNode(name).remove();
        }
        for (String name : Sets.difference(afterNames, beforeNames)) {
            builder.setChildNode(name);
        }
        NodeState after = builder.getNodeState();

        for (String name : Sets.difference(beforeNames, afterNames)) {
            expect(diff.childNodeDeleted(name, before.getChildNode(name))).andReturn(true);
        }
        for (String name : Sets.difference(afterNames, beforeNames)) {
            expect(diff.childNodeAdded(name, after.getChildNode(name))).andReturn(true);
        }
        replay(diff);

        after.compareAgainstBaseState(before, diff);
        verify(diff);
    }

}
