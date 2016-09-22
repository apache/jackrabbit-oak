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

package org.apache.jackrabbit.oak.checkpoint;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.io.Closer;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.plugins.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

final class SegmentCheckpoints extends Checkpoints {

    static Checkpoints create(File path, Closer closer) throws IOException, InvalidFileStoreVersionException {
        return new SegmentCheckpoints(closer.register(FileStore.builder(path).build()));
    }

    private final FileStore store;

    private SegmentCheckpoints(FileStore store) {
        this.store = store;
    }

    @Override
    public List<CP> list() {
        List<CP> list = Lists.newArrayList();
        NodeState ns = store.getHead().getChildNode("checkpoints");
        for (ChildNodeEntry cne : ns.getChildNodeEntries()) {
            NodeState cneNs = cne.getNodeState();
            list.add(new CP(cne.getName(),
                    cneNs.getLong("created"), cneNs.getLong("timestamp")));
        }
        return list;
    }

    @Override
    public long removeAll() {
        SegmentNodeState head = store.getHead();
        NodeBuilder builder = head.builder();

        NodeBuilder cps = builder.getChildNode("checkpoints");
        long cnt = cps.getChildNodeCount(Integer.MAX_VALUE);
        builder.setChildNode("checkpoints");
        if (store.setHead(head, asSegmentNodeState(builder))) {
            return cnt;
        } else {
            return -1;
        }
    }

    @Override
    public long removeUnreferenced() {
        SegmentNodeState head = store.getHead();

        Set<String> refs = getReferencedCheckpoints(head.getChildNode("root"));

        NodeBuilder builder = head.builder();
        NodeBuilder cps = builder.getChildNode("checkpoints");
        long cnt = 0;
        for (String c : cps.getChildNodeNames()) {
            if (refs.contains(c)) {
                continue;
            }
            cps.getChildNode(c).remove();
            cnt++;
        }

        if (store.setHead(head, asSegmentNodeState(builder))) {
            return cnt;
        } else {
            return -1;
        }
    }

    @Override
    public int remove(String cp) {
        SegmentNodeState head = store.getHead();
        NodeBuilder builder = head.builder();

        NodeBuilder cpn = builder.getChildNode("checkpoints")
                .getChildNode(cp);
        if (cpn.exists()) {
            cpn.remove();
            if (store.setHead(head, asSegmentNodeState(builder))) {
                return 1;
            } else {
                return -1;
            }
        } else {
            return 0;
        }
    }

    private static SegmentNodeState asSegmentNodeState(NodeBuilder builder) {
        return (SegmentNodeState) builder.getNodeState();
    }
}
