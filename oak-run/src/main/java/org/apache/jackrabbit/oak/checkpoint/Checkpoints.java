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

import java.util.List;
import java.util.Map;

import javax.annotation.CheckForNull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.document.CheckpointsHelper;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.collect.Lists;

import static org.apache.jackrabbit.oak.plugins.document.CheckpointsHelper.getCheckpoints;
import static org.apache.jackrabbit.oak.plugins.document.CheckpointsHelper.removeOlderThan;

/**
 * A helper class to manage checkpoints on TarMK and DocumentMK.
 */
public abstract class Checkpoints {

    public static Checkpoints onTarMK(FileStore store) {
        return new TarMKCheckpoints(store);
    }

    public static Checkpoints onDocumentMK(DocumentNodeStore store) {
        return new DocumentMKCheckpoints(store);
    }

    /**
     * @return a list of all checkpoints.
     */
    public abstract List<CP> list();

    /**
     * Remove all checkpoints.
     *
     * @return the number of removed checkpoints or {@code -1} if the operation
     *          did not succeed.
     */
    public abstract long removeAll();

    /**
     * Remove all unreferenced checkpoints.
     *
     * @return the number of removed checkpoints or {@code -1} if the operation
     *          did not succeed.
     */
    public abstract long removeUnreferenced();

    /**
     * Removes the given checkpoint.
     *
     * @param cp a checkpoint string.
     * @return {@code 1} if the checkpoint was successfully remove, {@code 0} if
     *          there is no such checkpoint or {@code -1} if the operation did
     *          not succeed.
     */
    public abstract int remove(String cp);

    private static final class TarMKCheckpoints extends Checkpoints {

        private final FileStore store;

        public TarMKCheckpoints(FileStore store) {
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

            String ref = getReferenceCheckpoint(head.getChildNode("root"));

            NodeBuilder builder = head.builder();
            NodeBuilder cps = builder.getChildNode("checkpoints");
            long cnt = 0;
            for (String c : cps.getChildNodeNames()) {
                if (c.equals(ref)) {
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

    private static final class DocumentMKCheckpoints extends Checkpoints {

        private final DocumentNodeStore store;

        private DocumentMKCheckpoints(DocumentNodeStore store) {
            this.store = store;
        }

        @Override
        public List<CP> list() {
            List<CP> list = Lists.newArrayList();
            for (Map.Entry<Revision, String> entry : getCheckpoints(store).entrySet()) {
                list.add(new CP(entry.getKey().toString(),
                        entry.getKey().getTimestamp(),
                        Long.parseLong(entry.getValue())));
            }
            return list;
        }

        @Override
        public long removeAll() {
            return CheckpointsHelper.removeAll(store);
        }

        @Override
        public long removeUnreferenced() {
            String ref = getReferenceCheckpoint(store.getRoot());
            if (ref == null) {
                return -1;
            }
            return removeOlderThan(store, Revision.fromString(ref));
        }

        @Override
        public int remove(String cp) {
            Revision r;
            try {
                r = Revision.fromString(cp);
            } catch (IllegalArgumentException e) {
                return 0;
            }
            return CheckpointsHelper.remove(store, r);
        }
    }

    @CheckForNull
    private static String getReferenceCheckpoint(NodeState root) {
        String ref = null;
        PropertyState refPS = root.getChildNode(":async").getProperty("async");
        if (refPS != null) {
            ref = refPS.getValue(Type.STRING);
        }
        if (ref != null) {
            System.out.println(
                    "Referenced checkpoint from /:async@async is " + ref);
        }
        return ref;
    }

    public static final class CP {

        public final String id;
        public final long created;
        public final long expires;

        private CP(String id, long created, long expires) {
            this.id = id;
            this.created = created;
            this.expires = expires;
        }

    }
}
