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
package org.apache.jackrabbit.oak.segment.consensus.aeron;

import io.aeron.ExclusivePublication;
import io.aeron.FragmentAssembler;
import io.aeron.Image;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.service.SnapshotTaker;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.apache.jackrabbit.oak.segment.consensus.service.AppliedLogPosition;
import org.apache.jackrabbit.oak.segment.http.server.util.JsonParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Writes and reads the clustered service's Aeron snapshot.
 *
 * <p>The snapshot holds the Oak applied-log watermark, the leadership term and the Ethereum epoch in force, the
 * Oak head record id for diagnostics, then one frame per proposal tracked by the durability tally and one per
 * replicated GC proposal. Snapshots without those frames restore them empty. Oak content is never streamed: the
 * store persists itself and records the watermark with every replicated merge, so on restart the store only has
 * to be at least as far as the snapshot. Each member snapshots its own store at the same log position.
 */
public class SnapshotService {

    private static final Logger log = LoggerFactory.getLogger(SnapshotService.class);
    static final String FORMAT = "oak-applied-watermark-v1";

    public static final class SnapshotState {
        public final AppliedLogPosition applied;
        /** Aeron leadership term in force at the snapshot position, or -1 if not yet known. */
        public final long leadershipTermId;
        public final int epoch;
        /** Oak head record id when the snapshot was taken; diagnostics only, never compared. */
        public final String head;
        /** Durability tally entries, as written by {@code DurabilityTally#snapshotEntries}. */
        public final List<Map<String, Object>> durability;
        /** Replicated GC proposals, as written by {@code GCProposalManager#snapshotProposals}. */
        public final List<Map<String, Object>> gcProposals;

        public SnapshotState(AppliedLogPosition applied, long leadershipTermId, int epoch, String head) {
            this(applied, leadershipTermId, epoch, head, Collections.emptyList(), Collections.emptyList());
        }

        public SnapshotState(AppliedLogPosition applied, long leadershipTermId, int epoch, String head,
                             List<Map<String, Object>> durability, List<Map<String, Object>> gcProposals) {
            this.applied = applied;
            this.leadershipTermId = leadershipTermId;
            this.epoch = epoch;
            this.head = head;
            this.durability = durability;
            this.gcProposals = gcProposals;
        }
    }

    /**
     * Offers the snapshot metadata, then the durability entries and GC proposals. Uses Aeron's own snapshot offer
     * loop: back pressure idles on the cluster idle strategy, a CLOSED, NOT_CONNECTED or MAX_POSITION_EXCEEDED
     * publication throws ClusterException and an interrupt throws AgentTerminationException, so a failed snapshot
     * is never acknowledged as taken.
     */
    public void createSnapshot(ExclusivePublication publication, IdleStrategy idleStrategy, SnapshotState state) {
        FrameWriter writer = new FrameWriter(publication, idleStrategy);
        writer.write(SimpleMessageHeader.TEMPLATE_ID_SNAPSHOT, encode(state));
        for (Map<String, Object> entry : state.durability) {
            writer.write(SimpleMessageHeader.TEMPLATE_ID_SNAPSHOT_DURABILITY, JsonParser.toJson(entry));
        }
        for (Map<String, Object> proposal : state.gcProposals) {
            writer.write(SimpleMessageHeader.TEMPLATE_ID_SNAPSHOT_GC_PROPOSAL, JsonParser.toJson(proposal));
        }
        log.info("📸 Snapshot written: applied {}, term {}, head {}, {} durability entries, {} GC proposals",
            state.applied, state.leadershipTermId, state.head, state.durability.size(), state.gcProposals.size());
    }

    /**
     * Reads the snapshot to the end of the snapshot image.
     *
     * @throws ClusterException if the image holds no metadata in this format
     */
    public SnapshotState restoreSnapshot(Image snapshotImage, IdleStrategy idleStrategy) {
        SnapshotState[] state = new SnapshotState[1];
        List<Map<String, Object>> durability = new ArrayList<>();
        List<Map<String, Object>> gcProposals = new ArrayList<>();
        FragmentAssembler assembler = new FragmentAssembler((buffer, offset, length, header) -> {
            if (length < SimpleMessageHeader.ENCODED_LENGTH) {
                return;
            }
            int templateId = SimpleMessageHeader.decode(buffer, offset).templateId;
            if (templateId == SimpleMessageHeader.TEMPLATE_ID_SNAPSHOT) {
                state[0] = decode(payload(buffer, offset, length));
            } else if (templateId == SimpleMessageHeader.TEMPLATE_ID_SNAPSHOT_DURABILITY) {
                durability.add(JsonParser.parseObject(payload(buffer, offset, length)));
            } else if (templateId == SimpleMessageHeader.TEMPLATE_ID_SNAPSHOT_GC_PROPOSAL) {
                gcProposals.add(JsonParser.parseObject(payload(buffer, offset, length)));
            }
        });
        idleStrategy.reset();
        while (!snapshotImage.isEndOfStream()) {
            idleStrategy.idle(snapshotImage.poll(assembler, 10));
        }
        if (state[0] == null) {
            throw new ClusterException("Aeron snapshot holds no " + FORMAT + " metadata");
        }
        log.info("📦 Snapshot read: applied {}, term {}, head {}, {} durability entries, {} GC proposals",
            state[0].applied, state[0].leadershipTermId, state[0].head, durability.size(), gcProposals.size());
        return new SnapshotState(state[0].applied, state[0].leadershipTermId, state[0].epoch, state[0].head,
            durability, gcProposals);
    }

    static String encode(SnapshotState state) {
        Map<String, Object> json = new LinkedHashMap<>();
        json.put("format", FORMAT);
        json.put("appliedLogPosition", state.applied.position());
        json.put("appliedLogItem", (long) state.applied.item());
        json.put("appliedTerm", state.applied.term());
        json.put("leadershipTermId", state.leadershipTermId);
        json.put("ethereumEpoch", (long) state.epoch);
        json.put("head", state.head);
        return JsonParser.toJson(json);
    }

    static SnapshotState decode(String payload) {
        Map<String, Object> json = JsonParser.parseObject(payload);
        if (!FORMAT.equals(json.get("format"))) {
            throw new ClusterException("unsupported Aeron snapshot format: " + json.get("format")
                + " (expected " + FORMAT + "; snapshots that streamed store files are no longer restored)");
        }
        AppliedLogPosition applied = new AppliedLogPosition(
            number(json, "appliedLogPosition"), (int) number(json, "appliedLogItem"), number(json, "appliedTerm"));
        Object head = json.get("head");
        return new SnapshotState(applied, number(json, "leadershipTermId"), (int) number(json, "ethereumEpoch"),
            head instanceof String ? (String) head : null);
    }

    private static long number(Map<String, Object> json, String field) {
        Object value = json.get(field);
        if (!(value instanceof Number)) {
            throw new ClusterException("Aeron snapshot metadata is missing " + field);
        }
        return ((Number) value).longValue();
    }

    private static String payload(DirectBuffer buffer, int offset, int length) {
        byte[] bytes = new byte[length - SimpleMessageHeader.ENCODED_LENGTH];
        buffer.getBytes(offset + SimpleMessageHeader.ENCODED_LENGTH, bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static final class FrameWriter extends SnapshotTaker {
        FrameWriter(ExclusivePublication publication, IdleStrategy idleStrategy) {
            super(publication, idleStrategy, null);
        }

        void write(int templateId, String json) {
            byte[] payload = json.getBytes(StandardCharsets.UTF_8);
            UnsafeBuffer buffer = new UnsafeBuffer(new byte[SimpleMessageHeader.ENCODED_LENGTH + payload.length]);
            SimpleMessageHeader.encode(buffer, 0, payload.length, templateId);
            buffer.putBytes(SimpleMessageHeader.ENCODED_LENGTH, payload);
            offer(buffer, 0, buffer.capacity());
        }
    }
}
