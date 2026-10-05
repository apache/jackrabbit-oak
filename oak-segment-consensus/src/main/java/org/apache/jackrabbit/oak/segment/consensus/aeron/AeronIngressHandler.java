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

import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.apache.jackrabbit.oak.segment.consensus.validation.MutationRejectedException;
import org.apache.jackrabbit.oak.segment.consensus.service.AppliedLogPosition;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Handles Aeron ingress messages and delegates to the MessageDispatcher.
 */
@Component(service = AeronIngressHandler.class)
public class AeronIngressHandler {

    private static final Logger log = LoggerFactory.getLogger(AeronIngressHandler.class);
    private static final long DISPATCH_FAIL_LOG_INTERVAL_MS = 5000;
    private final AtomicLong lastDispatchFailLogMs = new AtomicLong(0);
    private final AtomicInteger dispatchFailSuppressed = new AtomicInteger(0);

    private final AeronMessageCodec codec;
    private final MessageDispatcher dispatcher;
    private Runnable heartbeatCallback;
    private GenesisCallback genesisCallback;

    /** Applies a committed genesis trigger with Aeron's command timestamp and the watermark for its Oak merge. */
    @FunctionalInterface
    public interface GenesisCallback {
        void apply(long timestamp, String triggerJson, AppliedLogPosition logPosition);
    }

    @Activate
    public AeronIngressHandler(@Reference AeronMessageCodec codec,
                               @Reference MessageDispatcher dispatcher) {
        this.codec = codec;
        this.dispatcher = dispatcher;
    }

    public void setHeartbeatCallback(Runnable heartbeatCallback) {
        this.heartbeatCallback = heartbeatCallback;
    }

    public void setGenesisCallback(GenesisCallback genesisCallback) {
        this.genesisCallback = genesisCallback;
    }

    public boolean handleMessage(ClientSession session,
                                 long timestamp,
                                 DirectBuffer buffer,
                                 int offset,
                                 int length,
                                 Header header,
                                 Cluster cluster) {
        if (heartbeatCallback != null) {
            heartbeatCallback.run();
        }

        log.debug("📨 onSessionMessage() called - session: {}, length: {}, role: {}, timestamp: {}",
            session.id(), length, cluster != null ? cluster.role() : "UNKNOWN", timestamp);

        if (length < codec.headerLength()) {
            log.warn("⚠️  Message too short: {} (minimum {} bytes for SBE header)",
                length, codec.headerLength());
            return false;
        }

        // End of this entry in the cluster log; identical on every member (BoundedLogAdapter passes header.position()).
        // Apply failures propagate: the engine stops this member rather than skip the entry.
        long logPosition = header != null ? header.position() : -1L;
        SimpleMessageHeader.HeaderInfo headerInfo = codec.decodeHeader(buffer, offset);

        if (headerInfo.templateId == SimpleMessageHeader.TEMPLATE_ID_GENESIS_PROPOSAL) {
            AppliedLogPosition entry = dispatcher.entryPosition(logPosition, 0);
            if (dispatcher.isAlreadyApplied(entry)) {
                return true;
            }
            log.info("🎬 GENESIS proposal received via Aeron - creating genesis on this node");
            if (genesisCallback == null) {
                throw new MessageDispatcher.ReplicatedApplyException("Genesis callback unavailable", null);
            }
            try {
                genesisCallback.apply(timestamp, readGenesisProposal(buffer, offset, length, headerInfo.blockLength),
                    entry);
            } catch (MutationRejectedException e) {
                // Malformed trigger: a pure function of the replicated bytes, rejected identically on every member
                log.warn("Rejected malformed genesis trigger: {}", e.getMessage());
                return false;
            }
            log.info("✅ Genesis creation complete on this node");
            return true;
        }

        if (headerInfo.templateId == SimpleMessageHeader.TEMPLATE_ID_SNAPSHOT) {
            log.debug("📸 Snapshot message received in onSessionMessage (handled separately)");
            return true;
        }

        boolean success = dispatcher.dispatch(timestamp, logPosition, buffer, offset, length);
        if (!success) {
            logDispatchFailure(headerInfo.templateId);
        }
        return success;
    }

    private void logDispatchFailure(int templateId) {
        long now = System.currentTimeMillis();
        long last = lastDispatchFailLogMs.get();
        if ((now - last) >= DISPATCH_FAIL_LOG_INTERVAL_MS && lastDispatchFailLogMs.compareAndSet(last, now)) {
            int suppressed = dispatchFailSuppressed.getAndSet(0);
            if (suppressed > 0) {
                log.warn("⚠️  MessageDispatcher failed to process message (templateId: {}) (RATE LIMITED - suppressed {} in last {}ms)",
                    templateId, suppressed, DISPATCH_FAIL_LOG_INTERVAL_MS);
            } else {
                log.warn("⚠️  MessageDispatcher failed to process message (templateId: {}) (RATE LIMITED)", templateId);
            }
        } else {
            dispatchFailSuppressed.incrementAndGet();
        }
    }

    private String readGenesisProposal(DirectBuffer buffer, int offset, int length, int blockLength) {
        int payloadOffset = offset + codec.headerLength();
        int payloadLength = Math.max(0, Math.min(blockLength, length - codec.headerLength()));
        if (payloadLength == 0) {
            return "{}";
        }
        byte[] payload = new byte[payloadLength];
        buffer.getBytes(payloadOffset, payload);
        return new String(payload, StandardCharsets.UTF_8).trim();
    }
}
