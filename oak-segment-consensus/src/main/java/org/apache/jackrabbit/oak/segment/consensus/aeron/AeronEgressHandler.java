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

import io.aeron.Publication;
import io.aeron.cluster.client.AeronCluster;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.osgi.service.component.annotations.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles Aeron egress (outbound) message delivery with back-pressure retry logic.
 */
@Component(service = AeronEgressHandler.class)
public class AeronEgressHandler {

    private static final Logger log = LoggerFactory.getLogger(AeronEgressHandler.class);

    enum OfferResult {
        SENT,
        BACK_PRESSURED,
        NOT_CONNECTED,
        CLOSED,
        FAILED
    }

    public OfferResult offerWithRetryResult(AeronCluster client,
                                            IdleStrategy idleStrategy,
                                            DirectBuffer messageBuffer,
                                            int totalLength,
                                            String label,
                                            int maxRetries,
                                            Runnable onSuccess,
                                            boolean logSuccess) {
        if (client == null) {
            log.error("❌ AeronCluster client not available - cannot send {}", label);
            return OfferResult.FAILED;
        }

        idleStrategy.reset();
        long result;
        int retries = 0;
        while ((result = client.offer(messageBuffer, 0, totalLength)) < 0) {
            // Retried as in Aeron's Cluster.offer example (Cluster.java:291-300 in 1.53.3).
            if (result == Publication.BACK_PRESSURED || result == Publication.ADMIN_ACTION) {
                idleStrategy.idle();
                retries++;
                if (retries > maxRetries) {
                    log.warn("⚠️  {} back-pressured after {} retries", label, retries);
                    return OfferResult.BACK_PRESSURED;
                }
            } else if (result == Publication.NOT_CONNECTED) {
                log.warn("⚠️  {} not connected - waiting...", label);
                idleStrategy.idle();
                retries++;
                if (retries > maxRetries) {
                    log.error("❌ {} not connected after {} retries", label, retries);
                    return OfferResult.NOT_CONNECTED;
                }
            } else if (result == Publication.CLOSED || result == Publication.MAX_POSITION_EXCEEDED) {
                log.error("❌ Failed to send {} through ingress: {}", label, result);
                return OfferResult.CLOSED;
            } else {
                log.error("❌ Failed to send {} through ingress: {}", label, result);
                return OfferResult.FAILED;
            }
        }

        if (onSuccess != null) {
            onSuccess.run();
        }

        if (logSuccess) {
            log.info("✅ {} sent through AeronCluster.offer() - will replicate to all nodes via Raft", label);
        }

        return OfferResult.SENT;
    }

    public boolean offerWithRetry(AeronCluster client,
                                  IdleStrategy idleStrategy,
                                  DirectBuffer messageBuffer,
                                  int totalLength,
                                  String label,
                                  int maxRetries,
                                  Runnable onSuccess,
                                  boolean logSuccess) {
        return offerWithRetryResult(
            client,
            idleStrategy,
            messageBuffer,
            totalLength,
            label,
            maxRetries,
            onSuccess,
            logSuccess
        ) == OfferResult.SENT;
    }
}
