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

import org.agrona.ErrorHandler;
import org.slf4j.Logger;

final class AeronClusterErrorPolicy {

    enum Decision {
        SUPPRESS_DNS_PENDING,
        SUPPRESS_CLUSTER_WARNING,
        SUPPRESS_HEARTBEAT_TIMEOUT,
        LOG_ERROR
    }

    Decision classify(Throwable throwable) {
        String message = throwable.getMessage();
        if (message != null && message.contains("UnknownHostException") && message.contains("unresolved")) {
            return Decision.SUPPRESS_DNS_PENDING;
        }
        if (message != null && message.contains("ClusterEvent") && message.contains("WARN")) {
            return Decision.SUPPRESS_CLUSTER_WARNING;
        }
        if (message != null && message.contains("leader heartbeat timeout")) {
            return Decision.SUPPRESS_HEARTBEAT_TIMEOUT;
        }
        return Decision.LOG_ERROR;
    }

    ErrorHandler createHandler(String context, Logger log) {
        return throwable -> handle(context, throwable, log);
    }

    void handle(String context, Throwable throwable, Logger log) {
        Decision decision = classify(throwable);
        switch (decision) {
            case SUPPRESS_DNS_PENDING:
                log.debug("🌐 P2P: DNS resolution pending for peer (will retry): {}", throwable.getClass().getSimpleName());
                break;
            case SUPPRESS_CLUSTER_WARNING:
                log.info("✈️  Aeron Cluster warning (informational): {}", throwable.getMessage());
                break;
            case SUPPRESS_HEARTBEAT_TIMEOUT:
                log.info("✈️  Leader heartbeat timeout (normal during election): {}", throwable.getMessage());
                break;
            case LOG_ERROR:
            default:
                log.error("{} error", context, throwable);
                break;
        }
    }
}
