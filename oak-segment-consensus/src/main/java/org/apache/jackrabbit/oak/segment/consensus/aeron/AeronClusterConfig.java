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

import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;

/**
 * OSGi configuration for Aeron cluster service.
 */
@ObjectClassDefinition(
    name = "Blockchain AEM - Aeron Cluster",
    description = "Configuration for Aeron cluster consensus service"
)
public @interface AeronClusterConfig {

    @AttributeDefinition(
        name = "Enabled",
        description = "Enable Aeron cluster service"
    )
    boolean enabled() default true;

    @AttributeDefinition(
        name = "Node ID",
        description = "Aeron cluster node ID"
    )
    int nodeId() default 0;

    @AttributeDefinition(
        name = "Base Port",
        description = "Aeron cluster base port. 0 = use built-in default."
    )
    int basePort() default 0;

    @AttributeDefinition(
        name = "Self URL",
        description = "Validator self URL (e.g., http://localhost:8090)"
    )
    String selfUrl() default "http://localhost:8090";

    @AttributeDefinition(
        name = "Peer URLs",
        description = "Peer validator URLs"
    )
    String[] peerUrls() default {};

    @AttributeDefinition(
        name = "Observe Elections",
        description = "Observe elections on startup before genesis writes"
    )
    boolean observeElections() default true;

    @AttributeDefinition(
        name = "Log Cluster State Details",
        description = "Enable verbose cluster state logging on startup"
    )
    boolean logClusterStateDetails() default false;

    @AttributeDefinition(
        name = "Beacon API URL",
        description = "Ethereum beacon API URL"
    )
    String beaconApiUrl() default "https://beaconcha.in/api";

    @AttributeDefinition(
        name = "Cluster Environment",
        description = "Environment label (dev/staging/prod), reported only. Optional."
    )
    String clusterEnvironment() default "";

    @AttributeDefinition(
        name = "Session Timeout Minutes",
        description = "Aeron cluster session timeout in minutes. 0 = 30 s default; the oak.cluster.session.timeout.seconds system property takes precedence."
    )
    int sessionTimeoutMinutes() default 0;

    @AttributeDefinition(
        name = "Media Driver Timeout (ms)",
        description = "MediaDriver driverTimeoutMs. 0 = use built-in default."
    )
    int mediaDriverTimeoutMs() default 0;

    @AttributeDefinition(
        name = "Socket Send Buffer (bytes)",
        description = "Aeron UDP socket send buffer length. 0 = use built-in default."
    )
    int socketSendBufferBytes() default 0;

    @AttributeDefinition(
        name = "Socket Receive Buffer (bytes)",
        description = "Aeron UDP socket receive buffer length. 0 = use built-in default."
    )
    int socketReceiveBufferBytes() default 0;

    @AttributeDefinition(
        name = "Publication Term Buffer Length (bytes)",
        description = "MediaDriver publicationTermBufferLength. 0 = use built-in default."
    )
    int publicationTermBufferLengthBytes() default 0;

    @AttributeDefinition(
        name = "Cluster Term Length (bytes)",
        description = "Aeron cluster channel term length used for log/ingress channels. 0 = use built-in default."
    )
    int clusterTermLengthBytes() default 0;

    @AttributeDefinition(
        name = "Heartbeat Max Age (ms)",
        description = "Maximum heartbeat age before stale health. 0 = use built-in default."
    )
    long heartbeatMaxAgeMs() default 0L;

    @AttributeDefinition(
        name = "Reachability Cache (ms)",
        description = "Peer reachability cache duration in ms. 0 = use built-in default."
    )
    long reachabilityCacheMs() default 0L;

    @AttributeDefinition(
        name = "Reachability Connect Timeout (ms)",
        description = "HTTP connect timeout for peer reachability checks. 0 = use built-in default."
    )
    int reachabilityConnectTimeoutMs() default 0;

    @AttributeDefinition(
        name = "Reachability Read Timeout (ms)",
        description = "HTTP read timeout for peer reachability checks. 0 = use built-in default."
    )
    int reachabilityReadTimeoutMs() default 0;

    @AttributeDefinition(
        name = "Reconnect Max Attempts",
        description = "Maximum reconnect attempts for internal Aeron client. 0 = use built-in default."
    )
    int reconnectMaxAttempts() default 0;

    @AttributeDefinition(
        name = "Max Concurrent Sessions",
        description = "ConsensusModule max concurrent sessions. 0 = use built-in default."
    )
    int maxConcurrentSessions() default 0;

    @AttributeDefinition(
        name = "Peer Probe Mode",
        description = "Peer probe mode for health checks: http or none. Defaults to http."
    )
    String peerProbeMode() default "http";

    @AttributeDefinition(
        name = "Delete Aeron Dirs On Startup",
        description = "If true, clean stale Aeron directories on startup (dev/test only)."
    )
    boolean deleteAeronDirsOnStartup() default false;
}
