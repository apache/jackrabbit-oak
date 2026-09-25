/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.elastic.internal;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Holder for {@code oak-search-elastic} feature toggle flags. The {@code .internal} package is deliberately left
 * out of {@code Export-Package} (see oak-search-elastic/pom.xml), so the public fields here are bundle-internal
 * and can be removed once a toggle is retired without an OSGi baseline / API compatibility break -- unlike a
 * toggle declared directly on a class in an exported package (see {@code ElasticConnection.FT_OAK_12234}, which
 * predates this convention and already shipped in a release, so it can no longer be moved).
 */
public final class ElasticFeatureToggles {

    private ElasticFeatureToggles() {
    }

    /**
     * Feature toggle for OAK-12366: force HTTP/1.1 on the Elasticsearch REST client connection. As of the ES 9.x
     * client upgrade, the underlying transport negotiates HTTP/2 over TLS by default. Because HTTP/2 multiplexes
     * all requests over a single TCP connection, large bulk ingestion payloads (up to 8MB) have been observed to
     * trigger H2 stream resets ({@code RST_STREAM}) from the server or intermediary proxies, while small read
     * requests sharing the same connection succeed -- making the failures intermittent, hard to diagnose, and
     * unrecoverable within an indexing cycle. Enabled by default (bug fix). When the toggle is flipped the shared
     * {@link #FT_OAK_12366_DISABLE} flag is set to {@code true} and the client falls back to negotiating HTTP/2,
     * restoring the default library behaviour.
     */
    public static final String FT_OAK_12366 = "FT_OAK-12366";
    public static final AtomicBoolean FT_OAK_12366_DISABLE = new AtomicBoolean(false);

    /**
     * Feature toggle for OAK-12415: set {@code retry_on_conflict} on Elasticsearch bulk update operations so a
     * version conflict is re-applied server-side instead of dropped (which would leave the document stale).
     * Enabled by default (bug fix). Set to {@code false} to revert to the legacy behaviour (no retries).
     */
    public static final String FT_OAK_12415 = "FT_OAK-12415";
    public static final AtomicBoolean FT_OAK_12415_ENABLE = new AtomicBoolean(true);
}
