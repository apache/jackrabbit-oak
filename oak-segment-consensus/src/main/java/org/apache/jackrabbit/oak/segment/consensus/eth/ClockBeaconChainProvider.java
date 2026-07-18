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
package org.apache.jackrabbit.oak.segment.consensus.eth;

import java.util.function.LongSupplier;

/**
 * Local, deterministic-shape epoch source used by mock mode.
 *
 * <p>The value follows the Sepolia epoch clock so existing chain-context
 * reporting remains compatible, but no network request or blockchain
 * verification is performed.</p>
 */
final class ClockBeaconChainProvider implements BeaconChainProvider {

    static final long SEPOLIA_GENESIS_MS = 1_655_733_600_000L;
    static final long EPOCH_DURATION_MS = 384_000L;

    private final long genesisMs;
    private final long epochDurationMs;
    private final LongSupplier currentTimeMillis;

    ClockBeaconChainProvider() {
        this(SEPOLIA_GENESIS_MS, EPOCH_DURATION_MS, System::currentTimeMillis);
    }

    ClockBeaconChainProvider(long genesisMs, long epochDurationMs, LongSupplier currentTimeMillis) {
        this.genesisMs = genesisMs;
        this.epochDurationMs = epochDurationMs;
        this.currentTimeMillis = currentTimeMillis;
    }

    @Override
    public long fetchFinalizedEpoch() {
        long elapsedMs = Math.max(0L, currentTimeMillis.getAsLong() - genesisMs);
        long currentEpoch = elapsedMs / epochDurationMs;
        return Math.max(0L, currentEpoch - 2L);
    }

    @Override
    public String name() {
        return "local-clock(sepolia)";
    }
}
