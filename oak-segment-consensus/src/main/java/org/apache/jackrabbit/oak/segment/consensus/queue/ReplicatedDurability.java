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
package org.apache.jackrabbit.oak.segment.consensus.queue;

import org.jetbrains.annotations.Nullable;

/**
 * Durability decisions the replicated log has already made, so a proposer that missed one (for example while it
 * was restarting) takes it instead of re-sending a proposal the log already holds.
 */
@FunctionalInterface
public interface ReplicatedDurability {

    ReplicatedDurability NONE = proposalId -> null;

    /**
     * @return the decision, or {@code null} if the log has not decided the proposal or no longer tracks it
     */
    @Nullable
    Decision find(String proposalId);

    final class Decision {
        public final boolean durable;
        public final String durableHead;
        public final String error;

        public Decision(boolean durable, String durableHead, String error) {
            this.durable = durable;
            this.durableHead = durableHead;
            this.error = error;
        }
    }
}
