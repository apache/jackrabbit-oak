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
package org.apache.jackrabbit.oak.plugins.document;

/**
 * This handler gets called back when recovery is needed for a clusterId. An
 * implementation then tries to perform the recovery and returns whether the
 * recovery was successful. Upon successful recovery, the clusterId will have
 * transitioned to the inactive state.
 */
interface RecoveryHandler {

    /**
     * A no-op recovery handler, always returning false.
     */
    RecoveryHandler NOOP = clusterId -> false;

    /**
     * Perform recovery for the given clusterId and return whether the recovery
     * was successful.
     *
     * @param clusterId perform recovery for this clusterId.
     * @return {@code true} if recovery was successful, {@code false} otherwise.
     */
    boolean recover(int clusterId);
}
