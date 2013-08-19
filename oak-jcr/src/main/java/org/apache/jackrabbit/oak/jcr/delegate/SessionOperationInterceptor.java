/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.jcr.delegate;

import org.apache.jackrabbit.oak.jcr.operation.SessionOperation;

public interface SessionOperationInterceptor {
    SessionOperationInterceptor NOOP = new SessionOperationInterceptor() {
        @Override
        public void before(SessionDelegate delegate, SessionOperation operation) {
        }

        @Override
        public void after(SessionDelegate delegate, SessionOperation operation) {
        }
    };

    /**
     * Invoked before the sessionOperation is performed. SessionOperation MUST only be
     * used for reading purpose and implementation must not invoke the {@link SessionOperation#perform}
     *
     * @param delegate sessionDelegate performing the operation
     * @param operation operation to perform
     */
    void before(SessionDelegate delegate, SessionOperation operation);

    /**
     * Invoked after the sessionOperation is performed. SessionOperation MUST only be
     * used for reading purpose and implementation must not invoke the {@link SessionOperation#perform}
     *
     * @param delegate sessionDelegate performing the operation
     * @param operation operation to perform
     */
    void after(SessionDelegate delegate, SessionOperation operation);
}
