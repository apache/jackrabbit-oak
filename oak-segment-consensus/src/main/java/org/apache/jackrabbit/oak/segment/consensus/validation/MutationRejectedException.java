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
package org.apache.jackrabbit.oak.segment.consensus.validation;

/** Deterministic rejection before mutation, distinct from failure to apply valid committed state. */
public final class MutationRejectedException extends IllegalArgumentException {
    private static final long serialVersionUID = 1L;

    public MutationRejectedException(String message) {
        super(message);
    }

    public MutationRejectedException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * A replicated entry rejected for its own content is rejected the same way on every member, so the log entry is
     * skipped. Any other apply failure may be node-local and must stop the member instead.
     *
     * @return true when {@code error} or one of its causes is a {@link MutationRejectedException}
     */
    public static boolean isCauseOf(Throwable error) {
        for (Throwable t = error; t != null; t = t.getCause()) {
            if (t instanceof MutationRejectedException) {
                return true;
            }
        }
        return false;
    }
}
