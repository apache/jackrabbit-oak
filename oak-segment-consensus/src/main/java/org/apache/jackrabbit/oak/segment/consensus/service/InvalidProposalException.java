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
package org.apache.jackrabbit.oak.segment.consensus.service;

/**
 * A replicated proposal that is invalid by its own content. Every member rejects it the same way, so the log
 * entry is skipped. Any other apply failure may be node-local and must stop the member instead.
 */
public class InvalidProposalException extends IllegalArgumentException {

    public InvalidProposalException(String message) {
        super(message);
    }

    /**
     * @return true when {@code error} or one of its causes is an {@link InvalidProposalException}
     */
    public static boolean isCauseOf(Throwable error) {
        for (Throwable t = error; t != null; t = t.getCause()) {
            if (t instanceof InvalidProposalException) {
                return true;
            }
        }
        return false;
    }
}
