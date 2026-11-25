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
package org.apache.jackrabbit.oak.segment.spi;

/**
 * Indicates that a segment read was cancelled because the reading thread was interrupted.
 *
 * <p>Propagate this exception until the enclosing operation can handle cancellation.
 * Do not treat it as a cache miss, fall back to another segment source, or report
 * the segment as missing or corrupt. For example, a consistency check should abort,
 * and a standby reader should decline the interrupted request.
 *
 * <p>The persistent disk cache clears the thread's interrupt flag before throwing
 * this exception so that a later read on the same thread can proceed. Callers must
 * therefore use this exception, not the thread's interrupt status, to detect that
 * the read was cancelled.
 */
public class SegmentReadInterruptedException extends RuntimeException {

    public SegmentReadInterruptedException(String message, Throwable cause) {
        super(message, cause);
    }
}
