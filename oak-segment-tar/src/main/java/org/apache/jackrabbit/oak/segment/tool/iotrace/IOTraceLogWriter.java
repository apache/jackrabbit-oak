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
 *
 */

package org.apache.jackrabbit.oak.segment.tool.iotrace;

import static com.google.common.base.Preconditions.checkNotNull;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;

/**
 * This {@link IOTraceWriter} implementation implements persistence
 * through a {@code Logger} instance.
 */
public class IOTraceLogWriter implements IOTraceWriter {

    @NotNull
    private final Logger log;

    /**
     * Create a new instance persisting to {@code log}.
     * @param log
     */
    public IOTraceLogWriter(@NotNull Logger log) {
        this.log = checkNotNull(log);
    }

    @Override
    public void writeHeader(@NotNull String header) {
        log.debug(header);
    }

    @Override
    public void writeEntry(@NotNull String entry) {
        log.debug(entry);
    }

    @Override
    public void flush() {}
}
