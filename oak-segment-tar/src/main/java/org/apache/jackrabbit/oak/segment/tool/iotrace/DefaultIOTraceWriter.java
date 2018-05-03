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

import java.io.BufferedWriter;
import java.io.PrintWriter;
import java.io.Writer;

import javax.annotation.Nonnull;

/**
 * This {@link IOTraceWriter} implementation implements persistence
 * through a {@code Writer} instance.
 */
public class DefaultIOTraceWriter implements IOTraceWriter {
    @Nonnull
    private final PrintWriter out;

    /**
     * Create a new instance persisting to {@code writer}.
     * It is the callers responsibility to close the {@code writer} when
     * not needed anymore.
     * @param writer
     */
    public DefaultIOTraceWriter(@Nonnull Writer writer) {
        out = new PrintWriter(new BufferedWriter(checkNotNull(writer)));
    }

    @Override
    public void writeHeader(@Nonnull String header) {
        out.println(header);
    }

    @Override
    public void writeEntry(@Nonnull String entry) {
        out.println(entry);
    }

    @Override
    public void flush() {
        out.flush();
    }
}
