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

import java.io.Flushable;

import org.jetbrains.annotations.NotNull;

/**
 * Instances of {@code IOTraceWriter} are responsible for persisting
 * io traces.
 */
public interface IOTraceWriter extends Flushable {

    /**
     * Persist a {@code header}
     * @param header
     */
    void writeHeader(@NotNull String header);

    /**
     * Persist a {@code entry}
     * @param entry
     */
    void writeEntry(@NotNull String entry);

    @Override
    void flush();
}
