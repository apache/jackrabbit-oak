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

package org.apache.jackrabbit.oak.plugins.document.spi;

import javax.annotation.Nullable;

public interface JournalPropertyBuilder<T extends JournalProperty> {
    /**
     * Adds the JournalProperty instance fetched from CommitInfo to this builder
     */
    void addProperty(@Nullable T journalProperty);

    /**
     * Returns a string representation state of the builder which
     * would be stored in JournalEntry
     */
    String buildAsString();

    /**
     * Adds the serialized form of journal property (as build from #buildAsString)
     * call
     */
    void addSerializedProperty(@Nullable String serializedProperty);

    /**
     * Constructs a JournalProperty instance based on current builder state
     */
    JournalProperty build();
}
