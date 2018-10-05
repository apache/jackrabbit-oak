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

package org.apache.jackrabbit.oak.segment.file.tar.index;

import java.util.Set;
import java.util.UUID;

/**
 * An index for the entries in a TAR file.
 */
public interface Index {

    /**
     * Returns the identifiers of every entry in this index.
     *
     * @return A set of {@link UUID}.
     */
    Set<UUID> getUUIDs();

    /**
     * Find an entry by its identifier.
     *
     * @param msb The most significant bits of the identifier.
     * @param lsb The least significant bits of the identifier.
     * @return The index of the entry in this index, or {@code -1} if the entry
     * was not found.
     */
    int findEntry(long msb, long lsb);

    /**
     * Return the size of this index in bytes.
     *
     * @return The size of this index in bytes.
     */
    int size();

    /**
     * Return the number of entries in this index.
     *
     * @return The number of entries in this index.
     */
    int count();

    /**
     * Return the entry at a specified index.
     *
     * @param i The index of the entry.
     * @return An instance of {@link IndexEntry}.
     */
    IndexEntry entry(int i);

}
