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

package org.apache.jackrabbit.oak.segment.file;

import javax.annotation.Nonnull;

/**
 * A value class representing an entry in the revisions journal.
 */
public class JournalEntry {
    @Nonnull
    private final String revision;
    
    private final long timestamp;

    JournalEntry(@Nonnull String revision, long timestamp) {
        this.revision = revision;
        this.timestamp = timestamp;
    }

    @Nonnull
    public String getRevision() {
        return revision;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + revision.hashCode();
        result = prime * result + (int) (timestamp ^ (timestamp >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        } else if (object instanceof JournalEntry) {
            JournalEntry that = (JournalEntry) object;
            return revision.equals(that.revision) && timestamp == that.timestamp;
        } else {
            return false;
        }
    }
}
