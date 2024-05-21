/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * COPIED FROM APACHE LUCENE 4.7.2
 *
 * Git URL: git@github.com:apache/lucene.git, tag: releases/lucene-solr/4.7.2, path: lucene/core/src/java
 *
 * (see https://issues.apache.org/jira/browse/OAK-10786 for details)
 */

package org.apache.lucene.index;

import org.apache.lucene.store.DataInput;

/**
 * This exception is thrown when Lucene detects an index that is newer than this Lucene version.
 */
public class IndexFormatTooNewException extends CorruptIndexException {

    /**
     * Creates an {@code IndexFormatTooNewException}
     *
     * @param resourceDesc describes the file that was too old
     * @param version      the version of the file that was too old
     * @param minVersion   the minimum version accepted
     * @param maxVersion   the maxium version accepted
     * @lucene.internal
     */
    public IndexFormatTooNewException(String resourceDesc, int version, int minVersion,
        int maxVersion) {
        super("Format version is not supported (resource: " + resourceDesc + "): "
            + version + " (needs to be between " + minVersion + " and " + maxVersion + ")");
        assert resourceDesc != null;
    }

    /**
     * Creates an {@code IndexFormatTooNewException}
     *
     * @param in         the open file that's too old
     * @param version    the version of the file that was too old
     * @param minVersion the minimum version accepted
     * @param maxVersion the maxium version accepted
     * @lucene.internal
     */
    public IndexFormatTooNewException(DataInput in, int version, int minVersion, int maxVersion) {
        this(in.toString(), version, minVersion, maxVersion);
    }

}
