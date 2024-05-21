/*
 * COPIED FROM APACHE LUCENE 4.7.2
 *
 * Git URL: git@github.com:apache/lucene.git, tag: releases/lucene-solr/4.7.2, path: lucene/core/src/java
 *
 * (see https://issues.apache.org/jira/browse/OAK-10786 for details)
 */

package org.apache.lucene.store;

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

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * A delegating Directory that records which files were written to and deleted.
 */
public final class TrackingDirectoryWrapper extends FilterDirectory {

    private final Set<String> createdFileNames = Collections.synchronizedSet(new HashSet<String>());

    public TrackingDirectoryWrapper(Directory in) {
        super(in);
    }

    @Override
    public void deleteFile(String name) throws IOException {
        createdFileNames.remove(name);
        in.deleteFile(name);
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        createdFileNames.add(name);
        return in.createOutput(name, context);
    }

    @Override
    public void copy(Directory to, String src, String dest, IOContext context) throws IOException {
        createdFileNames.add(dest);
        in.copy(to, src, dest, context);
    }

    @Override
    public Directory.IndexInputSlicer createSlicer(final String name, final IOContext context)
        throws IOException {
        return in.createSlicer(name, context);
    }

    // maybe clone before returning.... all callers are
    // cloning anyway....
    public Set<String> getCreatedFiles() {
        return createdFileNames;
    }

}
