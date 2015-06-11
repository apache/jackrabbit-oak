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

package org.apache.jackrabbit.oak.remote.content;

import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.remote.RemoteResult;
import org.apache.jackrabbit.oak.remote.RemoteResults;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;

class ContentRemoteResults implements RemoteResults {

    private final ContentRemoteBinaries binaries;

    private final Result results;

    public ContentRemoteResults(ContentRemoteBinaries binaries, Result results) {
        this.binaries = binaries;
        this.results = results;
    }

    @Override
    public long getTotal() {
        return results.getSize();
    }

    @Override
    public Iterable<String> getColumns() {
        return Arrays.asList(results.getColumnNames());
    }

    @Override
    public Iterable<String> getSelectors() {
        return Arrays.asList(results.getSelectorNames());
    }

    @Override
    public Iterator<RemoteResult> iterator() {
        return getResults().iterator();
    }

    private Iterable<RemoteResult> getResults() {
        List<RemoteResult> results = newArrayList();

        for (ResultRow row : this.results.getRows()) {
            results.add(new ContentRemoteResult(binaries, row));
        }

        return results;
    }

}
