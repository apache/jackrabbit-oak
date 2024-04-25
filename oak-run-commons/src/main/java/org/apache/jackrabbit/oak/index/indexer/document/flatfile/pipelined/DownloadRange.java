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
package org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined;

import com.mongodb.client.model.Filters;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.bson.conversions.Bson;

import java.util.ArrayList;

public final class DownloadRange {
    private final long lastModifiedFrom;
    private final long lastModifiedToInclusive;
    private final String startAfterDocumentID;
    private final boolean traversingInAscendingOrder;

    public DownloadRange(long lastModifiedFrom, long lastModifiedToInclusive, String startAfterDocumentID, boolean traversingInAscendingOrder) {
        this.traversingInAscendingOrder = traversingInAscendingOrder;
        if (!(lastModifiedFrom <= lastModifiedToInclusive)) {
            throw new IllegalArgumentException("Invalid range (" + lastModifiedFrom + ", " + lastModifiedToInclusive + ")");
        }
        this.lastModifiedFrom = lastModifiedFrom;
        this.lastModifiedToInclusive = lastModifiedToInclusive;
        this.startAfterDocumentID = startAfterDocumentID;
    }

    public String getStartAfterDocumentID() {
        return startAfterDocumentID;
    }

    public long getLastModifiedFrom() {
        return lastModifiedFrom;
    }

    public long getLastModifiedToInclusive() {
        return lastModifiedToInclusive;
    }

    public Bson getFindQuery() {
        ArrayList<Bson> filters = new ArrayList<>(3);
        if (lastModifiedFrom == lastModifiedToInclusive) {
            filters.add(Filters.eq(NodeDocument.MODIFIED_IN_SECS, lastModifiedFrom));
        } else {
            if (lastModifiedFrom == 0 && lastModifiedToInclusive == Long.MAX_VALUE) {
                filters.add(Filters.exists(NodeDocument.MODIFIED_IN_SECS));
            } else {
                if (lastModifiedFrom != 0) {
                    filters.add(Filters.gte(NodeDocument.MODIFIED_IN_SECS, lastModifiedFrom));
                }
                if (lastModifiedToInclusive != Long.MAX_VALUE) {
                    filters.add(Filters.lte(NodeDocument.MODIFIED_IN_SECS, lastModifiedToInclusive));
                }
            }
        }
        if (startAfterDocumentID != null) {
            if (traversingInAscendingOrder) {
                filters.add(Filters.gt(NodeDocument.ID, startAfterDocumentID));
            } else {
                filters.add(Filters.lt(NodeDocument.ID, startAfterDocumentID));
            }
        }
        // If there is only one filter, do not wrap it in an $and
        return filters.size() == 1 ? filters.get(0) : Filters.and(filters);
    }

    @Override
    public String toString() {
        return "DownloadRange{" +
                "lastModifiedFrom=" + lastModifiedFrom +
                ", lastModifiedToInclusive=" + lastModifiedToInclusive +
                ", startAfterDocumentID='" + startAfterDocumentID + '\'' +
                '}';
    }
}
