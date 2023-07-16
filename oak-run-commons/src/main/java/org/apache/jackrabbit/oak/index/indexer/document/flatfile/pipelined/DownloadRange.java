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

import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.bson.BsonDocument;

final class DownloadRange {
    private final long lastModifiedFrom;
    private final long lastModifiedTo;
    private final String startAfterDocumentID;

    public DownloadRange(long lastModifiedFrom, long lastModifiedTo, String startAfterDocumentID) {
        if (lastModifiedTo < lastModifiedFrom) {
            throw new IllegalArgumentException("Invalid range (" + lastModifiedFrom + ", " + lastModifiedTo + ")");
        }
        this.lastModifiedFrom = lastModifiedFrom;
        this.lastModifiedTo = lastModifiedTo;
        this.startAfterDocumentID = startAfterDocumentID;
    }

    public String getStartAfterDocumentID() {
        return startAfterDocumentID;
    }

    public long getLastModifiedFrom() {
        return lastModifiedFrom;
    }

    public long getLastModifiedTo() {
        return lastModifiedTo;
    }

    public BsonDocument getFindQuery() {
        String lastModifiedRangeQueryPart = "{$gte:" + lastModifiedFrom;
        if (lastModifiedTo == Long.MAX_VALUE) {
            lastModifiedRangeQueryPart += "}";
        } else {
            lastModifiedRangeQueryPart += ", $lt:" + lastModifiedTo + "}";
        }
        String idRangeQueryPart = "";
        if (startAfterDocumentID != null) {
            String condition = "{$gt:\"" + startAfterDocumentID + "\"}";
            idRangeQueryPart = ", " + NodeDocument.ID + ":" + condition;
        }
        return BsonDocument.parse("{" + NodeDocument.MODIFIED_IN_SECS + ":" + lastModifiedRangeQueryPart
                + idRangeQueryPart + "}");
    }

    @Override
    public String toString() {
        return "DownloadRange{" +
                "lastModifiedFrom=" + lastModifiedFrom +
                ", lastModifiedTo=" + lastModifiedTo +
                ", startAfterDocumentID='" + startAfterDocumentID + '\'' +
                '}';
    }
}
