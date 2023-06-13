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
package org.apache.jackrabbit.oak.plugins.document.mongo;

import org.apache.jackrabbit.oak.index.indexer.document.LastModifiedRange;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.bson.BsonDocument;

import java.util.Objects;

public class TraversingRange {

    private final LastModifiedRange lastModifiedRange;
    /**
     * could be null to indicate start from first document in the lastModifiedRange
     */
    private final String startAfterDocumentID;

    public TraversingRange(LastModifiedRange lastModifiedRange, String startAfterDocumentID) {
        this.lastModifiedRange = lastModifiedRange;
        this.startAfterDocumentID = startAfterDocumentID;
    }

    public boolean coversAllDocuments() {
        return lastModifiedRange.coversAllDocuments() && startAfterDocumentID == null;
    }

    public LastModifiedRange getLastModifiedRange() {
        return lastModifiedRange;
    }

    public BsonDocument getFindQuery() {
        String lastModifiedRangeQueryPart = "{$gte:" + lastModifiedRange.getLastModifiedFrom() + ",";
        lastModifiedRangeQueryPart += "$lt:" + lastModifiedRange.getLastModifiedTo() + "}";
        String idRangeQueryPart = "";
        if (startAfterDocumentID != null) {
            String condition = "{$gt:\"" + startAfterDocumentID + "\"}";
            idRangeQueryPart = ", " + NodeDocument.ID + ":" + condition;
        }
        return BsonDocument.parse("{" + NodeDocument.MODIFIED_IN_SECS + ":" + lastModifiedRangeQueryPart
                + idRangeQueryPart + "}");
    }

    public String getStartAfterDocumentID() {
        return startAfterDocumentID;
    }

    @Override
    public String toString() {
        return "Range: " + lastModifiedRange.toString() + ", startAfterDocument: " + startAfterDocumentID;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TraversingRange that = (TraversingRange) o;
        return Objects.equals(lastModifiedRange, that.lastModifiedRange) && Objects.equals(startAfterDocumentID, that.startAfterDocumentID);
    }

    @Override
    public int hashCode() {
        return Objects.hash(lastModifiedRange, startAfterDocumentID);
    }
}