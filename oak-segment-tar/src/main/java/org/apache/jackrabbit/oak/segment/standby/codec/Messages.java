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

package org.apache.jackrabbit.oak.segment.standby.codec;

final class Messages {

    static final byte HEADER_RECORD = 0x00;

    static final byte HEADER_SEGMENT = 0x01;

    static final byte HEADER_BLOB = 0x02;

    static final byte HEADER_REFERENCES = 0x03;

    static final String GET_HEAD = "h";

    static final String GET_SEGMENT = "s.";

    static final String GET_BLOB = "b.";

    static final String GET_REFERENCES = "r.";

    private static final String MAGIC = "Standby-CMD@";

    private static final String SEPARATOR = ":";

    private Messages() {}

    private static String newRequest(String clientId, String body, boolean delimited) {
        StringBuilder builder = new StringBuilder(MAGIC);

        if (clientId != null) {
            builder.append(clientId.replace(SEPARATOR, "#"));
        }

        builder.append(SEPARATOR);
        builder.append(body);

        if (delimited) {
            builder.append("\r\n");
        }

        return builder.toString();
    }

    static String newGetHeadRequest(String clientId, boolean delimited) {
        return newRequest(clientId, GET_HEAD, delimited);
    }

    static String newGetHeadRequest(String clientId) {
        return newGetHeadRequest(clientId, true);
    }

    static String newGetSegmentRequest(String clientId, String segmentId, boolean delimited) {
        return newRequest(clientId, GET_SEGMENT + segmentId, delimited);
    }

    static String newGetSegmentRequest(String clientId, String segmentId) {
        return newGetSegmentRequest(clientId, segmentId, true);
    }

    static String newGetReferencesRequest(String clientId, String segmentId, boolean delimited) {
        return newRequest(clientId, GET_REFERENCES + segmentId, delimited);
    }

    static String newGetReferencesRequest(String clientId, String segmentId) {
        return newGetReferencesRequest(clientId, segmentId, true);
    }

    static String newGetBlobRequest(String clientId, String blobId, boolean delimited) {
        return newRequest(clientId, GET_BLOB + blobId, delimited);
    }

    static String newGetBlobRequest(String clientId, String blobId) {
        return newGetBlobRequest(clientId, blobId, true);
    }

    static String extractMessageFrom(String payload) {
        if (payload.startsWith(MAGIC) && payload.length() > MAGIC.length()) {
            int i = payload.indexOf(SEPARATOR);
            return payload.substring(i + 1);
        }
        return null;
    }

    static String extractClientFrom(String payload) {
        if (payload.startsWith(MAGIC) && payload.length() > MAGIC.length()) {
            payload = payload.substring(MAGIC.length());
            int i = payload.indexOf(SEPARATOR);
            return payload.substring(0, i);
        }
        return null;
    }

}
