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

public class Messages {

    public static final byte HEADER_RECORD = 0x00;
    public static final byte HEADER_SEGMENT = 0x01;
    public static final byte HEADER_BLOB = 0x02;

    public static final String GET_HEAD = "h";
    public static final String GET_SEGMENT = "s.";
    public static final String GET_BLOB = "b.";

    private static final String MAGIC = "Standby-CMD@";
    private static final String SEPARATOR = ":";

    private static String newRequest(String clientID, String body) {
        return MAGIC + (clientID == null ? "" : clientID.replace(SEPARATOR, "#")) + SEPARATOR + body + "\r\n";
    }

    public static String newGetHeadReq(String clientID) {
        return newRequest(clientID, GET_HEAD);
    }

    public static String newGetSegmentReq(String clientID, String sid) {
        return newRequest(clientID, GET_SEGMENT + sid);
    }

    public static String newGetBlobReq(String clientID, String blobId) {
        return newRequest(clientID, GET_BLOB + blobId);
    }

    public static String extractMessageFrom(String payload) {
        if (payload.startsWith(MAGIC) && payload.length() > MAGIC.length()) {
            int i = payload.indexOf(SEPARATOR);
            return payload.substring(i + 1);
        }
        return null;
    }

    public static String extractClientFrom(String payload) {
        if (payload.startsWith(MAGIC) && payload.length() > MAGIC.length()) {
            payload = payload.substring(MAGIC.length());
            int i = payload.indexOf(SEPARATOR);
            return payload.substring(0, i);
        }
        return null;
    }
}
