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
package org.apache.jackrabbit.oak.segment.consensus.aeron;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;

import java.nio.charset.StandardCharsets;

final class AeronIngressPayloadSupport {

    private AeronIngressPayloadSupport() {
        // utility
    }

    static AeronEncodedMessage encode(int templateId, String json) {
        byte[] jsonBytes = json.getBytes(StandardCharsets.UTF_8);
        int totalLength = SimpleMessageHeader.ENCODED_LENGTH + jsonBytes.length;
        MutableDirectBuffer messageBuffer = new UnsafeBuffer(new byte[totalLength]);
        SimpleMessageHeader.encode(messageBuffer, 0, jsonBytes.length, templateId);
        messageBuffer.putBytes(SimpleMessageHeader.ENCODED_LENGTH, jsonBytes);
        return new AeronEncodedMessage(templateId, json, messageBuffer, totalLength);
    }

    static String escapeJson(String str) {
        if (str == null) {
            return "";
        }
        StringBuilder escaped = new StringBuilder(str.length() + 16);
        JsopBuilder.escape(str, escaped);
        return escaped.toString();
    }
}
