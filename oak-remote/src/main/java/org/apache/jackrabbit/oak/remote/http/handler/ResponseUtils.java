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

package org.apache.jackrabbit.oak.remote.http.handler;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

class ResponseUtils {

    private ResponseUtils() {
    }

    private static void send(HttpServletResponse response, int code, String message) throws IOException {
        send(response, code, new HashMap<String, String>(), message);
    }

    private static void send(HttpServletResponse response, int code, Map<String, String> headers, String message) throws IOException {
        response.setStatus(code);
        response.setContentType("application/json");

        for (Map.Entry<String, String> entry : headers.entrySet()) {
            response.setHeader(entry.getKey(), entry.getValue());
        }

        ServletOutputStream stream = response.getOutputStream();

        JsonGenerator generator = new JsonFactory().createJsonGenerator(stream, JsonEncoding.UTF8);
        generator.writeStartObject();
        generator.writeStringField("error", message);
        generator.writeEndObject();
        generator.flush();

        stream.close();
    }

    public static void sendBadRequest(HttpServletResponse response, String message) throws IOException {
        send(response, HttpServletResponse.SC_BAD_REQUEST, message);
    }

    public static void sendInternalServerError(HttpServletResponse response, String message) throws IOException {
        send(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, message);
    }

    public static void sendGone(HttpServletResponse response, String message) throws IOException {
        send(response, HttpServletResponse.SC_GONE, message);
    }

    public static void sendNotFound(HttpServletResponse response, String message) throws IOException {
        send(response, HttpServletResponse.SC_NOT_FOUND, message);
    }

    public static void sendNotFound(HttpServletResponse response, Map<String, String> headers, String message) throws IOException {
        send(response, HttpServletResponse.SC_NOT_FOUND, headers, message);
    }

    public static void sendUnauthorized(HttpServletResponse response, String message) throws IOException {
        send(response, HttpServletResponse.SC_UNAUTHORIZED, message);
    }

    public static void sendUnauthorized(HttpServletResponse response, Map<String, String> headers, String message) throws IOException {
        send(response, HttpServletResponse.SC_UNAUTHORIZED, headers, message);
    }


}
