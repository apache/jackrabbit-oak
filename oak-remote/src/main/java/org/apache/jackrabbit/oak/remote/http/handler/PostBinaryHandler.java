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
import org.apache.jackrabbit.oak.remote.RemoteBinaryId;
import org.apache.jackrabbit.oak.remote.RemoteSession;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

import static org.apache.jackrabbit.oak.remote.http.handler.ResponseUtils.sendInternalServerError;

class PostBinaryHandler implements Handler {

    @Override
    public void handle(HttpServletRequest request, HttpServletResponse response) throws IOException {
        RemoteSession session = (RemoteSession) request.getAttribute("session");

        if (session == null) {
            sendInternalServerError(response, "session not found");
            return;
        }

        RemoteBinaryId binaryId = session.writeBinary(request.getInputStream());

        response.setStatus(HttpServletResponse.SC_CREATED);
        response.setContentType("application/json");

        ServletOutputStream stream = response.getOutputStream();

        JsonGenerator generator = new JsonFactory().createJsonGenerator(stream, JsonEncoding.UTF8);
        generator.writeStartObject();
        generator.writeStringField("binaryId", binaryId.asString());
        generator.writeEndObject();
        generator.flush();

        stream.close();
    }

}
