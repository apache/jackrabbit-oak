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

import org.apache.jackrabbit.oak.remote.RemoteBinaryId;
import org.apache.jackrabbit.oak.remote.RemoteSession;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.jackrabbit.oak.remote.http.handler.ResponseUtils.sendBadRequest;
import static org.apache.jackrabbit.oak.remote.http.handler.ResponseUtils.sendInternalServerError;
import static org.apache.jackrabbit.oak.remote.http.handler.ResponseUtils.sendNotFound;

class HeadBinaryHandler implements Handler {

    private static final Pattern REQUEST_PATTERN = Pattern.compile("^/binaries/(.*)$");

    @Override
    public void handle(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
        RemoteSession session = (RemoteSession) request.getAttribute("session");

        if (session == null) {
            sendInternalServerError(response, "session not found");
            return;
        }

        String providedBinaryId = readBinaryId(request);

        if (providedBinaryId == null) {
            sendBadRequest(response, "unable to read the provided binary ID");
            return;
        }

        RemoteBinaryId binaryId = session.readBinaryId(providedBinaryId);

        if (binaryId == null) {
            sendNotFound(response, "binary ID not found");
            return;
        }

        response.setStatus(HttpServletResponse.SC_OK);
        response.setHeader("Accept-Ranges", "bytes");
    }

    /**
     * Extract binary id from request path and return it
     */
    private String readBinaryId(HttpServletRequest request) {
        Matcher matcher = REQUEST_PATTERN.matcher(request.getPathInfo());

        if (matcher.matches()) {
            return matcher.group(1);
        }

        throw new IllegalStateException("handler bound at the wrong path");
    }
}
