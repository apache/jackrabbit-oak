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

import org.apache.jackrabbit.oak.remote.RemoteRevision;
import org.apache.jackrabbit.oak.remote.RemoteSession;
import org.apache.jackrabbit.oak.remote.RemoteTree;
import org.apache.jackrabbit.oak.remote.RemoteTreeFilters;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

import static java.util.Collections.singletonMap;
import static org.apache.jackrabbit.oak.remote.http.handler.ResponseUtils.sendGone;
import static org.apache.jackrabbit.oak.remote.http.handler.ResponseUtils.sendInternalServerError;
import static org.apache.jackrabbit.oak.remote.http.handler.ResponseUtils.sendNotFound;

abstract class HeadTreeHandler implements Handler {

    protected abstract String readPath(HttpServletRequest request);

    protected abstract RemoteRevision readRevision(HttpServletRequest request, RemoteSession session);

    @Override
    public void handle(HttpServletRequest request, HttpServletResponse response) throws IOException {
        RemoteSession session = (RemoteSession) request.getAttribute("session");

        if (session == null) {
            sendInternalServerError(response, "session not found");
            return;
        }

        RemoteRevision revision = readRevision(request, session);

        if (revision == null) {
            sendGone(response, "revision not found");
            return;
        }

        RemoteTree tree = session.readTree(revision, readPath(request), new RemoteTreeFilters());

        if (tree == null) {
            sendNotFound(response, singletonMap("Oak-Revision", revision.asString()), "tree not found");
            return;
        }

        response.setStatus(HttpServletResponse.SC_OK);
        response.setHeader("Oak-Revision", revision.asString());
        response.setContentType("application/json");
    }

}
