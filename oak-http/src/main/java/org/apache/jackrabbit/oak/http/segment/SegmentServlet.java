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
package org.apache.jackrabbit.oak.http.segment;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.UUID;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.jackrabbit.oak.plugins.segment.RecordId;
import org.apache.jackrabbit.oak.plugins.segment.Segment;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.plugins.segment.SegmentStore;

import com.google.common.io.ByteStreams;

public abstract class SegmentServlet extends HttpServlet {

    protected abstract SegmentStore getSegmentStore();

    private UUID getSegmentId(String info) {
        try {
            return UUID.fromString(info);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private RecordId getRecordId(BufferedReader reader) throws IOException {
        try {
            return RecordId.fromString(reader.readLine());
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    @Override
    protected void doGet(
            HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        String info = request.getPathInfo();
        if (info == null || info.equals("") || info.equals("/")) {
            response.setContentType("text/plain; charset=UTF-8");
            SegmentNodeState head = getSegmentStore().getHead();
            response.getWriter().write(head.getRecordId().toString());
        } else if (info.startsWith("/")) {
            doGetSegment(info.substring(1, info.length()), response);
        } else {
            response.sendError(HttpServletResponse.SC_NOT_FOUND);
        }
    }

    @Override
    protected void doPut(
            HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        String info = request.getPathInfo();
        if (info == null || info.equals("") || info.equals("/")) {
            RecordId id = getRecordId(request.getReader());
            if (id == null) {
                response.sendError(HttpServletResponse.SC_BAD_REQUEST);
            } else {
                SegmentStore store = getSegmentStore();
                SegmentNodeState head = new SegmentNodeState(
                        store.getWriter().getDummySegment(), id);
                if (store.setHead(store.getHead(), head)) {
                    response.setStatus(HttpServletResponse.SC_OK);
                } else {
                    response.sendError(HttpServletResponse.SC_CONFLICT);
                }
            }
        } else if (info.startsWith("/")) {
            doPutSegment(info.substring(1, info.length()), request, response);
        } else {
            response.sendError(HttpServletResponse.SC_NOT_FOUND);
        }
    }

    private void doGetSegment(
            String info, HttpServletResponse response)
            throws ServletException, IOException {
        UUID uuid = getSegmentId(info);
        if (uuid == null) {
            response.sendError(HttpServletResponse.SC_NOT_FOUND);
            return;
        }

        Segment segment = getSegmentStore().readSegment(uuid);
        if (segment == null) {
            response.sendError(HttpServletResponse.SC_NOT_FOUND);
            return;
        }

        response.setContentType("application/octet-stream");
        segment.writeTo(response.getOutputStream());
    }

    private void doPutSegment(
            String info,
            HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        UUID uuid = getSegmentId(info);
        if (uuid == null) {
            response.sendError(HttpServletResponse.SC_NOT_FOUND);
        } else if (getSegmentStore().readSegment(uuid) != null) {
            // can't modify an existing segment
            response.sendError(HttpServletResponse.SC_FORBIDDEN);
        } else {
            // TODO: sanity check the segment data?
            byte[] data = ByteStreams.toByteArray(request.getInputStream());
            getSegmentStore().writeSegment(uuid, data, 0, data.length);
            response.setStatus(HttpServletResponse.SC_OK);
        }
    }

}
