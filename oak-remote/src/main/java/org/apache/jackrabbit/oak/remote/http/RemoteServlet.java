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

package org.apache.jackrabbit.oak.remote.http;

import org.apache.jackrabbit.oak.remote.RemoteRepository;
import org.apache.jackrabbit.oak.remote.http.handler.Handler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Arrays;

import static org.apache.jackrabbit.oak.remote.http.handler.Handlers.createGetBinaryHandler;
import static org.apache.jackrabbit.oak.remote.http.handler.Handlers.createGetLastRevisionHandler;
import static org.apache.jackrabbit.oak.remote.http.handler.Handlers.createGetLastTreeHandler;
import static org.apache.jackrabbit.oak.remote.http.handler.Handlers.createGetRevisionTreeHandler;
import static org.apache.jackrabbit.oak.remote.http.handler.Handlers.createHeadBinaryHandler;
import static org.apache.jackrabbit.oak.remote.http.handler.Handlers.createHeadLastTreeHandler;
import static org.apache.jackrabbit.oak.remote.http.handler.Handlers.createHeadRevisionTreeHandler;
import static org.apache.jackrabbit.oak.remote.http.handler.Handlers.createNotFoundHandler;
import static org.apache.jackrabbit.oak.remote.http.handler.Handlers.createPatchLastRevisionHandler;
import static org.apache.jackrabbit.oak.remote.http.handler.Handlers.createPatchSpecificRevisionHandler;
import static org.apache.jackrabbit.oak.remote.http.handler.Handlers.createPostBinaryHandler;
import static org.apache.jackrabbit.oak.remote.http.handler.Handlers.createSearchLastRevisionHandler;
import static org.apache.jackrabbit.oak.remote.http.handler.Handlers.createSearchSpecificRevisionHandler;
import static org.apache.jackrabbit.oak.remote.http.matcher.Matchers.matchesRequest;

public class RemoteServlet extends HttpServlet {

    private static final Logger logger = LoggerFactory.getLogger(RemoteServlet.class);

    private final RemoteRepository repository;

    public RemoteServlet(RemoteRepository repository) {
        this.repository = repository;
    }

    @Override
    protected void service(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        request.setAttribute("repository", repository);

        try {
            firstMatching(readHandlers(), request, createNotFoundHandler()).handle(request, response);
        } catch (ServletException e) {
            logger.error("unable to serve the current request", e);
            throw e;
        } catch (IOException e) {
            logger.error("I/O error while serving the current request", e);
            throw e;
        } catch (Exception e) {
            logger.error("unexpected error while serving the current request", e);
            throw new ServletException(e);
        }
    }

    private Handler firstMatching(Iterable<RemoteHandler> handlers, HttpServletRequest request, Handler otherwise) {
        for (RemoteHandler handler : handlers) {
            if (handler.match(request)) {
                return handler;
            }
        }

        return otherwise;
    }

    private Iterable<RemoteHandler> readHandlers() {
        return handlers(
                handler("get", "/revisions/last", createGetLastRevisionHandler()),
                handler("get", "/revisions/last/tree/.*", createGetLastTreeHandler()),
                handler("head", "/revisions/last/tree/.*", createHeadLastTreeHandler()),
                handler("get", "/revisions/[^/]+/tree/.*", createGetRevisionTreeHandler()),
                handler("head", "/revisions/[^/]+/tree/.*", createHeadRevisionTreeHandler()),
                handler("head", "/binaries/.*", createHeadBinaryHandler()),
                handler("get", "/binaries/.*", createGetBinaryHandler()),
                handler("post", "/binaries", createPostBinaryHandler()),
                handler("patch", "/revisions/last/tree", createPatchLastRevisionHandler()),
                handler("patch", "/revisions/[^/]+/tree", createPatchSpecificRevisionHandler()),
                handler("get", "/revisions/last/tree", createSearchLastRevisionHandler()),
                handler("get", "/revisions/[^/]+/tree", createSearchSpecificRevisionHandler())
        );
    }

    private Iterable<RemoteHandler> handlers(RemoteHandler... handlers) {
        return Arrays.asList(handlers);
    }

    private RemoteHandler handler(String method, String path, Handler handler) {
        return new RemoteHandler(matchesRequest(method, path), handler);
    }

}
