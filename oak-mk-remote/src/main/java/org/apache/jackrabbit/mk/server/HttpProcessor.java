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
package org.apache.jackrabbit.mk.server;

import org.apache.jackrabbit.oak.commons.IOUtils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

/**
 * Process all HTTP requests on a single socket.
 */
public class HttpProcessor {

    private static final int INITIAL_SO_TIMEOUT = 10000;

    private static final int DEFAULT_SO_TIMEOUT = 30000;

    private static final int MAX_KEEP_ALIVE_REQUESTS = 100;

    private final Socket socket;

    private final Servlet servlet;

    private InputStream socketIn;

    private OutputStream socketOut;

    private final Request request = new Request();

    private final Response response = new Response();

    /**
     * Create a new instance of this class.
     *
     * @param socket socket
     * @param servlet servlet to invoke for incoming requests
     */
    public HttpProcessor(Socket socket, Servlet servlet) {
        this.socket = socket;
        this.servlet = servlet;
    }

    /**
     * Process all requests on a single socket.
     *
     * @throws IOException if an I/O error occurs
     */
    public void process() throws IOException {
        try {
            socketIn = new BufferedInputStream(socket.getInputStream());
            socketOut = new BufferedOutputStream(socket.getOutputStream());

            socket.setSoTimeout(INITIAL_SO_TIMEOUT);

            for (int requestNum = 0;; requestNum++) {
                if (!process(requestNum)) {
                    break;
                }
                if (requestNum == 0) {
                    socket.setSoTimeout(DEFAULT_SO_TIMEOUT);
                }
            }
        }  finally {
            IOUtils.closeQuietly(socketOut);
            IOUtils.closeQuietly(socketIn);
            IOUtils.closeQuietly(socket);
        }
    }

    /**
     * Process a single request.
     *
     * @param requestNum number of this request on the same persistent connection
     * @return {@code true} if the connection should be kept alive;
     *         {@code false} otherwise
     *
     * @throws IOException if an I/O error occurs
     */
    private boolean process(int requestNum) throws IOException {
        try {
            request.parse(socketIn);
        } catch (IOException e) {
            if (requestNum == 0) {
                // ignore errors on the very first request (might be wrong protocol)
                return false;
            }
            throw e;
        }
        try {
            boolean keepAlive = request.isKeepAlive() &&
                    (requestNum + 1 < MAX_KEEP_ALIVE_REQUESTS);
            response.recycle(socketOut, keepAlive);
            servlet.service(request, response);
            return keepAlive;
        } finally {
            IOUtils.closeQuietly(request);
            IOUtils.closeQuietly(response);
        }
    }
}
