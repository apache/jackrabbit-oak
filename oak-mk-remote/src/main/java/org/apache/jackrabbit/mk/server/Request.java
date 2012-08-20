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

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLDecoder;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.jackrabbit.mk.remote.util.BoundedInputStream;
import org.apache.jackrabbit.mk.remote.util.ChunkedInputStream;
import org.apache.jackrabbit.mk.util.IOUtils;

/**
 * HTTP Request implementation.
 */
class Request implements Closeable {

    private static final String HTTP_11_PROTOCOL = "HTTP/1.1";

    private InputStream in;

    private String method;

    private String file;

    private String queryString;

    private String protocol;

    private final Map<String, String> headers = new LinkedHashMap<String, String>();

    private boolean paramsChecked;

    private final Map<String, String> params = new LinkedHashMap<String, String>();

    private final ChunkedInputStream chunkedIn = new ChunkedInputStream(null);

    private InputStream reqIn;

    /**
     * Parse a request. This automatically resets any internal state, so it can be
     * used multiple times
     *
     * @param in input stream
     * @throws IOException if an I/O error occurs
     */
    void parse(InputStream in) throws IOException {
        String requestLine = readLine(in);

        String[] parts = requestLine.split(" ");
        if (parts.length != 3) {
            String msg = String.format("Bad HTTP request line: %s", requestLine);
            throw new IOException(msg);
        }
        method = parts[0];

        String uri = parts[1];
        int index = uri.lastIndexOf('?');
        if (index == -1) {
            file = uri;
            queryString = null;
        } else {
            file = uri.substring(0, index);
            queryString = uri.substring(index + 1);
        }

        protocol = parts[2];

        headers.clear();

        for (;;) {
            String headerLine = readLine(in);
            if (headerLine.length() == 0) {
                break;
            }
            parts = headerLine.split(":");
            if (parts.length == 2) {
                headers.put(parts[0].trim().toLowerCase(), parts[1].trim());
            }
        }

        params.clear();
        paramsChecked = false;
        reqIn = null;

        this.in = in;
    }

    /**
     * Read a single line, terminated by a CR LF combination from an {@code InputStream}.
     *
     * @return line
     * @throws IOException if an I/O error occurs
     */
    private static String readLine(InputStream in) throws IOException {
        StringBuilder line = new StringBuilder(128);

        for (;;) {
            int c = in.read();
            switch (c) {
            case '\r':
                // swallow
                break;
            case '\n':
                return line.toString();
            case -1:
                throw new EOFException();
            default:
                line.append((char) c);
            }
        }
    }

    public String getMethod() {
        return method;
    }

    public String getFile() {
        return file;
    }

    private String getContentType() {
        String ct = headers.get("content-type");
        if (ct != null) {
            int sep = ct.indexOf(';');
            if (sep != -1) {
                ct = ct.substring(0, sep).trim();
            }
        }
        return ct;
    }

    private int getContentLength() {
        String s = headers.get("content-length");
        if (s != null) {
            try {
                return Integer.parseInt(s);
            } catch (RuntimeException e) {
                /* ignore */
            }
        }
        return -1;
    }
    
    public String getUserAgent() {
        return headers.get("user-agent");
    }


    public String getQueryString() {
        return queryString;
    }

    public String getParameter(String name) throws IOException {
        if (!paramsChecked) {
            try {
                String contentType = getContentType();
                if ("application/x-www-form-urlencoded".equals(contentType)) {
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    IOUtils.copy(getInputStream(), out);
                    collectParameters(out.toString(), params);
                }
            } finally {
                paramsChecked = true;
            }
        }
        return params.get(name);
    }

    public String getParameter(String name, String defaultValue) throws IOException {
        String s = getParameter(name);
        if (s != null) {
            return s;
        }
        return defaultValue;
    }

    public int getParameter(String name, int defaultValue) throws IOException {
        String s = getParameter(name);
        if (s != null) {
            try {
                return Integer.parseInt(s);
            } catch (RuntimeException e) {
                /* ignore */
            }
        }
        return defaultValue;
    }

    public long getParameter(String name, long defaultValue) throws IOException {
        String s = getParameter(name);
        if (s != null) {
            try {
                return Long.parseLong(s);
            } catch (RuntimeException e) {
                /* ignore */
            }
        }
        return defaultValue;
    }

    public InputStream getFileParameter(String name) throws IOException {
        String ct = getContentType();
        if (ct == null || !ct.startsWith("multipart/form-data")) {
            return null;
        }
        if (reqIn != null) {
            /* might already be consumed */
            return null;
        }

        InputStream body = getInputStream();
        String boundary = readLine(body);

        for (;;) {
            String line = readLine(body);
            if (line.length() == 0) {
                break;
            }
            // TODO evaluate other information (such as mime type)
        }
        return new BoundaryInputStream(body, boundary);
    }

    private static void collectParameters(String s, Map<String, String> map) throws IOException {
        for (String param : s.split("&")) {
            String[] nv = param.split("=", 2);
            if (nv.length == 2) {
                map.put(URLDecoder.decode(nv[0], "UTF-8"), URLDecoder.decode(nv[1], "UTF-8"));
            }
        }
    }

    public InputStream getInputStream() {
        if (reqIn == null) {
            String encoding = headers.get("transfer-encoding");
            if ("chunked".equalsIgnoreCase(encoding)) {
                chunkedIn.recycle(in);
                reqIn = chunkedIn;
            } else {
                int contentLength = getContentLength();
                if (contentLength == -1) {
                    contentLength = 0;
                }
                reqIn = new BoundedInputStream(in, contentLength);
            }
        }
        return reqIn;
    }

    boolean isKeepAlive() {
        return HTTP_11_PROTOCOL.equals(protocol);
    }

    @Override
    public void close() {
        if (in != null) {
            try {
                // Consume a possibly non-empty body by triggering the
                // creation of our request input stream
                getInputStream();
                IOUtils.closeQuietly(reqIn);
            } finally {
                in = null;
            }
        }
    }
}
