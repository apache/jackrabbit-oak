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

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.jackrabbit.mk.util.IOUtils;

import static org.apache.jackrabbit.mk.remote.util.ChunkedInputStream.MAX_CHUNK_SIZE;

/**
 * HTTP Response implementation.
 */
class Response implements Closeable {

    private OutputStream out;

    private boolean keepAlive;

    private boolean headersSent;

    private boolean committed;

    private boolean chunked;

    private int statusCode;

    private String contentType;

    private final BodyOutputStream bodyOut = new BodyOutputStream();

    private OutputStream respOut;

    private final Map<String, String> headers = new LinkedHashMap<String, String>();

    /**
     * Recycle this instance, using another output stream and a keep-alive flag.
     *
     * @param out output stream
     * @param keepAlive whether to keep alive the connection
     */
    void recycle(OutputStream out, boolean keepAlive) {
        this.out = out;
        this.keepAlive = keepAlive;

        headersSent = committed = chunked = false;
        statusCode = 0;
        contentType = null;
        bodyOut.reset();
        respOut = null;
        headers.clear();
    }

    /**
     * Return the status message associated with a status code.
     *
     * @param sc status code
     * @return associated status message
     */
    private static String getStatusMsg(int sc) {
        switch (sc) {
        case 200:
            return "OK";
        case 400:
            return "Bad request";
        case 401:
            return "Unauthorized";
        case 404:
            return "Not found";
        default:
            return "Internal server error";
        }
    }

    private void sendHeaders() throws IOException {
        if (headersSent) {
            return;
        }

        headersSent = true;

        int statusCode = this.statusCode;
        if (statusCode == 0) {
            statusCode = 200;
        }
        String msg = getStatusMsg(statusCode);
        if (respOut == null) {
            /* Generate minimal body  */
            String body = String.format(
                    "<!DOCTYPE HTML PUBLIC \"-//IETF//DTD HTML 2.0//EN\">" +
                    "<html><head>" +
                    "<title>%d %s</title>" +
                    "</head><body>" +
                    "<h1>%s</h1>" +
                    "</body></html>", statusCode, msg, msg);
            setContentType("text/html");
            write(body);
        }

        writeLine(String.format("HTTP/1.1 %d %s", statusCode, msg));

        if (committed) {
            writeLine(String.format("Content-Length: %d", bodyOut.getCount()));
        } else {
            chunked = true;
            writeLine("Transfer-Encoding: chunked");
        }
        if (contentType != null) {
            writeLine(String.format("Content-Type: %s", contentType));
        }
        if (!keepAlive) {
            writeLine("Connection: Close");
        }
        for (Map.Entry<String, String> header : headers.entrySet()) {
            writeLine(String.format("%s: %s", header.getKey(), header.getValue()));
        }

        writeLine("");

        if (out != null) {
            out.flush();
        }
    }

    @Override
    public void close() throws IOException {
        committed = true;

        try {
            sendHeaders();
            IOUtils.closeQuietly(respOut);

            if (out != null) {
                out.flush();
            }
        } finally {
            out = null;
        }
    }

    private void writeLine(String s) throws IOException {
        if (out == null) {
            return;
        }
        out.write(s.getBytes());
        out.write("\r\n".getBytes());
    }

    /**
     * Write some bytes to the body of the response.
     * @param b buffer
     * @param off offset
     * @param len length
     * @throws IOException if an I/O error occurs
     */
    void writeBody(byte[] b, int off, int len) throws IOException {
        if (out == null) {
            return;
        }

        sendHeaders();

        if (chunked) {
            out.write(String.format("%04X\r\n", len).getBytes());
        }
        out.write(b, off, len);
        if (chunked) {
            out.write(("\r\n").getBytes());
        }
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    public OutputStream getOutputStream() {
        if (respOut == null) {
            respOut = bodyOut;
        }
        return respOut;
    }

    public void setStatusCode(int statusCode) {
        this.statusCode = statusCode;
    }

    public void addHeader(String name, String value) {
        headers.put(name, value);
    }

    public void write(String s) throws IOException {
        getOutputStream().write(s.getBytes("8859_1"));
    }

    /**
     * Internal {@code OutputStream} passed to servlet handlers.
     */
    class BodyOutputStream extends OutputStream {

        /**
         * Buffer size chosen intentionally to not exceed maximum chunk
         * size we'd like to transmit.
         */
        private final byte[] buf = new byte[MAX_CHUNK_SIZE];

        private int offset;

        /**
         * Return the number of valid bytes in the buffer.
         *
         * @return number of bytes
         */
        public int getCount() {
            return offset;
        }

        @Override
        public void write(int b) throws IOException {
            if (offset == buf.length) {
                writeBody(buf, 0, offset);
                offset = 0;
            }
            buf[offset++] = (byte) b;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            int count = 0;
            while (count < len) {
                if (offset == buf.length) {
                    writeBody(buf, 0, offset);
                    offset = 0;
                }
                int n = Math.min(len - count, buf.length - offset);
                System.arraycopy(b, off + count, buf, offset, n);
                count += n;
                offset += n;
            }
        }

        @Override
        public void flush() throws IOException {
            if (offset > 0) {
                writeBody(buf, 0, offset);
                offset = 0;
            }
        }

        public void reset() {
            offset = 0;
        }

        @Override
        public void close() throws IOException {
            flush();

            writeBody(buf, 0, 0);
        }
    }
}
