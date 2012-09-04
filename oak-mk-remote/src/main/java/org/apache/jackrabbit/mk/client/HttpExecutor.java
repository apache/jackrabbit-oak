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
package org.apache.jackrabbit.mk.client;

import org.apache.jackrabbit.mk.remote.util.BoundedInputStream;
import org.apache.jackrabbit.mk.remote.util.ChunkedInputStream;
import org.apache.jackrabbit.mk.util.IOUtils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URLEncoder;
import java.security.SecureRandom;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

import javax.net.SocketFactory;

/**
 * Executes commands as HTTP requests.
 * <p>
 * This class is NOT thread-safe: its execute() method should operate within a
 * lock, which must be held if a result input stream is returned UNTIL this
 * stream is consumed or closed.
 */
class HttpExecutor implements Closeable {

    private final Socket socket;
    
    private InputStream socketIn;
    
    private OutputStream socketOut;
    
//    private final ChunkedOutputStream bodyOut = new ChunkedOutputStream(null);
    private final ByteArrayOutputStream bodyOut = new ByteArrayOutputStream(256);
    
    private final ChunkedInputStream bodyIn = new ChunkedInputStream(null); 
    
    private boolean connectionClosed;
    
    /**
     * Create a new instance of this class.
     * 
     * @param socketFactory socket factory
     * @param socketAddress server address
     * @throws IOException if the server could not be contacted
     */
    public HttpExecutor(SocketFactory socketFactory, InetSocketAddress socketAddress)
	    throws IOException {
        if (socketAddress != null) {
            socket = socketFactory.createSocket(
                    socketAddress.getAddress(), socketAddress.getPort());
        } else {
            socket = socketFactory.createSocket();
        }
    }
    
    /**
     * Execute a request.
     * 
     * @param command command to execute
     * @param params arguments to command
     * @param in bytes to pass
     * @return result input stream
     * 
     * @throws IOException if an I/O error occurs
     */
    public InputStream execute(String command, Map<String, String> params, InputStream in)
            throws IOException {
        
        // send request
        if (socketOut == null) {
            socketOut = new BufferedOutputStream(socket.getOutputStream());
        }
        String contentType = "application/x-www-form-urlencoded";
        String boundary = null;
        if (in != null) {
            boundary = getBoundary();
            contentType = "multipart/form-data; boundary=" + boundary;
        }
        
        writeLine(String.format("POST /%s HTTP/1.1", command));
        writeLine(String.format("Content-Type: %s", contentType));

        if (in != null) {
            bodyOut.write("--".getBytes());
            bodyOut.write(boundary.getBytes());
            bodyOut.write("\r\n".getBytes());
            bodyOut.write("Content-Disposition: form-data; name=\"file\"; filename=\"upload\"\r\n".getBytes());
            bodyOut.write("Content-Type: application/octet-stream\r\n".getBytes());
            bodyOut.write("\r\n".getBytes());
            IOUtils.copy(in, bodyOut);
            bodyOut.write("\r\n".getBytes());
            bodyOut.write("--".getBytes());
            bodyOut.write(boundary.getBytes());
            bodyOut.write("--".getBytes	());
        } else {
            for (Map.Entry<String,String> param : params.entrySet()) {
                String s = String.format("%s=%s&", 
                        URLEncoder.encode(param.getKey(), "8859_1"),
                        URLEncoder.encode(param.getValue(), "8859_1"));
                bodyOut.write(s.getBytes());
            }
        }
        
        byte[] data = bodyOut.toByteArray();
        writeLine(String.format("Content-Length: %d", data.length));
        writeLine("");     
        socketOut.write(data);
        socketOut.flush();
        
        // read response
        if (socketIn == null) {
            socketIn = new BufferedInputStream(socket.getInputStream());
        }
        
        String responseLine = readLine(socketIn);
        String[] parts = responseLine.split(" ");
        if (parts.length < 3) {
            String msg = String.format("Malformed HTTP response line: %s", responseLine);
            throw new IOException(msg);
        }
        
        int statusCode;
        
        try {
            statusCode = Integer.parseInt(parts[1]);
        } catch (RuntimeException e) {
            String msg = String.format("Malformed HTTP response line: %s", responseLine);
            throw new IOException(msg);
        }
        
        Map<String, String> headers = new LinkedHashMap<String, String>();
        
        for (;;) {
            String headerLine = readLine(socketIn);
            if (headerLine.length() == 0) {
                break;
            }
            parts = headerLine.split(":");
            if (parts.length == 2) {
                headers.put(parts[0].trim(), parts[1].trim());
            }
        }

        InputStream reqIn;
        
        String encoding = headers.get("Transfer-Encoding");
        if ("chunked".equalsIgnoreCase(encoding)) {
            bodyIn.recycle(socketIn);
            reqIn = bodyIn;
        } else {
            int contentLength = -1;
            
            String s = headers.get("Content-Length");
            if (s != null) {
                try {
                    contentLength = Integer.parseInt(s);
                } catch (RuntimeException e) {
                    /* ignore */
                }
            }
            if (contentLength == -1) {
                contentLength = 0;
            }
            reqIn = new BoundedInputStream(socketIn, contentLength);
        }
        
        String connectionState = headers.get("Connection");
        if ("close".equalsIgnoreCase(connectionState)) {
            connectionClosed = true;
        }
        
        switch (statusCode) {
        case 200:
            return reqIn;
        case 500:
            try {
                throw new IOException(readLine(reqIn));
            } finally {
                IOUtils.closeQuietly(reqIn);
            }
        default:
            String msg = String.format("HTTP request failed with status code: %d", statusCode);
            throw new IOException(msg);
        }
    }
    
    /**
     * Return a flag indicating whether the executor is alive.
     *
     * @return {@code true} if it is alive; {@code false} otherwise
     */
    public boolean isAlive() {
        return !connectionClosed && !socket.isClosed();
    }
    
    /**
     * Close this executor.
     */
    @Override
    public void close() {
        IOUtils.closeQuietly(socketOut);
        IOUtils.closeQuietly(socketIn);
        IOUtils.closeQuietly(socket);
    }

    /**
     * Write a request header.
     * 
     * @param s line
     * @throws IOException if an I/O error occurs
     */
    private void writeLine(String s) throws IOException {
        socketOut.write(s.getBytes());
        socketOut.write("\r\n".getBytes());
    }

    /**
     * Read a single line, terminated by a CR LF combination from an input.
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

    /**
     * Boundary.
     */
    private static String boundary; 
    
    /**
     * Boundary characters.
     */
    private static final char[] BOUNDARY_CHARACTERS = 
            "-_1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray();
    
    /**
     * Return a multipart boundary.
     * 
     * @return boundary
     */
    private static String getBoundary() {
        if (boundary == null) {
            StringBuilder b = new StringBuilder();
            Random random = new SecureRandom();
            
            for (int i = 0; i < 16; i++) {
                b.append(BOUNDARY_CHARACTERS[random.nextInt(BOUNDARY_CHARACTERS.length)]);
            }
            boundary = String.format("----ClientFormBoundary%s", b.toString());
        }
        return boundary;
    }
}
