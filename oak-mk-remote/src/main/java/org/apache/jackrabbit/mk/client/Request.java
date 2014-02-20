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

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.net.SocketFactory;

import org.apache.jackrabbit.oak.commons.IOUtils;

/**
 * Contains the details of a request to some remote {@code MicroKernel}
 * implementation.
 */
class Request implements Closeable {

    private final SocketFactory socketFactory;

    private final InetSocketAddress socketAddress;
    
    private final String command;
    
    private final Map<String, String> params = new LinkedHashMap<String, String>();
    
    private InputStream in;
    
    /**
     * Create a new instance of this class.
     * 
     * @param socketFactory socket factory
     * @param socketAddress server address
     * @param command command name
     */
    public Request(
            SocketFactory socketFactory, InetSocketAddress socketAddress,
            String command) {
        this.socketFactory = socketFactory;
        this.socketAddress = socketAddress;
        this.command = command;
    }

    /**
     * Add a string parameter.
     *
     * @param name name
     * @param value value, if {@code null} the call is ignored
     */
    public Request addParameter(String name, String value) {
        if (value != null) {
            params.put(name, value);
        }
        return this;
    }

    /**
     * Add an integer parameter, equivalent to 
     * {@code addParameter(name, String.valueOf(value))}.
     *
     * @param name name
     * @param value value
     */
    public Request addParameter(String name, int value) {
        params.put(name, String.valueOf(value));
        return this;
    }
    
    /**
     * Add a long parameter, equivalent to 
     * {@code addParameter(name, String.valueOf(value))}.
     *
     * @param name name
     * @param value value
     */
    public Request addParameter(String name, long value) {
        params.put(name, String.valueOf(value));
        return this;
    }
    
    /**
     * Add a file parameter that will be transmitted as form data. 
     * 
     * @param name name
     * @param in input stream
     */
    public Request addFileParameter(String name, InputStream in) {
        this.in = in;
        return this;
    }
    
    /**
     * Execute the request.
     * 
     * @throws IOException if an I/O error occurs
     */
    private byte[] execute() throws IOException {
        HttpExecutor executor = new HttpExecutor(socketFactory, socketAddress);
        try {
            InputStream stream = executor.execute(command, params, in);
            try {
                ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                IOUtils.copy(stream, buffer);
                return buffer.toByteArray();
            } finally {
                stream.close();
            }
        } finally {
            executor.close();
        }
    }

    /**
     * Return a string from the result stream. Automatically executes
     * the request first.
     * 
     * @return string
     * @throws IOException if an I/O error occurs
     */
    public String getString() throws IOException {
        return new String(execute(), "8859_1");
    }

    /**
     * Return a boolean from the result stream, equivalent to 
     * {@code Boolean.parseBoolean(getString())}.
     * Automatically executes the request first.
     *
     * @return boolean
     * @throws IOException if an I/O error occurs
     */
    public boolean getBoolean() throws IOException {
        return Boolean.parseBoolean(getString());
    }
    
    /**
     * Return a long from the result stream, equivalent to 
     * {@code Long.parseLong(getString())}.
     * Automatically executes the request first.
     *
     * @return boolean
     * @throws IOException if an I/O error occurs
     */
    public long getLong() throws IOException {
        return Long.parseLong(getString());
    }

    /**
     * Read bytes from the result stream. Automatically executes the
     * request first.
     *
     * @param b buffer
     * @param off offset
     * @param len length
     * @return number of bytes or {@code -1} if no more bytes are available
     *
     * @throws IOException if an I/O error occurs
     */
    public int read(byte[] b, int off, int len) throws IOException {
        if (len == 0) {
            return 0;
        }

        byte[] bytes = execute();
        len = Math.min(bytes.length, len);
        if (len == 0) {
            return -1;
        }

        System.arraycopy(bytes, 0, b, off, len);
        return len;
    }

    @Override
    public void close() {
        // do nothing
    }

}
