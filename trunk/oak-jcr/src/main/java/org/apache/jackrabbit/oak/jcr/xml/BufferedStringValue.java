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
package org.apache.jackrabbit.oak.jcr.xml;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;

import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFactory;

import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.xml.TextValue;
import org.apache.jackrabbit.util.Base64;
import org.apache.jackrabbit.util.TransientFileFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code BufferedStringValue} represents an appendable
 * serialized value that is either buffered in-memory or backed
 * by a temporary file if its size exceeds a certain limit.
 * <p>
 * <b>Important:</b> Note that in order to free resources
 * {@code {@link #dispose()}} should be called as soon as
 * {@code BufferedStringValue} instance is not used anymore.
 */
class BufferedStringValue implements TextValue {

    private static final Logger log = LoggerFactory.getLogger(BufferedStringValue.class);

    /**
     * The maximum size for buffering data in memory.
     */
    private static final int MAX_BUFFER_SIZE = 0x10000;

    /**
     * The in-memory buffer.
     */
    private StringWriter buffer;

    /**
     * The number of characters written so far.
     * If the in-memory buffer is used, this is position within buffer (size of actual data in buffer)
     */
    private long length;

    /**
     * Backing temporary file created when size of data exceeds
     * MAX_BUFFER_SIZE.
     */
    private File tmpFile;

    /**
     * Writer used to write to tmpFile.
     */
    private Writer writer;

    private final NamePathMapper namePathMapper;
    private final ValueFactory valueFactory;

    /**
     * Whether the value is base64 encoded.
     */
    private final boolean base64;

    /**
     * Constructs a new empty {@code BufferedStringValue}.
     *
     * @param valueFactory The value factory
     * @param namePathMapper the name/path mapper
     */
    protected BufferedStringValue(
            ValueFactory valueFactory, NamePathMapper namePathMapper,
            boolean base64) {
        buffer = new StringWriter();
        length = 0;
        tmpFile = null;
        writer = null;
        this.namePathMapper = namePathMapper;
        this.valueFactory = valueFactory;
        this.base64 = base64;
    }

    /**
     * Returns the length of the serialized value.
     *
     * @return the length of the serialized value
     * @throws IOException if an I/O error occurs
     */
    public long length() throws IOException {
        return length;
    }

    @Override
    public String getString() {
        try {
            return retrieveString();
        } catch (IOException e) {
            log.error("could not retrieve string value", e);
            return "";
        }
    }

    private String retrieveString() throws IOException {
        String value = retrieve();
        if (base64) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            Base64.decode(value, out);
            value = new String(out.toByteArray(), "UTF-8");
        }
        return value;
    }

    /**
     * Retrieves the serialized value.
     *
     * @return the serialized value
     * @throws IOException if an I/O error occurs
     */
    public String retrieve() throws IOException {
        if (buffer != null) {
            return buffer.toString();
        } else if (tmpFile != null) {
            // close writer first
            writer.close();
            if (tmpFile.length() > Integer.MAX_VALUE) {
                throw new IOException("size of value is too big, use reader()");
            }
            StringBuilder sb = new StringBuilder((int) length);
            char[] chunk = new char[0x2000];
            Reader reader = openReader();
            try {
                int read;
                while ((read = reader.read(chunk)) > -1) {
                    sb.append(chunk, 0, read);
                }
            } finally {
                reader.close();
            }
            return sb.toString();
        } else {
            throw new IOException("this instance has already been disposed");
        }
    }

    private Reader openReader() throws IOException {
        return new InputStreamReader(openStream(), "UTF-8");
    }

    private InputStream openStream() throws IOException {
        return new BufferedInputStream(new FileInputStream(tmpFile));
    }

    private InputStream stream() throws IOException {
        if (base64) {
            return new Base64ReaderInputStream(reader());
        } else if (buffer != null) {
            return new ByteArrayInputStream(retrieve().getBytes("UTF-8"));
        } else if (tmpFile != null) {
            // close writer first
            writer.close();
            return openStream();
        } else {
            throw new IOException("this instance has already been disposed");
        }
    }

    /**
     * Returns a {@code Reader} for reading the serialized value.
     *
     * @return a {@code Reader} for reading the serialized value.
     * @throws IOException if an I/O error occurs
     */
    public Reader reader() throws IOException {
        if (buffer != null) {
            return new StringReader(retrieve());
        } else if (tmpFile != null) {
            // close writer first
            writer.close();
            return openReader();
        } else {
            throw new IOException("this instance has already been disposed");
        }
    }

    /**
     * Append a portion of an array of characters.
     *
     * @param chars the characters to be appended
     * @param start the index of the first character to append
     * @param len   the number of characters to append
     * @throws IOException if an I/O error occurs
     */
    public void append(char[] chars, int start, int len)
            throws IOException {
        if (buffer != null) {
            if (this.length + len > MAX_BUFFER_SIZE) {
                // threshold for keeping data in memory exceeded;
                // create temp file and spool buffer contents
                TransientFileFactory fileFactory = TransientFileFactory.getInstance();
                tmpFile = fileFactory.createTransientFile("txt", null, null);
                BufferedOutputStream fout = new BufferedOutputStream(new FileOutputStream(tmpFile));
                writer = new OutputStreamWriter(fout, "UTF-8");
                writer.write(buffer.toString());
                writer.write(chars, start, len);
                // reset the in-memory buffer
                buffer = null;
            } else {
                buffer.write(chars, start, len);
            }
        } else if (tmpFile != null) {
            writer.write(chars, start, len);
        } else {
            throw new IOException("this instance has already been disposed");
        }
        length += len;
    }

    /**
     * Close this value. Once a value has been closed,
     * further append() invocations will cause an IOException to be thrown.
     *
     * @throws IOException if an I/O error occurs
     */
    public void close() throws IOException {
        if (buffer != null) {
            // nop
        } else if (tmpFile != null) {
            writer.close();
        } else {
            throw new IOException("this instance has already been disposed");
        }
    }

    //--------------------------------------------------------< TextValue >

    @Override @SuppressWarnings("deprecation")
    public Value getValue(int targetType) throws RepositoryException {
        try {
            if (targetType == PropertyType.BINARY) {
                return valueFactory.createValue(stream());
            }

            String jcrValue = retrieveString();
            if (targetType == PropertyType.NAME) {
                jcrValue = namePathMapper.getOakName(jcrValue);
            } else if (targetType == PropertyType.PATH) {
                jcrValue = namePathMapper.getOakPath(jcrValue);
            }
            return valueFactory.createValue(jcrValue, targetType);
        } catch (IOException e) {
            throw new RepositoryException(
                    "failed to retrieve serialized value", e);
        }
    }

    @Override
    public void dispose() {
        if (buffer != null) {
            buffer = null;
        } else if (tmpFile != null) {
            try {
                writer.close();
                tmpFile.delete();
                tmpFile = null;
                writer = null;
            } catch (IOException e) {
                log.warn("Problem disposing property value", e);
            }
        } else {
            log.warn("this instance has already been disposed");
        }
    }

    /**
     * This class converts the text read Converts a base64 reader to an input stream.
     */
    private static class Base64ReaderInputStream extends InputStream {

        private static final int BUFFER_SIZE = 1024;
        private final char[] chars;
        private final ByteArrayOutputStream out;
        private final Reader reader;
        private int pos;
        private int remaining;
        private byte[] buffer;

        public Base64ReaderInputStream(Reader reader) {
            chars = new char[BUFFER_SIZE];
            this.reader = reader;
            out = new ByteArrayOutputStream(BUFFER_SIZE);
        }

        private void fillBuffer() throws IOException {
            int len = reader.read(chars, 0, BUFFER_SIZE);
            if (len < 0) {
                remaining = -1;
                return;
            }
            Base64.decode(chars, 0, len, out);
            buffer = out.toByteArray();
            pos = 0;
            remaining = buffer.length;
            out.reset();
        }

        @Override
        public int read() throws IOException {
            if (remaining == 0) {
                fillBuffer();
            }
            if (remaining < 0) {
                return -1;
            }
            remaining--;
            return buffer[pos++] & 0xff;
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }

    }

}
