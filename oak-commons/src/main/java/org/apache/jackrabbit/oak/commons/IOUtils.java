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
package org.apache.jackrabbit.oak.commons;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Locale;

/**
 * Input/output utility methods.
 */
public final class IOUtils {

    /**
     * Avoid instantiation
     */
    private IOUtils() {
    }

    /**
     * Try to read the given number of bytes to the buffer. This method reads
     * until the maximum number of bytes have been read or until the end of
     * file.
     *
     * @param in     the input stream
     * @param buffer the output buffer
     * @param off    the offset in the buffer
     * @param max    the number of bytes to read at most
     * @return the number of bytes read, 0 meaning EOF
     * @throws java.io.IOException If an error occurs.
     */
    public static int readFully(InputStream in, byte[] buffer, int off, int max) throws IOException {
        int len = Math.min(max, buffer.length);
        int result = 0;
        while (len > 0) {
            int l = in.read(buffer, off, len);
            if (l < 0) {
                break;
            }
            result += l;
            off += l;
            len -= l;
        }
        return result;
    }

    /**
     * Skip a number of bytes in an input stream.
     *
     * @param in   the input stream
     * @param skip the number of bytes to skip
     * @throws EOFException if the end of file has been reached before all bytes
     *                      could be skipped
     * @throws IOException  if an IO exception occurred while skipping
     */
    public static void skipFully(InputStream in, long skip) throws IOException {
        while (skip > 0) {
            long skipped = in.skip(skip);
            if (skipped <= 0) {
                throw new EOFException();
            }
            skip -= skipped;
        }
    }

    /**
     * Write a String. This will first write the length as 4 bytes, and then the
     * UTF-8 encoded string.
     *
     * @param out the data output stream
     * @param s   the string (maximum length about 2 GB)
     * @throws IOException if an IO exception occurred while writing
     */
    public static void writeString(OutputStream out, String s) throws IOException {
        writeBytes(out, s.getBytes("UTF-8"));
    }

    /**
     * Read a String. This will first read the length as 4 bytes, and then the
     * UTF-8 encoded string.
     *
     * @param in the data input stream
     * @return the string
     * @throws IOException if an IO exception occurred while reading
     */
    public static String readString(InputStream in) throws IOException {
        return new String(readBytes(in), "UTF-8");
    }

    /**
     * Write a byte array. This will first write the length as 4 bytes, and then
     * the actual bytes.
     *
     * @param out  the data output stream
     * @param data the byte array
     * @throws IOException if an IO exception occurred while writing.
     */
    public static void writeBytes(OutputStream out, byte[] data) throws IOException {
        writeVarInt(out, data.length);
        out.write(data);
    }

    /**
     * Read a byte array. This will first read the length as 4 bytes, and then
     * the actual bytes.
     *
     * @param in the data input stream
     * @return the bytes
     * @throws IOException if an IO exception occurred while reading from the stream.
     */
    public static byte[] readBytes(InputStream in) throws IOException {
        int len = readVarInt(in);
        byte[] data = new byte[len];
        for (int pos = 0; pos < len;) {
            int l = in.read(data, pos, data.length - pos);
            if (l < 0) {
                throw new EOFException();
            }
            pos += l;
        }
        return data;
    }

    /**
     * Write a variable size integer. Negative values need 5 bytes.
     *
     * @param out the output stream
     * @param x   the value
     * @throws IOException if an IO exception occurred while writing.
     */
    public static void writeVarInt(OutputStream out, int x) throws IOException {
        while ((x & ~0x7f) != 0) {
            out.write((x & 0x7f) | 0x80);
            x >>>= 7;
        }
        out.write(x);
    }

    /**
     * Read a variable size integer.
     *
     * @param in the input stream
     * @return the integer
     * @throws IOException if an IO exception occurred while reading.
     */
    public static int readVarInt(InputStream in) throws IOException {
        int x = (byte) in.read();
        if (x >= 0) {
            return x;
        }
        x &= 0x7f;
        for (int s = 7;; s += 7) {
            int b = in.read();
            if (b < 0) {
                throw new EOFException();
            }
            b = (byte) b;
            x |= (b & 0x7f) << s;
            if (b >= 0) {
                return x;
            }
        }
    }

    /**
     * Write a variable size long.
     * Negative values need 10 bytes.
     *
     * @param out the output stream
     * @param x   the value
     * @throws IOException if an IO exception occurred while writing.
     */
    public static void writeVarLong(OutputStream out, long x) throws IOException {
        while ((x & ~0x7f) != 0) {
            out.write((byte) ((x & 0x7f) | 0x80));
            x >>>= 7;
        }
        out.write((byte) x);
    }

    /**
     * Write a long (8 bytes).
     *
     * @param out the output stream
     * @param x   the value
     * @throws IOException if an IO exception occurred while writing.
     */
    public static void writeLong(OutputStream out, long x) throws IOException {
        writeInt(out, (int) (x >>> 32));
        writeInt(out, (int) x);
    }

    /**
     * Read a long (8 bytes).
     *
     * @param in the input stream
     * @return the value
     * @throws IOException if an IO exception occurred while reading.
     */
    public static long readLong(InputStream in) throws IOException {
        return ((long) (readInt(in)) << 32) + (readInt(in) & 0xffffffffL);
    }

    /**
     * Write an integer (4 bytes).
     *
     * @param out the output stream
     * @param x   the value
     * @throws IOException if an IO exception occurred while writing.
     */
    public static void writeInt(OutputStream out, int x) throws IOException {
        out.write((byte) (x >> 24));
        out.write((byte) (x >> 16));
        out.write((byte) (x >> 8));
        out.write((byte) x);
    }

    /**
     * Read an integer (4 bytes).
     *
     * @param in the input stream
     * @return the value
     * @throws IOException if an IO exception occurred while reading.
     */
    public static int readInt(InputStream in) throws IOException {
        return ((in.read() & 0xff) << 24) +
                ((in.read() & 0xff) << 16) +
                ((in.read() & 0xff) << 8) +
                (in.read() & 0xff);
    }

    /**
     * Read a variable size long.
     *
     * @param in the input stream
     * @return the long
     * @throws IOException if an IO exception occurred while reading.
     */
    public static long readVarLong(InputStream in) throws IOException {
        long x = (byte) in.read();
        if (x >= 0) {
            return x;
        }
        x &= 0x7f;
        for (int s = 7;; s += 7) {
            long b = in.read();
            if (b < 0) {
                throw new EOFException();
            }
            b = (byte) b;
            x |= (b & 0x7f) << s;
            if (b >= 0) {
                return x;
            }
        }
    }

    /**
     * Get the value that is equal or higher than this value, and that is a
     * power of two.  The returned value will be in the range [0, 2^31].
     * If the input is less than zero, the result of 1 is returned (powers of
     * negative numbers are not integer values).
     *
     * @param x the original value.
     * @return the next power of two value.  Results are always in the
     * range [0, 2^31].
     */
    public static long nextPowerOf2(int x) {
        long i = 1;
        while (i < x) {
            i += i;
        }
        return i;
    }

    /**
     * Unconditionally close a {@code Closeable}.
     * <p>
     * Equivalent to {@link Closeable#close()}, except any exceptions will be ignored.
     * This is typically used in finally blocks.
     *
     * @param closeable the object to close, may be null or already closed
     */
    public static void closeQuietly(Closeable closeable) {
        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (IOException ioe) {
            // ignore
        }
    }

    /**
     * Unconditionally close a {@code Socket}.
     * <p>
     * Equivalent to {@link Socket#close()}, except any exceptions will be ignored.
     * This is typically used in finally blocks.
     *
     * @param sock the Socket to close, may be null or already closed
     */
    public static void closeQuietly(Socket sock) {
        if (sock != null) {
            try {
                sock.close();
            } catch (IOException ioe) {
                // ignored
            }
        }
    }

    /**
     * Copy bytes from an {@code InputStream} to an
     * {@code OutputStream}.
     * <p>
     * This method buffers the input internally, so there is no need to use a
     * {@code BufferedInputStream}.
     *
     * @param input  the {@code InputStream} to read from
     * @param output the {@code OutputStream} to write to
     * @return the number of bytes copied
     * @throws IOException if an I/O error occurs
     */
    public static long copy(InputStream input, OutputStream output)
            throws IOException {
        byte[] buffer = new byte[4096];
        long count = 0;
        int n = 0;
        while (-1 != (n = input.read(buffer))) {
            output.write(buffer, 0, n);
            count += n;
        }
        return count;
    }

    /**
     * Returns a human-readable version of the file size, where the input represents
     * a specific number of bytes. Based on http://stackoverflow.com/a/3758880/1035417
     */
    public static String humanReadableByteCount(long bytes) {
        if (bytes < 0) {
            return "0";
        }
        int unit = 1000;
        if (bytes < unit) {
            return bytes + " B";
        }
        int exp = (int) (Math.log(bytes) / Math.log(unit));
        char pre = "kMGTPE".charAt(exp - 1);
        return String.format(Locale.ENGLISH, "%.1f %sB", bytes / Math.pow(unit, exp), pre);
    }
}
