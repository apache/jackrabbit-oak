/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.commons.pio;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;

/**
 * Convenience utility to close a list of {@link Closeable}s.
 * <p>
 * Inspired by and replacing Guava's Closer.
 */
public class Closer implements Closeable {

    private Closer() {
        // no instances for you
    }

    // stack of closeables to close
    private final Deque<Closeable> closeables = new ArrayDeque<>();

    // set by rethrow method
    private Throwable rethrow = null;

    /**
     * Create instance of Closer.
     */
    public static Closer create() {
        return new Closer();
    }

    /**
     * Add a {@link Closeable} to the list.
     * @param closeable {@link Closeable} object to be added
     * @return the closeable param
     */
    public @Nullable <C extends Closeable> C register(@Nullable C closeable) {
        if (closeable != null) {
            closeables.add(closeable);
        }
        return closeable;
    }

    /**
     * Closes the set of {@link Closeable}s in reverse order.
     * <p>
     * Swallows all {@link IOException}s except the first that
     * was thrown.
     * <p>
     * If {@link #rethrow} was called before, throw <em>that</em>
     * exception instead (wrapped into a {@link RuntimeException}
     * when necessary).
     */
    public void close() throws IOException {
        // keep track of the IOException to throw
        IOException toThrow = null;

        // close all in reverse order
        while (!closeables.isEmpty()) {
            Closeable closeable = closeables.removeLast();
            try {
                closeable.close();
            } catch (IOException exception) {
                // remember the first one that occurred
                if (toThrow == null) {
                    toThrow = exception;
                }
            }
        }

        // consider exceptions passed to rethrow()
        if (rethrow instanceof IOException) {
            throw (IOException) rethrow;
        } else if (rethrow instanceof RuntimeException) {
            throw (RuntimeException) rethrow;
        } else if (rethrow != null) {
            throw new RuntimeException(rethrow);
        }

        // otherwise throw the IOException we selected
        if (toThrow != null) {
            throw toThrow;
        }
    }

    /**
     * Stores a {@link Throwable} for later use in {@link #close()} and
     * rethrows it (potentially wrapped into {@link RuntimeException} or
     * {@link Error}).
     * <p>
     * {@link #close()} will use the exception passed in the last call of this
     * method.
     * @return never returns
     * @throws IOException wrapping the input, when needed
     */
    public RuntimeException rethrow(@NotNull Throwable throwable) throws IOException {
        rethrow = Objects.requireNonNull(throwable);
        if (throwable instanceof IOException) {
            throw (IOException) throwable;
        } else if (throwable instanceof RuntimeException) {
            throw (RuntimeException) throwable;
        } else if (throwable instanceof Error) {
            throw (Error) throwable;
        } else {
            throw new RuntimeException(throwable);
        }
    }
}
