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

package org.apache.jackrabbit.oak.plugins.document.util;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

import com.google.common.io.Closer;

import org.jetbrains.annotations.NotNull;

public class CloseableIterable<T> implements Iterable<T>, Closeable {
    private final Iterable<T> iterable;
    private final Closer closer = Closer.create();

    public static <T> CloseableIterable<T> wrap(Iterable<T> iterable, Closeable closeable) {
        return new CloseableIterable<T>(iterable, closeable);
    }

    public static <T> CloseableIterable<T> wrap(Iterable<T> iterable) {
        return new CloseableIterable<T>(iterable, null);
    }

    public CloseableIterable(Iterable<T> iterable, Closeable closeable) {
        this.iterable = iterable;
        if (closeable != null) {
            this.closer.register(closeable);
        }
    }

    @Override
    public void close() throws IOException {
        closer.close();
    }

    @NotNull
    @Override
    public Iterator<T> iterator() {
        Iterator<T> it = iterable.iterator();
        if (it instanceof Closeable) {
            closer.register((Closeable) it);
        }
        return it;
    }
}
