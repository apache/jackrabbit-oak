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

package org.apache.jackrabbit.oak.commons.io;

import static org.apache.commons.io.FileUtils.forceDelete;
import static org.apache.jackrabbit.oak.commons.IOUtils.closeQuietly;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.function.Function;

import org.apache.commons.io.LineIterator;
import org.apache.jackrabbit.oak.commons.FileIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.AbstractIterator;

/**
 * Implements a {@link java.io.Closeable} wrapper over a {@link LineIterator}.
 * Also has a transformer to transform the output. If the underlying file is
 * provided then it deletes the file on {@link #close()}.
 *
 * If there is a scope for lines in the file containing line break characters it
 * should be ensured that the files is written with
 * {@link FileIOUtils#writeAsLine(BufferedWriter, String, boolean)} with true to escape
 * line break characters and should be properly unescaped on read. A custom
 * transformer can also be provided to unescape.
 *
 * @param <T>
 *            the type of elements in the iterator
 */
public class BurnOnCloseFileIterator<T> implements Closeable, Iterator<T> {

    private static final Logger LOG = LoggerFactory.getLogger(BurnOnCloseFileIterator.class);

    private final Impl<T> delegate;

    public BurnOnCloseFileIterator(Iterator<String> iterator, Function<String, T> transformer) {
        this.delegate = new Impl<T>(iterator, null, transformer);
    }

    public BurnOnCloseFileIterator(Iterator<String> iterator, File backingFile, Function<String, T> transformer) {
        this.delegate = new Impl<T>(iterator, backingFile, transformer);
    }

    @Override
    public boolean hasNext() {
        return this.delegate.hasNext();
    }

    @Override
    public T next() {
        return this.delegate.next();
    }

    @Override
    public void close() throws IOException {
        this.delegate.close();
    }

    public static BurnOnCloseFileIterator<String> wrap(Iterator<String> iter) {
        return new BurnOnCloseFileIterator<String>(iter, new Function<String, String>() {
            public String apply(String s) {
                return s;
            }
        });
    }

    public static BurnOnCloseFileIterator<String> wrap(Iterator<String> iter, File backingFile) {
        return new BurnOnCloseFileIterator<String>(iter, backingFile, new Function<String, String>() {
            public String apply(String s) {
                return s;
            }
        });
    }

    private static class Impl<T> extends AbstractIterator<T> implements Closeable {
        private final Iterator<String> iterator;
        private final Function<String, T> transformer;
        private final File backingFile;

        public Impl(Iterator<String> iterator, File backingFile, Function<String, T> transformer) {
            this.iterator = iterator;
            this.transformer = transformer;
            this.backingFile = backingFile;
        }

        @Override
        protected T computeNext() {
            if (iterator.hasNext()) {
                return transformer.apply(iterator.next());
            }

            try {
                close();
            } catch (IOException e) {
                LOG.warn("Error closing iterator", e);
            }
            return endOfData();
        }

        @Override
        public void close() throws IOException {
            if (iterator instanceof Closeable) {
                closeQuietly((Closeable) iterator);
            }
            if (backingFile != null && backingFile.exists()) {
                forceDelete(backingFile);
            }
        }
    }
}
