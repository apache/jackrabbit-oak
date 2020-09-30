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

package org.apache.jackrabbit.oak.commons.io;

import static com.google.common.base.Charsets.UTF_8;
import static org.apache.jackrabbit.oak.commons.IOUtils.closeQuietly;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.function.Function;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.jackrabbit.oak.commons.FileIOUtils;
import org.jetbrains.annotations.Nullable;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

/**
 * FileLineDifferenceIterator class which iterates over the difference of 2 files line by line.
 *
 * If there is a scope for lines in the files containing line break characters it should be
 * ensured that both the files are written with
 * {@link FileIOUtils#writeAsLine(BufferedWriter, String, boolean)} with true to escape line break
 * characters.
 */
public class FileLineDifferenceIterator implements Closeable, Iterator<String> {

    private final Impl delegate;

    public FileLineDifferenceIterator(LineIterator marked, LineIterator available, @Nullable Function<String, String> transformer)
            throws IOException {
        this.delegate = new Impl(marked, available, transformer);
    }

    public FileLineDifferenceIterator(File marked, File available, @Nullable Function<String, String> transformer)
            throws IOException {
        this(FileUtils.lineIterator(marked, UTF_8.toString()), FileUtils.lineIterator(available, UTF_8.toString()), transformer);
    }

    public FileLineDifferenceIterator(LineIterator marked, LineIterator available) throws IOException {
        this(marked, available, null);
    }

    @Override
    public boolean hasNext() {
        return this.delegate.hasNext();
    }

    @Override
    public String next() {
        return this.delegate.next();
    }

    @Override
    public void close() throws IOException {
        this.delegate.close();
    }

    private static class Impl extends AbstractIterator<String> implements Closeable {

        private final PeekingIterator<String> peekMarked;
        private final LineIterator marked;
        private final LineIterator all;

        private Function<String, String> transformer = new Function<String, String>() {
            @Override
            public String apply(String input) {
                return input;
            }
        };

        public Impl(LineIterator marked, LineIterator available, @Nullable Function<String, String> transformer)
                throws IOException {
            this.marked = marked;
            this.peekMarked = Iterators.peekingIterator(marked);
            this.all = available;
            if (transformer != null) {
                this.transformer = transformer;
            }
        }

        @Override
        protected String computeNext() {
            String diff = computeNextDiff();
            if (diff == null) {
                close();
                return endOfData();
            }
            return diff;
        }

        @Override
        public void close() {
            if (marked instanceof Closeable) {
                closeQuietly(marked);
            }
            if (all instanceof Closeable) {
                closeQuietly(all);
            }
        }

        private String computeNextDiff() {
            if (!all.hasNext()) {
                return null;
            }

            // Marked finish the rest of all are part of diff
            if (!peekMarked.hasNext()) {
                return all.next();
            }

            String diff = null;
            while (all.hasNext() && diff == null) {
                diff = all.next();
                while (peekMarked.hasNext()) {
                    String marked = peekMarked.peek();
                    int comparisonResult = transformer.apply(diff).compareTo(transformer.apply((marked)));
                    if (comparisonResult > 0) {
                        // Extra entries in marked. Ignore them and move on
                        peekMarked.next();
                    } else if (comparisonResult == 0) {
                        // Matching entry found in marked move past it. Not a
                        // dif candidate
                        peekMarked.next();
                        diff = null;
                        break;
                    } else {
                        // This entry is not found in marked entries
                        // hence part of diff
                        return diff;
                    }
                }
            }
            return diff;
        }
    }
}
