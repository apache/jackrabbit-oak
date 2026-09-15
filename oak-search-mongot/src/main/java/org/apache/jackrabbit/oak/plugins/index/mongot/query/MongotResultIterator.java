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
package org.apache.jackrabbit.oak.plugins.index.mongot.query;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.function.Function;

import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex.FulltextResultRow;
import org.bson.Document;

final class MongotResultIterator implements Iterator<FulltextResultRow> {

    private final Iterator<Document> source;
    private final Function<Document, FulltextResultRow> mapper;
    private final Runnable close;
    private final Queue<FulltextResultRow> buffered = new ArrayDeque<>();
    private long resultCount;
    private boolean exhausted;
    private boolean closed;

    MongotResultIterator(Iterator<Document> source,
                        Function<Document, FulltextResultRow> mapper,
                        Runnable close) {
        this.source = source;
        this.mapper = mapper;
        this.close = close;
    }

    @Override
    public boolean hasNext() {
        fillOne();
        return !buffered.isEmpty();
    }

    @Override
    public FulltextResultRow next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return buffered.remove();
    }

    long getSize() {
        try {
            if (!exhausted) {
                while (source.hasNext()) {
                    FulltextResultRow row = mapper.apply(source.next());
                    if (row != null) {
                        buffered.add(row);
                        resultCount++;
                    }
                }
                exhausted = true;
                closeSource();
            }
            return resultCount;
        } catch (RuntimeException e) {
            closeSource();
            throw e;
        }
    }

    private void fillOne() {
        try {
            while (buffered.isEmpty() && !exhausted) {
                if (!source.hasNext()) {
                    exhausted = true;
                    closeSource();
                    return;
                }
                FulltextResultRow row = mapper.apply(source.next());
                if (row != null) {
                    buffered.add(row);
                    resultCount++;
                }
            }
        } catch (RuntimeException e) {
            closeSource();
            throw e;
        }
    }

    private void closeSource() {
        if (!closed) {
            closed = true;
            close.run();
        }
    }
}
