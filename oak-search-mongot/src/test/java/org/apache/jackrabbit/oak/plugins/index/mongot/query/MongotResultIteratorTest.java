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

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex.FulltextResultRow;
import org.bson.Document;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class MongotResultIteratorTest {

    @Test
    public void readsOnlyAsFarAsTheConsumerAndClosesOnExhaustion() {
        AtomicInteger mapped = new AtomicInteger();
        AtomicBoolean closed = new AtomicBoolean();
        MongotResultIterator iterator = new MongotResultIterator(
                List.of(new Document("path", "/a"), new Document("path", "/skip"),
                        new Document("path", "/b")).iterator(),
                document -> {
                    mapped.incrementAndGet();
                    String path = document.getString("path");
                    return "/skip".equals(path) ? null : new FulltextResultRow(
                            path, 1, null, null, null);
                }, () -> closed.set(true));

        assertTrue(iterator.hasNext());
        assertEquals(1, mapped.get());
        assertEquals("/a", iterator.next().path);
        assertTrue(iterator.hasNext());
        assertEquals(3, mapped.get());
        assertEquals("/b", iterator.next().path);
        assertFalse(iterator.hasNext());
        assertTrue(closed.get());
    }

    @Test
    public void exactSizeMaterializesRemainingRowsWithoutLosingThem() {
        AtomicBoolean closed = new AtomicBoolean();
        MongotResultIterator iterator = new MongotResultIterator(
                List.of(new Document("path", "/a"), new Document("path", "/b"),
                        new Document("path", "/c")).iterator(),
                document -> new FulltextResultRow(document.getString("path"),
                        1, null, null, null), () -> closed.set(true));

        assertEquals("/a", iterator.next().path);
        assertEquals(3, iterator.getSize());
        assertTrue(closed.get());
        assertEquals("/b", iterator.next().path);
        assertEquals("/c", iterator.next().path);
        assertFalse(iterator.hasNext());
    }

    @Test
    public void closesTheSourceWhenRowAdaptationFails() {
        AtomicBoolean closed = new AtomicBoolean();
        MongotResultIterator iterator = new MongotResultIterator(
                List.of(new Document("path", "/a")).iterator(),
                document -> {
                    throw new IllegalStateException("cannot adapt");
                }, () -> closed.set(true));

        assertThrows(IllegalStateException.class, iterator::hasNext);
        assertTrue(closed.get());
    }
}
