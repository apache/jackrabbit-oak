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
package org.apache.jackrabbit.oak.plugins.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Result.SizePrecision;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.IndexRow;
import org.junit.Test;

/**
 * Tests the cursors implementations.
 */
public class CursorsTest {

    @Test
    public void intersectionCursor() {
        QueryEngineSettings s = new QueryEngineSettings();
        Cursor a = new SimpleCursor("1:", "/b", "/c", "/e", "/e", "/c");
        Cursor b = new SimpleCursor("2:", "/a", "/c", "/d", "/b", "/c");
        Cursor c = Cursors.newIntersectionCursor(a, b, s);
        assertEquals("1:/b, 1:/c", list(c));
        assertFalse(c.hasNext());
    }

    @Test
    public void intersectionCursorExceptions() {
        QueryEngineSettings s = new QueryEngineSettings();
        Cursor a = new SimpleCursor("1:", "/x", "/b", "/c", "/e", "/e", "/c");
        Cursor b = new SimpleCursor("2:", "/a", "/c", "/d", "/b", "/c");
        Cursor c = Cursors.newIntersectionCursor(a, b, s);
        c.next();
        c.next();
        try {
            c.remove();
            fail();
        } catch (UnsupportedOperationException e) {
            // expected
        }
        try {
            c.next();
            fail();
        } catch (IllegalStateException e) {
            // expected
        }
    }

    static String list(Cursor c) {
        StringBuilder buff = new StringBuilder();
        while (c.hasNext()) {
            buff.append(buff.length() == 0 ? "" : ", ");
            buff.append(c.next());
        }
        return buff.toString();
    }
    
    static class SimpleCursor implements Cursor {
        
        final Iterator<IndexRow> rows;
        
        SimpleCursor(String idPrefix, String... paths) {
            ArrayList<IndexRow> list = new ArrayList<IndexRow>();
            for (String p : paths) {
                list.add(new SimpleIndexRow(p, idPrefix + p));
            }
            rows = list.iterator();
        }

        @Override
        public boolean hasNext() {
            return rows.hasNext();
        }

        @Override
        public void remove() {
            rows.remove();
        }

        @Override
        public IndexRow next() {
            return rows.next();
        }

        @Override
        public long getSize(SizePrecision precision, long max) {
            return -1;
        }
        
    }
    
    static class SimpleIndexRow implements IndexRow {
        
        final String path;
        final String id;
        
        SimpleIndexRow(String path, String id) {
            this.path = path;
            this.id = id;
        }

        @Override
        public boolean isVirtualRow() {
            return false;
        }

        @Override
        public String getPath() {
            return path;
        }

        @Override
        public PropertyValue getValue(String columnName) {
            return null;
        }
        
        @Override
        public String toString() {
            return id;
        }
        
    }
}
