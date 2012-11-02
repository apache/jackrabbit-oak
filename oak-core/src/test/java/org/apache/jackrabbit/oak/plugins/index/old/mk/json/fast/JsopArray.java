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
package org.apache.jackrabbit.oak.plugins.index.old.mk.json.fast;

import org.apache.jackrabbit.mk.json.JsopBuilder;
import org.apache.jackrabbit.mk.json.JsopTokenizer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

/**
 * An array of objects.
 */
public class JsopArray extends Jsop implements List<Object> {

    private static final ArrayList<String> EMPTY_LIST = new ArrayList<String>();

    private ArrayList<String> list;
    private JsopTokenizer tokenizer;

    public JsopArray(String jsop, int start, int end) {
        super(jsop, start, end);
    }

    public JsopArray() {
        super(null, 0, 0);
        list = new ArrayList<String>();
    }

    @Override
    public Object get(int index) {
        init();
        String s = load(index);
        if (s == null) {
            throw new IndexOutOfBoundsException(index + " (size: " + list.size() + ")");
        }
        return Jsop.parse(s);
    }

    private String load(int index) {
        if (index < list.size()) {
            return list.get(index);
        }
        while (tokenizer != null) {
            String v = tokenizer.readRawValue();
            list.add(v);
            if (tokenizer.matches(']')) {
                tokenizer = null;
            } else {
                tokenizer.read(',');
            }
            if (index < list.size()) {
                return v;
            }
        }
        return null;
    }

    private void init() {
        if (list != null) {
            return;
        }
        tokenizer = new JsopTokenizer(jsop, start);
        tokenizer.read('[');
        if (tokenizer.matches(']')) {
            list = EMPTY_LIST;
            tokenizer = null;
        } else {
            list = new ArrayList<String>();
        }
    }

    @Override
    public boolean isEmpty() {
        init();
        return list == EMPTY_LIST;
    }

    @Override
    public int size() {
        init();
        load(Integer.MAX_VALUE);
        return list.size();
    }

    @Override
    public String toString() {
        if (jsop == null) {
            JsopBuilder w = new JsopBuilder();
            w.array();
            for (String e : list) {
                w.encodedValue(e);
            }
            w.endArray();
            jsop = w.toString();
            start = 0;
        }
        return jsop.substring(start);
    }

    @Override
    public boolean add(Object e) {
        initWrite();
        list.add(toString(e));
        return true;
    }

    @Override
    public void clear() {
        initWrite();
        list.clear();
    }

    private void initWrite() {
        readAll();
        jsop = null;
    }

    private void readAll() {
        load(Integer.MAX_VALUE);
    }

    @Override
    public void add(int index, Object element) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(int index, Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean contains(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int indexOf(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<Object> iterator() {
        return new Iterator<Object>() {

            int index;

            @Override
            public boolean hasNext() {
                return index < size();
            }

            @Override
            public Object next() {
                return get(index++);
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

        };
    }

    @Override
    public int lastIndexOf(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListIterator<Object> listIterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListIterator<Object> listIterator(int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object remove(int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object set(int index, Object element) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Object> subList(int fromIndex, int toIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        throw new UnsupportedOperationException();
    }

}
