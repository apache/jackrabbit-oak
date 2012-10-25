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

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * A map.
 *
 * This implementation supports an options 'lengthIndex' to speed up
 * sparse parsing of an object (only the requested values are parsed). The
 * speedup depends on the use case.
 */
public class JsopObject extends Jsop implements Map<String, Object> {

    private static final String LENGTHS_KEY = ":lengths:";
    private static final HashMap<String, String> EMPTY_MAP = new HashMap<String, String>();

    private HashMap<String, String> map;
    private JsopTokenizer tokenizer;
    private boolean lengthIndex;
    private int[] length;

    JsopObject(String jsop, int start, int end) {
        super(jsop, start, end);
    }

    public JsopObject() {
        super(null, 0, 0);
        map = new LinkedHashMap<String, String>();
    }

    private void init() {
        if (map != null) {
            return;
        }
        tokenizer = new JsopTokenizer(jsop, start);
        tokenizer.read('{');
        if (tokenizer.matches('}')) {
            map = EMPTY_MAP;
            tokenizer = null;
        } else {
            map = new LinkedHashMap<String, String>();
        }
    }

    @Override
    public Object get(Object key) {
        init();
        String v = map.get(key);
        if (tokenizer == null) {
            return v == null ? null : Jsop.parse(v);
        }
        if (v == null) {
            while (tokenizer != null) {
                String k = tokenizer.readString();
                if (lengthIndex) {
                    int len = length[map.size()];
                    int p = tokenizer.getPos();
                    v = jsop.substring(p, p + len);
                    tokenizer.setPos(p + len);
                    tokenizer.read();
                } else {
                    tokenizer.read(':');
                    v = tokenizer.readRawValue();
                }
                if (LENGTHS_KEY.equals(k)) {
                    v = JsopTokenizer.decodeQuoted(v);
                    String[] lengthsStrings = v.split(",");
                    length = new int[lengthsStrings.length];
                    for (int i = 0; i < lengthsStrings.length; i++) {
                        length[i] = Integer.parseInt(lengthsStrings[i]);
                    }
                    lengthIndex = true;
                } else {
                    map.put(k, v);
                }
                if (tokenizer.matches('}')) {
                    tokenizer = null;
                } else {
                    tokenizer.read(',');
                }
                if (k.equals(key)) {
                    return Jsop.parse(v);
                }
            }
        }
        return null;
    }

    @Override
    public boolean containsKey(Object key) {
        get(key);
        return map.containsKey(key);
    }

    @Override
    public boolean isEmpty() {
        init();
        return map == EMPTY_MAP;
    }

    @Override
    public int size() {
        readAll();
        return map.size();
    }

    private void readAll() {
        get(null);
    }

    private void initWrite() {
        readAll();
        jsop = null;
    }

    @Override
    public String toString() {
        if (jsop == null) {
            JsopBuilder w = new JsopBuilder();
            w.object();
            if (lengthIndex) {
                if (map.containsKey(LENGTHS_KEY)) {
                    throw new IllegalStateException("Object already contains the key " + LENGTHS_KEY);
                }
                StringBuilder buff = new StringBuilder();
                for (Entry<String, String> e : map.entrySet()) {
                    if (buff.length() > 0) {
                        buff.append(',');
                    }
                    buff.append(e.getValue().length());
                }
                w.key(LENGTHS_KEY).value(buff.toString());
            }
            for (Entry<String, String> e : map.entrySet()) {
                w.key(e.getKey()).encodedValue(e.getValue());
            }
            w.endObject();
            jsop = w.toString();
            start = 0;
        }
        return jsop.substring(start);
    }

    @Override
    public void clear() {
        initWrite();
        map.clear();
    }

    @Override
    public Object put(String key, Object value) {
        initWrite();
        String old = map.put(key, toString(value));
        return Jsop.parse(old);
    }

    @Override
    public boolean containsValue(Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Entry<String, Object>> entrySet() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> keySet() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(Map<? extends String, ?> m) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object remove(Object key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<Object> values() {
        throw new UnsupportedOperationException();
    }

    public void setLengthIndex(boolean lengthIndex) {
        this.lengthIndex = lengthIndex;
    }

}
