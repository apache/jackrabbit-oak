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
package org.apache.jackrabbit.oak.query;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.jackrabbit.mk.json.JsopTokenizer;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.CoreValueFactory;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;

/**
 * Utility class for working with jsop string diffs
 * 
 */
public class JsopUtil {

    private JsopUtil() {

    }

    /**
     * Applies the commit string to a given Root instance
     * 
     * 
     * The commit string represents a sequence of operations, jsonp style:
     * 
     * <p>
     * / + "test": { "a": { "id": "ref:123" }, "b": { "id" : "str:123" }}
     * <p>
     * or
     * <p>
     * "/ - "test"
     * </p>
     * 
     * @param root
     * @param commit
     *            - the commit string
     */
    public static void apply(Root root, String commit, CoreValueFactory vf)
            throws Exception {
        int index = commit.indexOf(' ');
        String path = commit.substring(0, index).trim();
        commit = commit.substring(index).trim();

        index = commit.indexOf(' ');
        String op = commit.substring(0, index).trim();
        commit = commit.substring(index).trim();

        if (!"+".equals(op) && !"-".equals(op)) {
            throw new Exception("Unknown operation " + op
                    + ". This should be either '+' or '-'.");
        }
        if (commit.length() == 0) {
            return;
        }
        boolean isAdd = "+".equals(op);
        Tree c = root.getTree(path);
        if (c == null) {
            // TODO create intermediary?
            throw new Exception("non existing path " + path);
        }
        if (isAdd) {
            addTree(c, commit, vf);
        } else {
            removeTree(c, commit);
        }
    }

    private static void removeTree(Tree t, String pathIn) {
        String path = new JsopTokenizer(pathIn).readString();

        Iterator<String> pathIterator = PathUtils.elements(path).iterator();
        while (pathIterator.hasNext()) {
            String p = pathIterator.next();
            if (!t.hasChild(p)) {
                return;
            }
            t = t.getChild(p);
        }
        t.remove();
    }

    private static void addTree(Tree t, String pathIn, CoreValueFactory vf)
            throws Exception {
        addTree(t, new JsopTokenizer(pathIn), vf);
    }

    private static void addTree(Tree t, JsopTokenizer tokenizer,
            CoreValueFactory vf) throws Exception {
        if (!tokenizer.matches('}')) {
            do {
                String key = tokenizer.readString();
                tokenizer.read(':');
                if (tokenizer.matches('{')) {
                    addTree(t.addChild(key), tokenizer, vf);
                } else {
                    String val = tokenizer.readRawValue();
                    if (val.startsWith("[")) {
                        val = val.substring(val.indexOf('[') + 1,
                                val.indexOf(']'));
                        List<CoreValue> mvp = new ArrayList<CoreValue>();
                        for (String v : val.split(",")) {
                            mvp.add(vf.createValue(JsopTokenizer.decodeQuoted(v
                                    .trim())));
                        }
                        t.setProperty(key, mvp);
                    } else {
                        t.setProperty(key, vf.createValue(JsopTokenizer
                                .decodeQuoted(val.trim())));
                    }
                }
            } while (tokenizer.matches(','));
            if (tokenizer.getPos() != tokenizer.getLastPos()) {
                tokenizer.read('}');
            }
        }
    }
}
