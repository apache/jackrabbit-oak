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

import org.apache.jackrabbit.mk.json.JsopTokenizer;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;

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
     * @param commit the commit string
     * @throws UnsupportedOperationException if the operation is not supported
     */
    public static void apply(Root root, String commit)
            throws UnsupportedOperationException {
        int index = commit.indexOf(' ');
        String path = commit.substring(0, index).trim();
        Tree c = root.getTree(path);
        if (c == null) {
            // TODO create intermediary?
            throw new UnsupportedOperationException("Non existing path " + path);
        }
        commit = commit.substring(index);
        JsopTokenizer tokenizer = new JsopTokenizer(commit);
        if (tokenizer.matches('-')) {
            removeTree(c, tokenizer);
        } else if (tokenizer.matches('+')) {
            addTree(c, tokenizer);
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported " + (char) tokenizer.read() + 
                    ". This should be either '+' or '-'.");
        }
    }

    private static void removeTree(Tree t, JsopTokenizer tokenizer) {
        String path = tokenizer.readString();
        for (String p : PathUtils.elements(path)) {
            if (!t.hasChild(p)) {
                return;
            }
            t = t.getChild(p);
        }
        t.remove();
    }

    private static void addTree(Tree t, JsopTokenizer tokenizer) {
        do {
            String key = tokenizer.readString();
            tokenizer.read(':');
            if (tokenizer.matches('{')) {
                Tree c = t.addChild(key);
                if (!tokenizer.matches('}')) {
                    addTree(c, tokenizer);
                    tokenizer.read('}');
                }
            } else if (tokenizer.matches('[')) {
                t.setProperty(PropertyStates.readArrayProperty(key, tokenizer, null));
            } else {
                t.setProperty(PropertyStates.readProperty(key, tokenizer, null));
            }
        } while (tokenizer.matches(','));
    }
}
