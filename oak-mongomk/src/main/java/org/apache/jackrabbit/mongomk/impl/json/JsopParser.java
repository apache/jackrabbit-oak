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
package org.apache.jackrabbit.mongomk.impl.json;

import javax.xml.parsers.SAXParser;

import org.apache.jackrabbit.mk.json.JsopReader;
import org.apache.jackrabbit.mk.json.JsopTokenizer;
import org.apache.jackrabbit.oak.commons.PathUtils;

/**
 * An event based parser for <a href="http://wiki.apache.org/jackrabbit/Jsop">JSOP</a>.
 *
 * <p>
 * This parser is similar to a {@link SAXParser} using a callback ({@code DefaultHandler}) to inform about certain
 * events during parsing,i.e. node was added, node was removed, etc. This relieves the implementor from the burden of
 * performing a semantic analysis of token which are being parsed.
 * </p>
 *
 * <p>
 * The underlying token parser is the {@link JsopTokenizer}.
 * </p>
 */
public class JsopParser {

    private final DefaultJsopHandler defaultHandler;
    private final String path;
    private final JsopTokenizer tokenizer;

    /**
     * Constructs a new {@link JsopParser}
     *
     * @param path The root path of the JSON diff.
     * @param jsonDiff The JSON diff.
     * @param defaultHandler The {@link DefaultJsopHandler} to use.
     */
    public JsopParser(String path, String jsonDiff, DefaultJsopHandler defaultHandler) {
        this.path = path;
        this.defaultHandler = defaultHandler;
        tokenizer = new JsopTokenizer(jsonDiff);
    }

    /**
     * Parses the JSON diff.
     *
     * @throws Exception If an error occurred while parsing.
     */
    public void parse() throws Exception {
        if (path.length() > 0 && !PathUtils.isAbsolute(path)) {
            throw new IllegalArgumentException("Absolute path expected: " + path);
        }

        while (true) {
            int token = tokenizer.read();

            if (token == JsopReader.END) {
                break;
            }

            switch (token) {
                case '+': {
                    parseOpAdded(path);
                    break;
                }
                case '*': {
                    parseOpCopied();
                    break;
                }
                case '>': {
                    parseOpMoved();
                    break;
                }
                case '^': {
                    parseOpSet();
                    break;
                }
                case '-': {
                    parseOpRemoved();
                    break;
                }
                default:
                    throw new IllegalStateException("Unknown operation: " + (char) token);
            }
        }
    }

    private void parseOpAdded(String currentPath) throws Exception {
        String subPath = tokenizer.readString();
        tokenizer.read(':');
        String path = PathUtils.concat(currentPath, subPath);

        if (tokenizer.matches('{')) {
            String parentPath = PathUtils.denotesRoot(path) ? "" : PathUtils.getParentPath(path);
            String nodeName = PathUtils.denotesRoot(path) ? "/" : PathUtils.getName(path);
            defaultHandler.nodeAdded(parentPath, nodeName);

            if (!tokenizer.matches('}')) {
                do {
                    int pos = tokenizer.getLastPos();
                    String propName = tokenizer.readString();
                    tokenizer.read(':');

                    if (tokenizer.matches('{')) { // Nested node.
                        // Reset to last pos as parseOpAdded expected the whole JSON.
                        tokenizer.setPos(pos);
                        tokenizer.read();
                        parseOpAdded(path);
                    }
                    else { // Property.
                        String valueAsString = tokenizer.readRawValue().trim();
                        Object value = JsonUtil.toJsonValue(valueAsString);
                        defaultHandler.propertySet(path, propName, value);
                    }
                } while (tokenizer.matches(','));

                tokenizer.read('}'); // explicitly close the bracket
            }
        }
    }

    private void parseOpCopied() throws Exception {
        int pos = tokenizer.getLastPos();
        String subPath = tokenizer.readString();
        String srcPath = PathUtils.concat(path, subPath);
        if (!PathUtils.isAbsolute(srcPath)) {
            throw new Exception("Absolute path expected: " + srcPath + ", pos: " + pos);
        }
        tokenizer.read(':');
        String targetPath = tokenizer.readString();
        if (!PathUtils.isAbsolute(targetPath)) {
            targetPath = PathUtils.concat(path, targetPath);
            if (!PathUtils.isAbsolute(targetPath)) {
                throw new Exception("Absolute path expected: " + targetPath + ", pos: " + pos);
            }
        }
        defaultHandler.nodeCopied(path, srcPath, targetPath);
    }

    private void parseOpMoved() throws Exception {
        int pos = tokenizer.getLastPos();
        String subPath = tokenizer.readString();
        String srcPath = PathUtils.concat(path, subPath);
        if (!PathUtils.isAbsolute(srcPath)) {
            throw new Exception("Absolute path expected: " + srcPath + ", pos: " + pos);
        }
        tokenizer.read(':');
        pos = tokenizer.getLastPos();
        String targetPath = tokenizer.readString();
        if (!PathUtils.isAbsolute(targetPath)) {
            targetPath = PathUtils.concat(path, targetPath);
            if (!PathUtils.isAbsolute(targetPath)) {
                throw new Exception("absolute path expected: " + targetPath + ", pos: " + pos);
            }
        }
        defaultHandler.nodeMoved(path, srcPath, targetPath);
    }

    private void parseOpSet() throws Exception {
        int pos = tokenizer.getLastPos();
        String subPath = tokenizer.readString();
        tokenizer.read(':');
        String value;
        if (tokenizer.matches(JsopReader.NULL)) {
            value = null;
        } else {
            value = tokenizer.readRawValue().trim();
        }
        String targetPath = PathUtils.concat(path, subPath);
        if (!PathUtils.isAbsolute(targetPath)) {
            throw new Exception("Absolute path expected: " + targetPath + ", pos: " + pos);
        }
        String parentPath = PathUtils.getParentPath(targetPath);
        String propName = PathUtils.getName(targetPath);
        defaultHandler.propertySet(parentPath, propName, JsonUtil.toJsonValue(value));
    }

    private void parseOpRemoved() throws Exception {
        int pos = tokenizer.getLastPos();
        String subPath = tokenizer.readString();
        String targetPath = PathUtils.concat(path, subPath);
        if (!PathUtils.isAbsolute(targetPath)) {
            throw new Exception("Absolute path expected: " + targetPath + ", pos: " + pos);
        }
        defaultHandler.nodeRemoved(path, subPath);
    }
}