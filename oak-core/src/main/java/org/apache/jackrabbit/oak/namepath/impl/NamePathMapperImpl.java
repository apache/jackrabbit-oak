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
package org.apache.jackrabbit.oak.namepath.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.namepath.JcrPathParser;
import org.apache.jackrabbit.oak.namepath.JcrPathParser.Listener;
import org.apache.jackrabbit.oak.namepath.NameMapper;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.identifier.IdentifierManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO document
 */
public class NamePathMapperImpl implements NamePathMapper {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(NamePathMapperImpl.class);

    private final NameMapper nameMapper;
    private final IdentifierManager idManager;

    public NamePathMapperImpl(NameMapper nameMapper) {
        this.nameMapper = nameMapper;
        this.idManager = null;
    }

    public NamePathMapperImpl(NameMapper nameMapper, IdentifierManager idManager) {
        this.nameMapper = nameMapper;
        this.idManager = idManager;
    }

    //---------------------------------------------------------< NameMapper >---
    @Override
    public String getOakNameOrNull(@Nonnull String jcrName) {
        return nameMapper.getOakNameOrNull(jcrName);
    }

    @Nonnull
    @Override
    public String getOakName(@Nonnull String jcrName) throws RepositoryException {
        return nameMapper.getOakName(jcrName);
    }

    @Nonnull
    @Override
    public String getJcrName(@Nonnull String oakName) {
        return nameMapper.getJcrName(oakName);
    }

    @Override @Nonnull
    public Map<String, String> getSessionLocalMappings() {
        return nameMapper.getSessionLocalMappings();
    }

    //---------------------------------------------------------< PathMapper >---
    @Override
    public String getOakPath(String jcrPath) {
        if (!needsFullMapping(jcrPath)) {
            return jcrPath;
        }

        int length = jcrPath.length();

        // identifier path?
        if (length > 0 && jcrPath.charAt(0) == '[') {
            if (jcrPath.charAt(length - 1) != ']') {
                log.debug("Could not parse path " + jcrPath + ": unterminated identifier");
                return null;
            }
            if (this.idManager == null) {
                log.debug("Could not parse path " + jcrPath + ": could not resolve identifier");
                return null;
            }
            return this.idManager.getPath(jcrPath.substring(1, length - 1));
        }

        final StringBuilder parseErrors = new StringBuilder();

        PathListener listener = new PathListener() {
            @Override
            public void error(String message) {
                parseErrors.append(message);
            }

            @Override
            public boolean name(String name, int index) {
                if (index < 0) {
                    error("invalid index: " + index);
                    return false;
                }

                String oakName = nameMapper.getOakNameOrNull(name);
                if (oakName == null) {
                    error("Invalid name: " + name);
                    return false;
                }
                if (index > 1) {
                    oakName += "[" + index + ']';
                }
                elements.add(oakName);
                return true;
            }
        };

        JcrPathParser.parse(jcrPath, listener);
        if (parseErrors.length() != 0) {
            log.debug("Could not parse path " + jcrPath + ": " + parseErrors.toString());
            return null;
        }

        // Empty path maps to ""
        if (listener.elements.isEmpty()) {
            return "";
        }

        StringBuilder oakPath = new StringBuilder();
        for (String element : listener.elements) {
            if (element.isEmpty()) {
                // root
                oakPath.append('/');
            }
            else {
                oakPath.append(element);
                oakPath.append('/');
            }
        }

        // root path is special-cased early on so it does not need to
        // be considered here
        oakPath.deleteCharAt(oakPath.length() - 1);
        return oakPath.toString();
    }

    @Override
    @Nonnull
    public String getJcrPath(final String oakPath) {
        if ("/".equals(oakPath)) {
            // avoid the need to special case the root path later on
            return "/";
        } else if (oakPath.isEmpty()) {
            // empty path: map to "."
            return ".";
        } else if (nameMapper.getSessionLocalMappings().isEmpty()) {
            // no local namespace mappings
            return oakPath;
        }

        PathListener listener = new PathListener() {
            @Override
            public boolean current() {
                // nothing to do here
                return false;
            }

            @Override
            public void error(String message) {
                throw new IllegalArgumentException(message);
            }

            @Override
            public boolean name(String name, int index) {
                String p = nameMapper.getJcrName(name);
                if (index == 0) {
                    elements.add(p);
                } else {
                    elements.add(p + '[' + index + ']');
                }
                return true;
            }
        };

        JcrPathParser.parse(oakPath, listener);

        StringBuilder jcrPath = new StringBuilder();
        for (String element : listener.elements) {
            if (element.isEmpty()) {
                // root
                jcrPath.append('/');
            }
            else {
                jcrPath.append(element);
                jcrPath.append('/');
            }
        }

        jcrPath.deleteCharAt(jcrPath.length() - 1);
        return jcrPath.toString();
    }

    /**
     * Checks if the given path needs to be fully parsed to apply namespace
     * mappings or to validate its syntax. If the given path is "simple", i.e.
     * it doesn't contain any complex constructs, and there are no local
     * namespace remappings, it's possible to skip the full path parsing
     * and simply use the JCR path string as-is as an Oak path.
     *
     * @param path JCR path
     * @return {@code true} if the path needs to be fully parsed,
     *         {@code false} if not
     */
    private boolean needsFullMapping(String path) {
        int length = path.length();
        if (length == 0) {
            return true;
        }

        int slash = -1; // index of the last slash in the path
        int colon = -1; // index of the last colon in the path

        switch (path.charAt(0)) {
            case '{': // possibly an expanded name
            case '[': // starts with an identifier
            case '.': // possibly "." or ".."
            case ':': // colon as the first character
                return true;
            case '/':
                if (length == 1) {
                    return false; // the root path
                }
                slash = 0;
                break;
        }

        for (int i = 1; i < length; i++) {
            switch (path.charAt(i)) {
                case '{': // possibly an expanded name
                case '[': // possibly an index
                case ']': // illegal character if not part of index
                case '|': // illegal character
                case '*': // illegal character
                    return true;
                case '.':
                    if (i == slash + 1) {
                        return true; // possibly "." or ".."
                    }
                    break;
                case ':':
                    if (i == slash + 1              // "x/:y"
                            || i == colon + i       // "x::y"
                            || colon > slash        // "x:y:z"
                            || i + 1 == length) {   // "x:"
                        return true;
                    }
                    colon = i;
                    break;
                case '/':
                    if (i == slash + 1              // "x//y"
                            || i == colon + i       // "x:/y"
                            || i + 1 == length) {   // "x/"
                        return true;
                    }
                    slash = i;
                    break;
            }
        }

        return colon != -1 && !nameMapper.getSessionLocalMappings().isEmpty();
    }

    //------------------------------------------------------------< PathListener >---

    private abstract static class PathListener implements Listener {
        final List<String> elements = new ArrayList<String>();

        @Override
        public boolean root() {
            if (!elements.isEmpty()) {
                error("/ on non-empty path");
                return false;
            }
            elements.add("");
            return true;
        }

        @Override
        public boolean current() {
            // nothing to do here
            return true;
        }

        @Override
        public boolean parent() {
            int prevIdx = elements.size() - 1;
            String prevElem = prevIdx >= 0 ? elements.get(prevIdx) : null;

            if (prevElem == null || PathUtils.denotesParent(prevElem)) {
                elements.add("..");
                return true;
            }
            if (prevElem.isEmpty()) {
                error("Absolute path escapes root");
                return false;
            }

            elements.remove(prevIdx);
            return true;
        }
    }

}
