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
package org.apache.jackrabbit.oak.namepath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * NamePathMapperImpl...
 */
public class NamePathMapperImpl implements NamePathMapper {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(NamePathMapperImpl.class);

    private final NameMapper nameMapper;

    public NamePathMapperImpl(NameMapper nameMapper) {
        this.nameMapper = nameMapper;
    }

    //---------------------------------------------------------< NameMapper >---
    @Override
    public String getOakName(String jcrName) {
        return nameMapper.getOakName(jcrName);
    }

    @Override
    public String getJcrName(String oakName) {
        return nameMapper.getJcrName(oakName);
    }

    //---------------------------------------------------------< PathMapper >---
    @Override
    public String getOakPath(String jcrPath) {
        final List<String> elements = new ArrayList<String>();
        final StringBuilder parseErrors = new StringBuilder();

        if ("/".equals(jcrPath)) {
            // avoid the need to special case the root path later on
            return "/";
        }

        JcrPathParser.Listener listener = new JcrPathParser.Listener() {

            @Override
            public boolean root() {
                if (!elements.isEmpty()) {
                    parseErrors.append("/ on non-empty path");
                    return false;
                }
                elements.add("");
                return true;
            }

            @Override
            public boolean identifier(String identifier) {
                if (!elements.isEmpty()) {
                    parseErrors.append("[identifier] on non-empty path");
                    return false;
                }
                elements.add(identifier);  // todo resolve identifier
                // todo seal elements
                return true;
            }

            @Override
            public boolean current() {
                // nothing to do here
                return true;
            }

            @Override
            public boolean parent() {
                if (elements.isEmpty()) {
                    parseErrors.append(".. of empty path");
                    return false;
                }
                elements.remove(elements.size() - 1);
                return true;
            }

            @Override
            public boolean index(int index) {
                if (index > 1) {
                    parseErrors.append("index > 1");
                    return false;
                }
                return true;
            }

            @Override
            public void error(String message) {
                parseErrors.append(message);
            }

            @Override
            public boolean name(String name) {
                String p = nameMapper.getOakName(name);
                if (p == null) {
                    parseErrors.append("Invalid name: ").append(name);
                    return false;
                }
                elements.add(p);
                return true;
            }
        };

        JcrPathParser.parse(jcrPath, listener);
        if (parseErrors.length() != 0) {
            log.debug("Could not parser path " + jcrPath + ": " + parseErrors.toString());
            return null;
        }

        StringBuilder oakPath = new StringBuilder();
        for (String element : elements) {
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
    public String getJcrPath(String oakPath) {
        final List<String> elements = new ArrayList<String>();

        if ("/".equals(oakPath)) {
            // avoid the need to special case the root path later on
            return "/";
        }

        JcrPathParser.Listener listener = new JcrPathParser.Listener() {
            @Override
            public boolean root() {
                if (!elements.isEmpty()) {
                    throw new IllegalArgumentException("/ on non-empty path");
                }
                elements.add("");
                return true;
            }

            @Override
            public boolean identifier(String identifier) {
                if (!elements.isEmpty()) {
                    throw new IllegalArgumentException("[identifier] on non-empty path");
                }
                elements.add(identifier);  // todo resolve identifier
                // todo seal elements
                return true;
            }

            @Override
            public boolean current() {
                // nothing to do here
                return false;
            }

            @Override
            public boolean parent() {
                if (elements.isEmpty()) {
                    throw new IllegalArgumentException(".. of empty path");
                }
                elements.remove(elements.size() - 1);
                return true;
            }

            @Override
            public boolean index(int index) {
                if (index > 1) {
                    throw new IllegalArgumentException("index > 1");
                }
                return true;
            }

            @Override
            public void error(String message) {
                throw new IllegalArgumentException(message);
            }

            @Override
            public boolean name(String name) {
                String p = nameMapper.getJcrName(name);
                elements.add(p);
                return true;
            }
        };

        JcrPathParser.parse(oakPath, listener);

        StringBuilder jcrPath = new StringBuilder();
        for (String element : elements) {
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
}
