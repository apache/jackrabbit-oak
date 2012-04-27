/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.jackrabbit.oak.namepath;

import org.apache.jackrabbit.oak.namepath.JcrNameParser.Listener;

import java.util.ArrayList;
import java.util.List;

/**
 * All path in the Oak API have the following form
 * <p>
 * <pre>
 * PATH    := ("/" ELEMENT)* | ELEMENT ("/" ELEMENT)*
 * ELEMENT := [PREFIX ":"] NAME
 * PREFIX  := non empty string not containing ":" and "/"
 * NAME    := non empty string not containing ":" and "/" TODO: check whether this is correct
 * </pre>
 */
public final class Paths {

    private Paths() {}

    public static String toOakName(String name, final NameMapper mapper) {
        final StringBuilder element = new StringBuilder();
        
        Listener listener = new JcrNameParser.Listener() {
            @Override
            public void error(String message) {
                throw new RuntimeException(message);
            }

            @Override
            public void name(String name) {
                String p = mapper.getOakName(name);
                element.append(p);
            }
        };

        JcrNameParser.parse(name, listener);
        return element.toString();
    }
    
    public static String toOakPath(String jcrPath, final NameMapper mapper) {
        final List<String> elements = new ArrayList<String>();

        if ("/".equals(jcrPath)) {
            // avoid the need to special case the root path later on
            return "/";
        }

        JcrPathParser.Listener listener = new JcrPathParser.Listener() {

            // TODO: replace RuntimeException by something that oak-jcr can deal with (e.g. ValueFactory)

            @Override
            public void root() {
                if (!elements.isEmpty()) {
                    throw new RuntimeException("/ on non-empty path");
                }
                elements.add("");
            }

            @Override
            public void identifier(String identifier) {
                if (!elements.isEmpty()) {
                    throw new RuntimeException("[identifier] on non-empty path");
                }
                elements.add(identifier);  // todo resolve identifier
                // todo seal
            }

            @Override
            public void current() {
                // nothing to do here
            }

            @Override
            public void parent() {
                if (elements.isEmpty()) {
                    throw new RuntimeException(".. of empty path");
                }
                elements.remove(elements.size() - 1);
            }

            @Override
            public void index(int index) {
                if (index > 1) {
                    throw new RuntimeException("index > 1");
                }
            }

            @Override
            public void error(String message) {
                throw new RuntimeException(message);
            }

            @Override
            public void name(String name) {
                String p = mapper.getOakName(name);
                elements.add(p);
            }
        };

        JcrPathParser.parse(jcrPath, listener);
        
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
    
    public static String toJcrPath(String oakPath, final NameMapper mapper) {
        final List<String> elements = new ArrayList<String>();
        
        if ("/".equals(oakPath)) {
            // avoid the need to special case the root path later on
            return "/";
        }

        JcrPathParser.Listener listener = new JcrPathParser.Listener() {
            @Override
            public void root() {
                if (!elements.isEmpty()) {
                    throw new RuntimeException("/ on non-empty path");
                }
                elements.add("");
            }

            @Override
            public void identifier(String identifier) {
                if (!elements.isEmpty()) {
                    throw new RuntimeException("[identifier] on non-empty path");
                }
                elements.add(identifier);  // todo resolve identifier
                // todo seal
            }

            @Override
            public void current() {
                // nothing to do here
            }

            @Override
            public void parent() {
                if (elements.isEmpty()) {
                    throw new RuntimeException(".. of empty path");
                }
                elements.remove(elements.size() - 1);
            }

            @Override
            public void index(int index) {
                if (index > 1) {
                    throw new RuntimeException("index > 1");
                }
            }

            @Override
            public void error(String message) {
                throw new RuntimeException(message);
            }

            @Override
            public void name(String name) {
                String p = mapper.getJcrName(name);
                elements.add(p);
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
