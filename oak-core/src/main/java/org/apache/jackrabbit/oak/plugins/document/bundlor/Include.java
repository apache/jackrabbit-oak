/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.plugins.document.bundlor;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.commons.PathUtils;

import static com.google.common.base.Preconditions.checkElementIndex;
import static org.apache.jackrabbit.oak.commons.PathUtils.concat;

/**
 * Include represents a single path pattern which captures the path which
 * needs to be included in bundling. Path patterns can be like below.
 * <ul>
 *     <li>* - Match any immediate child</li>
 *     <li>*\/* - Match child with any name upto 2 levels of depth</li>
 *     <li>jcr:content - Match immediate child with name jcr:content</li>
 *     <li>jcr:content\/*;all - Match jcr:content and all its child</li>
 * </ul>
 *
 * The last path element can specify a directive. Supported directive
 * <ul>
 *     <li>all - Include all nodes under given path</li>
 * </ul>
 */
public class Include {
    enum Directive {ALL, NONE}
    private final String[] elements;
    private final Directive directive;
    private final String pattern;

    public Include(String pattern){
        List<String> pathElements = ImmutableList.copyOf(PathUtils.elements(pattern));
        List<String> elementList = Lists.newArrayListWithCapacity(pathElements.size());
        Directive directive = Directive.NONE;
        for (int i = 0; i < pathElements.size(); i++) {
            String e = pathElements.get(i);
            int indexOfColon = e.indexOf(";");
            if (indexOfColon > 0){
                directive = Directive.valueOf(e.substring(indexOfColon + 1).toUpperCase());
                e = e.substring(0, indexOfColon);
            }
            elementList.add(e);

            if (directive != Directive.NONE && i < pathElements.size() - 1){
                throw new IllegalArgumentException("Directive can only be specified for last path segment ["+pattern+"]");
            }
        }

        this.elements = elementList.toArray(new String[0]);
        this.directive = directive;
        this.pattern = pattern;
    }

    public boolean match(String relativePath) {
        Matcher m = createMatcher();
        for (String e : PathUtils.elements(relativePath)){
            m = m.next(e);
        }
        return m.isMatch();
    }

    public String getPattern() {
        return pattern;
    }

    @Override
    public String toString() {
        return pattern;
    }

    public Matcher createMatcher() {
        return new IncludeMatcher(this);
    }

    Directive getDirective() {
        return directive;
    }

    public boolean match(String nodeName, int depth) {
        checkElementIndex(depth, elements.length);
        String e = elements[depth];
        return "*".equals(e) || nodeName.equals(e);
    }

    public int size() {
        return elements.length;
    }

}
