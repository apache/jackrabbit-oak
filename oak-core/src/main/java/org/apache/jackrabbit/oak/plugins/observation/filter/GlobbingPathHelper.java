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
package org.apache.jackrabbit.oak.plugins.observation.filter;

import static org.apache.jackrabbit.oak.commons.PathUtils.elements;

import java.util.List;

import com.google.common.collect.Lists;

public class GlobbingPathHelper {

    /**
     * Converts the provided path containing glob characters <code>*</code> 
     * and <code>**</code> into a regular expression. The definition matches 
     * that of the GlobbingPathFilter with the addition that this conversion
     * also supports sub-paths which do not start with a <code>/</code>.
     * <p>
     * The rules are:
     * <ul>
     * <li>leading <code>**</code> matches <code>/foo</code> and <code>bar</code></li>
     * <li>leading <code>/**</code> matches <code>/foo</code> but not <code>bar</code></li>
     * <li>intermittent <code>**</code> matches zero or any number of path elements </li>
     * <li>trailing <code>**</code> matches anything not ending with a <code>/</code></li>
     * <li>single <code>*</code> matches anything except <code>/</code></li>
     * <li><code>?</code> is not a special character</li>
     * <li>anything not a star is wrapped into <code>\Q...\E</code> pairs</li>
     * </ul>
     * @param pathWithGlobs path that can contain * and **
     * @return a regular expression
     * @see GlobbingPathFilter
     */
    public static String globPathAsRegex(String pathWithGlobs) {
        if (pathWithGlobs == null) {
            return null;
        } else if (!pathWithGlobs.contains("*")) {
            return pathWithGlobs;
        }
        List<String> elements = Lists.newLinkedList(elements(pathWithGlobs));
        StringBuffer sb = new StringBuffer();
        sb.append("\\Q");
        if (pathWithGlobs.startsWith("/")) {
            sb.append("/");
        }
        if (elements.get(0).equals("**")) {
            sb.append("\\E[^/]*(/[^/]*)*\\Q");
            elements.remove(0);
        }
        int size = elements.size();
        boolean endsWithStarStar = size == 0 ? false : elements.get(size - 1).equals("**");
        if (endsWithStarStar) {
            elements.remove(size - 1);
        }
        boolean addSlash = false;
        for(int i=0; i<elements.size(); i++) {
            String pathElem = elements.get(i);
            if (addSlash) {
                sb.append("/");
            }
            if (pathElem.equals("**")) {
                addSlash = false;
                sb.append("\\E([^/]*/)*\\Q");
            } else {
                sb.append(pathElem.replace("*", "\\E[^/]*\\Q"));
                addSlash = true;
            }
        }
        if (endsWithStarStar) {
            sb.append("\\E(/[^/]*)*");
        } else if (pathWithGlobs.endsWith("/")) {
            sb.append("/\\E");
        } else {
            sb.append("\\E");
        }
        return sb.toString();
    }

}
