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

import static com.google.common.base.Objects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.commons.PathUtils.elements;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterators;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * This {@code Filter} implementation supports filtering on paths using
 * simple glob patterns. Such a pattern is a string denoting a path. Each
 * element of the pattern is matched against the corresponding element of
 * a path. Elements of the pattern are matched literally except for the special
 * elements {@code *} and {@code **} where the former matches an arbitrary
 * path element and the latter matches any number of path elements (including none).
 * <p>
 * Note: an empty path pattern matches no path.
 * <p>
 * Note: path patterns only match against the corresponding elements of the path
 * and <em>do not</em> distinguish between absolute and relative paths.
 * <p>
 * Note: there is no way to escape {@code *} and {@code **}.
 * <p>
 * Examples:
 * <pre>
 *    q matches q only
 *    * matches character matches zero or more characters of a name component without crossing path boundaries (ie without crossing /)
 *    ** matches every path
 *    a/b/c matches a/b/c only
 *    a/*&#47;c matches a/x/c for every element x
 *    a/*.html&#47;c matches a/x.html/c for every character sequence x (that doesn't include /)
 *    a/*.*&#47;c matches a/x.y/c for every character sequence x and y (that don't include /)
 *    **&#47;y/z match every path ending in y/z
 *    r/s/t&#47;** matches r/s/t and all its descendants
 * </pre>
 */
public class GlobbingPathFilter implements EventFilter {
    public static final String STAR = "*";
    public static final String STAR_STAR = "**";

    // OAK-5589 : pattern can be a normal List, it doesn't have to be immutable -
    // given we create a new list in the public constructor anyway
    private final List<String> pattern;
    private final Map<String, Pattern> patternMap;

    private GlobbingPathFilter(@Nonnull List<String> pattern, Map<String, Pattern> patternMap) {
        // OAK-5589 : for internal constructor case don't copy the pattern, refer to the same one
        // this will work fine given the public constructors make a copy and internally we're
        // never fiddling with the pattern list
        this.pattern = checkNotNull(pattern);
        this.patternMap = checkNotNull(patternMap);
    }

    public GlobbingPathFilter(@Nonnull String pattern, Map<String, Pattern> patternMap) {
        // OAK-5589 : use the fastest way to create a List based on an unknown deep pattern
        this.pattern = new ArrayList<String>(10);
        Iterators.addAll(this.pattern, elements(checkNotNull(pattern)).iterator());
        this.patternMap = checkNotNull(patternMap);
    }

    /** for testing only - use variant which passes the patternMap for productive code **/
    public GlobbingPathFilter(@Nonnull String pattern) {
        this(pattern, new HashMap<String, Pattern>());
    }

    @Override
    public boolean includeAdd(PropertyState after) {
        return includeItem(after.getName());
    }

    @Override
    public boolean includeChange(PropertyState before, PropertyState after) {
        return includeItem(after.getName());
    }

    @Override
    public boolean includeDelete(PropertyState before) {
        return includeItem(before.getName());
    }

    @Override
    public boolean includeAdd(String name, NodeState after) {
        return includeItem(name);
    }

    @Override
    public boolean includeDelete(String name, NodeState before) {
        return includeItem(name);
    }

    @Override
    public boolean includeMove(String sourcePath, String name, NodeState moved) {
        return includeItem(name);
    }

    @Override
    public boolean includeReorder(String destName, String name, NodeState reordered) {
        return includeItem(name);
    }
    
    private Pattern getPattern(String wildcardStr) {
        Pattern p = patternMap.get(wildcardStr);
        if (p == null) {
            p = Pattern.compile("\\Q"+wildcardStr.replace("*", "\\E[^/]*\\Q") + "\\E");
            patternMap.put(wildcardStr, p);
        }
        return p;
    }
    
    private boolean wildcardMatch(String pathElement, String wildcardStr) {
        if (STAR_STAR.equals(wildcardStr) || !wildcardStr.contains(STAR)) {
            return pathElement.equals(wildcardStr);
        }
        Pattern regexPattern = getPattern(wildcardStr);
        return regexPattern.matcher(pathElement).matches();
    }

    @Override
    public EventFilter create(String name, NodeState before, NodeState after) {
        if (pattern.isEmpty()) {
            return null;
        }

        String head = pattern.get(0);
        if (pattern.size() == 1 && !STAR_STAR.equals(head)) {
            // shortcut when no further matches are possible
            return null;
        }

        if (wildcardMatch(name, head)) {
            return new GlobbingPathFilter(pattern.subList(1, pattern.size()), patternMap);
        } else if (STAR_STAR.equals(head)) {
            if (pattern.size() >= 2 && wildcardMatch(name, pattern.get(1))) {
                // ** matches empty list of elements and pattern.get(1) matches name
                // match the rest of the pattern against the rest of the path and
                // match the whole pattern against the rest of the path
                return Filters.any(
                        new GlobbingPathFilter(pattern.subList(2, pattern.size()), patternMap),
                        new GlobbingPathFilter(pattern, patternMap)
                );
            } else {
                // ** matches name, match the whole pattern against the rest of the path
                return new GlobbingPathFilter(pattern, patternMap);
            }
        } else {
            return null;
        }
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("path", Joiner.on('/').join(pattern))
                .toString();
    }

    //------------------------------------------------------------< private >---

    private boolean includeItem(String name) {
        if (!pattern.isEmpty() && pattern.size() <= 2) {
            String head = pattern.get(0);
            if (STAR_STAR.equals(head)) {
                if (pattern.size() == 1) {
                    return true;
                } else {
                    return wildcardMatch(name, pattern.get(1));
                }
            }
            boolean headMatches = wildcardMatch(name, head);
            boolean result = pattern.size() == 1
                ? headMatches
                : headMatches && STAR_STAR.equals(pattern.get(1));
            return result;
        } else {
            return false;
        }
    }

}
