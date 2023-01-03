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
package org.apache.jackrabbit.oak.plugins.index.cursor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.IndexRow;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.PrefetchNodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A cursor that is able to prefetch nodes from the node store.
 */
public class PrefetchCursor extends AbstractCursor {

    private static final Logger LOG = LoggerFactory.getLogger(PrefetchCursor.class);

    // match ${...} -- unfortunately it needs a lot of escaping. 
    // } inside {...} need to be escaped: ${...\}...} 
    private static final Pattern FUNCTION = Pattern.compile("\\$\\{((\\}|[^}])*)\\}");

    private final Cursor cursor;
    private final PrefetchNodeStore store;
    private final int prefetchCount;
    private final NodeState rootState;
    private Iterator<IndexRow> prefetched;
    private final List<String> prefetchRelative;

    PrefetchCursor(Cursor cursor, PrefetchNodeStore store, int prefetchCount, NodeState rootState, List<String> prefetchRelative) {
        this.cursor = cursor;
        this.store = store;
        this.prefetchCount = prefetchCount;
        this.rootState = rootState;
        this.prefetched = Collections.emptyIterator();
        this.prefetchRelative = prefetchRelative;
    }

    @Override
    public IndexRow next() {
        if (!prefetched.hasNext()) {
            ArrayList<IndexRow> rows = new ArrayList<>();
            TreeSet<String> paths = new TreeSet<>();
            for (int i = 0; i < prefetchCount && cursor.hasNext(); i++) {
                IndexRow row = cursor.next();
                rows.add(row);
                if (row.isVirtualRow()) {
                    continue;
                }
                String p = row.getPath();
                if (!PathUtils.isAbsolute(p)) {
                    LOG.warn("Unexpected relative path {}", p);
                    continue;
                }
                prefetchRelative(paths, p);
                do {
                    paths.add(p);
                    p = PathUtils.getParentPath(p);
                } while (!PathUtils.denotesRoot(p));
            }
            store.prefetch(paths, rootState);
            prefetched = rows.iterator();
        }
        return prefetched.next();
    }

    @Override
    public boolean hasNext() {
        return prefetched.hasNext() || cursor.hasNext();
    }

    private void prefetchRelative(Set<String> target, String p) {
        try {
            for (String r : prefetchRelative) {
                String result = resolve(r, p);
                if (result != null) {
                    target.add(result);
                }
            }
        } catch (IllegalArgumentException e) {
            // ignore
        }
    }
    
    /**
     * Try to compute an absolute path using a relative path specification.
     * 
     * @param relativePath a relative path that may include patterns, e.g. jcr:content/metadata,
     *              or jcr:content/renditions/${regex(".+/(.*)\\..*")}
     * @param path the input path, e.g. /content/images/abc
     * @return null if not a match, or the output path, e.g.
     *         /content/images/abc.png/jcr:content/renditions/abc
     * @throws IllegalArgumentException if an error occurs
     */
    public static String resolve(String relativePath, String path) throws IllegalArgumentException {
        String resolved = applyAndResolvePatterns(relativePath, path);
        if (resolved == null) {
            return null;
        }
        if (PathUtils.isAbsolute(resolved)) {
            return resolved;
        }
        return PathUtils.concat(path, resolved);
    }
    
    /**
     * Compute the new relative path from a input path and a relative path (which
     * may include patterns).
     *
     * @param relativePath the value that may include patterns, e.g.
     *                     jcr:content/metadata, or
     *                     jcr:content/renditions/${regex(".+/(.*)\\..*")}
     * @param path the input path, e.g. /content/images/abc
     * @return null if not a match, or the output (relative) path, e.g.
     *         jcr:content/renditions/abc
     * @throws IllegalArgumentException if an error occurs
     */
    public static String applyAndResolvePatterns(String relativePath, String path) 
            throws IllegalArgumentException {
        Matcher m = FUNCTION.matcher(relativePath);
        // appendReplacement with StringBuilder is not supported in Java 8
        StringBuffer buff = new StringBuffer();
        // iterate over all the "${...}"
        try {
            while (m.find()) {
                String f = m.group(1);
                // un-escape: \} => }
                f = f.replaceAll("\\\\}", "}");
                // we only support regex("...") currently
                if (f.startsWith("regex(\"") && f.endsWith("\")")) {
                    String regexPattern = f.substring("regex(\"".length(), f.length() - 2);
                    // un-escape: \" => "
                    regexPattern = regexPattern.replaceAll("\\\"", "\"");
                    String x = findMatch(path, regexPattern);
                    if (x == null) {
                        // not a match
                        return null;
                    }
                    m.appendReplacement(buff, x);
                } else {
                    // don't know how to replace - don't
                    // (we could also throw an exception)
                    break;
                }
            }
        } catch (IllegalStateException | IndexOutOfBoundsException e) {
            LOG.warn("Can not replace {} match in {}", relativePath, path, e);
            throw new IllegalArgumentException("Can not replace");
        }            
        m.appendTail(buff);
        return buff.toString();
    }

    /**
     * Find a pattern in a given path.
     * 
     * @param path the absolute path, e.g. "/content/images/hello.jpg"
     * @param regexPattern the pattern, e.g. ".+/(.*).jpg"
     * @return null if not found, or the matched group, e.g. "hello"
     * @throws IllegalArgumentException if an error occurs
     */
    public static String findMatch(String path, String regexPattern) 
            throws IllegalArgumentException {
        try {
            Pattern p = Pattern.compile(regexPattern);
            Matcher m = p.matcher(path);
            if (!m.find()) {
                return null;
            }
            return m.group(1);
        } catch (PatternSyntaxException | IllegalStateException | IndexOutOfBoundsException e) {
            LOG.warn("Can not find match in {} pattern {}", path, regexPattern, e);
            throw new IllegalArgumentException("Can not find match", e);
        }
    }

}
