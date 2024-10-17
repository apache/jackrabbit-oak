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
package org.apache.jackrabbit.oak.plugins.document;

import java.util.Arrays;
import java.util.Objects;

import org.apache.jackrabbit.oak.cache.CacheValue;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.jackrabbit.oak.commons.conditions.Validate.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.apache.jackrabbit.guava.common.collect.Iterables.elementsEqual;

/**
 * The {@code Path} class is closely modeled after the semantics of
 * {@code PathUtils} in oak-commons. Corresponding methods in this class can
 * be used as a replacement for the methods in {@code PathUtils} on {@code Path}
 * objects.
 */
public final class Path implements CacheValue, Comparable<Path> {

    public static final Path ROOT = new Path(null, "", "".hashCode());

    @Nullable
    private final Path parent;

    @NotNull
    private final String name;

    private int hash;

    private Path(@Nullable Path parent,
                 @NotNull String name,
                 int hash) {
        this.parent = parent;
        this.name = name;
        this.hash = hash;
    }

    /**
     * Creates a new {@code Path} from the given parent {@code Path}. The name
     * of the new {@code Path} cannot be the empty {@code String}.
     *
     * @param parent the parent {@code Path}.
     * @param name the name of the new {@code Path}.
     * @throws IllegalArgumentException if the {@code name} is empty.
     */
    public Path(@NotNull Path parent, @NotNull String name) {
        this(requireNonNull(parent), requireNonNull(name), -1);
        checkArgument(!name.isEmpty(), "name cannot be the empty String");
    }

    /**
     * Creates a relative path with a single name element. The name cannot be
     * the empty {@code String}.
     *
     * @param name the name of the first path element.
     * @throws IllegalArgumentException if the {@code name} is empty.
     */
    public Path(@NotNull String name) {
        this(null, requireNonNull(name), -1);
        checkArgument(!name.isEmpty(), "name cannot be the empty String");
        checkArgument(name.indexOf('/') == -1, "name must not contain path separator: %s", name);
    }

    /**
     * Returns the name of this path. The {@link #ROOT} is the only path with
     * an empty name. That is a String with length zero.
     *
     * @return the name of this path.
     */
    @NotNull
    public String getName() {
        return name;
    }

    /**
     * Returns the names of the path elements with increasing {@link #getDepth()}
     * starting at depth 1.
     * 
     * @return the names of the path elements.
     */
    @NotNull
    public Iterable<String> elements() {
        return elements(false);
    }

    /**
     * Returns {@code true} if this is the {@link #ROOT} path; {@code false}
     * otherwise.
     *
     * @return whether this is the {@link #ROOT} path.
     */
    public boolean isRoot() {
        return name.isEmpty();
    }

    /**
     * The parent of this path or {@code null} if this path does not have a
     * parent. The {@link #ROOT} path and the first path element of a relative
     * path do not have a parent.
     *
     * @return the parent of this path or {@code null} if this path does not
     *      have a parent.
     */
    @Nullable
    public Path getParent() {
        return parent;
    }

    /**
     * @return the number of characters of the {@code String} representation of
     *  this path.
     */
    public int length() {
        if (isRoot()) {
            return 1;
        }
        int length = 0;
        Path p = this;
        while (p != null) {
            length += p.name.length();
            if (p.parent != null) {
                length++;
            }
            p = p.parent;
        }
        return length;
    }

    /**
     * The depth of this path. The {@link #ROOT} has a depth of 0. The path
     * {@code /foo/bar} as well as {@code bar/baz} have depth 2.
     *
     * @return the depth of the path.
     */
    public int getDepth() {
        return getNumberOfPathElements(false);
    }

    /**
     * Get the nth ancestor of a path. The 1st ancestor is the parent path,
     * 2nd ancestor the grandparent path, and so on...
     * <p>
     * If {@code nth <= 0}, then this path is returned.
     *
     * @param nth  indicates the ancestor level for which the path should be
     *             calculated.
     * @return the ancestor path
     */
    @NotNull
    public Path getAncestor(int nth) {
        Path p = this;
        while (nth-- > 0 && p.parent != null) {
            p = p.parent;
        }
        return p;
    }

    /**
     * Return {@code true} if {@code this} path is an ancestor of the
     * {@code other} path, otherwise {@code false}.
     *
     * @param other the other path.
     * @return whether this path is an ancestor of the other path.
     */
    public boolean isAncestorOf(@NotNull Path other) {
        requireNonNull(other);
        int depthDiff = other.getDepth() - getDepth();
        return depthDiff > 0
                && elementsEqual(elements(true), other.getAncestor(depthDiff).elements(true));
    }

    /**
     * @return {@code true} if this is an absolute path; {@code false} otherwise.
     */
    public boolean isAbsolute() {
        Path p = this;
        while (p.parent != null) {
            p = p.parent;
        }
        return p.isRoot();
    }

    /**
     * Creates a {@code Path} from a {@code String}.
     *
     * @param path the {@code String} to parse.
     * @return the {@code Path} from the {@code String}.
     * @throws IllegalArgumentException if the {@code path} is the empty
     *      {@code String}.
     */
    @NotNull
    public static Path fromString(@NotNull String path) throws IllegalArgumentException {
        requireNonNull(path);
        Path p = null;
        if (PathUtils.isAbsolute(path)) {
            p = ROOT;
        }
        for (String name : PathUtils.elements(path)) {
            name = StringCache.get(name);
            if (p == null) {
                p = new Path(name);
            } else {
                p = new Path(p, name);
            }
        }
        if (p == null) {
            throw new IllegalArgumentException("path must not be empty");
        }
        return p;
    }

    /**
     * Appends the {@code String} representation of this {@code Path} to the
     * passed {@code StringBuilder}. See also {@link #toString()}.
     *
     * @param sb the {@code StringBuilder} this {@code Path} is appended to.
     * @return the passed {@code StringBuilder}.
     */
    @NotNull
    public StringBuilder toStringBuilder(@NotNull StringBuilder sb) {
        if (isRoot()) {
            sb.append('/');
        } else {
            buildPath(sb);
        }
        return sb;
    }

    @Override
    public int getMemory() {
        int memory = 0;
        Path p = this;
        while (p.parent != null) {
            memory += 24; // shallow size
            memory += StringUtils.estimateMemoryUsage(name);
            p = p.parent;
        }
        return memory;
    }

    @Override
    public int compareTo(@NotNull Path other) {
        if (this == other) {
            return 0;
        }
        Path t = this;
        int off = t.getNumberOfPathElements(true) -
                requireNonNull(other).getNumberOfPathElements(true);
        int corrected = off;
        while (corrected > 0) {
            t = t.parent;
            corrected--;
        }
        while (corrected < 0) {
            other = other.parent;
            corrected++;
        }
        int cp = comparePath(t, other);
        if (cp != 0) {
            return cp;
        }
        return Integer.signum(off);
    }

    @Override
    public String toString() {
        if (isRoot()) {
            return "/";
        } else {
            return buildPath(new StringBuilder(length())).toString();
        }
    }

    @Override
    public int hashCode() {
        int h = hash;
        if (h == -1 && parent != null) {
            h = 17;
            h = 37 * h + parent.hashCode();
            h = 37 * h + name.hashCode();
            hash = h;
        }
        return h;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj instanceof Path) {
            Path other = (Path) obj;
            return this.name.equals(other.name)
                    && Objects.equals(this.parent, other.parent);
        }
        return false;
    }

    //-------------------------< internal >-------------------------------------

    private Iterable<String> elements(boolean withRoot) {
        int size = getNumberOfPathElements(withRoot);
        String[] elements = new String[size];
        Path p = this;
        for (int i = size - 1; p != null; i--) {
            if (withRoot || !p.isRoot()) {
                elements[i] = p.name;
            }
            p = p.parent;
        }
        return Arrays.asList(elements);
    }

    private StringBuilder buildPath(StringBuilder sb) {
        if (parent != null) {
            parent.buildPath(sb).append("/");
        }
        sb.append(name);
        return sb;
    }

    /**
     * Returns the number of path elements. Depending on {@code withRoot} the
     * root of an absolute path is also taken into account.
     *
     * @param withRoot whether the root of an absolute path is also counted.
     * @return the number of path elements.
     */
    private int getNumberOfPathElements(boolean withRoot) {
        int depth = 0;
        for (Path p = this; p != null; p = p.parent) {
            if (withRoot || !p.isRoot()) {
                depth++;
            }
        }
        return depth;
    }

    private static int comparePath(Path a, Path b) {
        if (a.parent != b.parent) {
            int cp = comparePath(a.parent, b.parent);
            if (cp != 0) {
                return cp;
            }
        }
        return a.name.compareTo(b.name);
    }
}
