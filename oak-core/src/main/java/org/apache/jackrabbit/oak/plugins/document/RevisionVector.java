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
package org.apache.jackrabbit.oak.plugins.document;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import org.apache.jackrabbit.oak.cache.CacheValue;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.toArray;
import static com.google.common.collect.Iterators.peekingIterator;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static java.util.Arrays.sort;

/**
 * A vector of revisions. Instances of this class are immutable and methods
 * like {@link #update(Revision)} create a new instance as needed.
 *
 * This class implements {@link Comparable}. While
 * {@link #compareTo(RevisionVector)} provides a total order of revision
 * vector instances, this order is unrelated to when changes are visible in
 * a DocumentNodeStore cluster. Do not use this method to determine whether
 * a given revision vector happened before or after another!
 */
public final class RevisionVector implements Iterable<Revision>, Comparable<RevisionVector>, CacheValue {

    private static final Logger log = LoggerFactory.getLogger(RevisionVector.class);

    private final static RevisionVector EMPTY = new RevisionVector();

    private final Revision[] revisions;

    //lazily initialized
    private int hash;

    private RevisionVector(@Nonnull Revision[] revisions,
                           boolean checkUniqueClusterIds,
                           boolean sort) {
        checkNotNull(revisions);
        if (checkUniqueClusterIds) {
            checkUniqueClusterIds(revisions);
        }
        if (sort) {
            sort(revisions, RevisionComparator.INSTANCE);
        }
        this.revisions = revisions;
    }

    public RevisionVector(@Nonnull Revision... revisions) {
        this(Arrays.copyOf(revisions, revisions.length), true, true);
    }

    public RevisionVector(@Nonnull Iterable<Revision> revisions) {
        this(toArray(revisions, Revision.class), true, true);
    }

    public RevisionVector(@Nonnull Set<Revision> revisions) {
        this(toArray(revisions, Revision.class), false, true);
    }

    /**
     * Creates a new revision vector with based on this vector and the given
     * {@code revision}. If this vector contains a revision with the same
     * clusterId as {@code revision}, the returned vector will have the
     * revision updated with the given one. Otherwise the returned vector will
     * have all elements of this vector plus the given {@code revision}.
     *
     * @param revision the revision set to use for the new vector.
     * @return the resulting revision vector.
     */
    public RevisionVector update(@Nonnull Revision revision) {
        checkNotNull(revision);
        Revision existing = null;
        int i;
        for (i = 0; i < revisions.length; i++) {
            Revision r = revisions[i];
            if (r.getClusterId() == revision.getClusterId()) {
                existing = r;
                break;
            }
        }
        Revision[] newRevisions;
        boolean sort;
        if (existing != null) {
            if (revision.equals(existing)) {
                return this;
            } else {
                newRevisions = Arrays.copyOf(revisions, revisions.length);
                newRevisions[i] = revision;
                sort = false;
            }
        } else {
            newRevisions = new Revision[revisions.length + 1];
            System.arraycopy(revisions, 0, newRevisions, 0, revisions.length);
            newRevisions[revisions.length] = revision;
            sort = true;
        }
        return new RevisionVector(newRevisions, false, sort);
    }

    /**
     * Returns a RevisionVector without the revision element with the given
     * {@code clusterId}.
     *
     * @param clusterId the clusterId of the revision to remove.
     * @return RevisionVector without the revision element.
     */
    public RevisionVector remove(int clusterId) {
        if (revisions.length == 0) {
            return this;
        }
        boolean found = false;
        for (Revision r : revisions) {
            if (r.getClusterId() == clusterId) {
                found = true;
                break;
            } else if (r.getClusterId() > clusterId) {
                break;
            }
        }
        if (!found) {
            return this;
        }
        Revision[] revs = new Revision[revisions.length - 1];
        int idx = 0;
        for (Revision r : revisions) {
            if (r.getClusterId() != clusterId) {
                revs[idx++] = r;
            }
        }
        return new RevisionVector(revs, false, false);
    }

    /**
     * Calculates the parallel minimum of this and the given {@code vector}.
     *
     * @param vector the other vector.
     * @return the parallel minimum of the two.
     */
    public RevisionVector pmin(@Nonnull RevisionVector vector) {
        // optimize single revision case
        if (revisions.length == 1 && vector.revisions.length == 1) {
            if (revisions[0].getClusterId() == vector.revisions[0].getClusterId()) {
                return revisions[0].compareRevisionTime(vector.revisions[0]) < 0 ? this : vector;
            } else {
                return EMPTY;
            }
        }
        int capacity = Math.min(revisions.length, vector.revisions.length);
        List<Revision> pmin = newArrayListWithCapacity(capacity);
        PeekingIterator<Revision> it = peekingIterator(vector.iterator());
        for (Revision r : revisions) {
            Revision other = peekRevision(it, r.getClusterId());
            if (other != null) {
                if (other.getClusterId() == r.getClusterId()) {
                    pmin.add(Utils.min(r, other));
                }
            } else {
                break;
            }
        }
        return new RevisionVector(toArray(pmin, Revision.class), false, false);
    }

    /**
     * Calculates the parallel maximum of this and the given {@code vector}.
     *
     * @param vector the other vector.
     * @return the parallel maximum of the two.
     */
    public RevisionVector pmax(@Nonnull RevisionVector vector) {
        // optimize single revision case
        if (revisions.length == 1 && vector.revisions.length == 1) {
            if (revisions[0].getClusterId() == vector.revisions[0].getClusterId()) {
                return revisions[0].compareRevisionTime(vector.revisions[0]) > 0 ? this : vector;
            } else {
                return new RevisionVector(revisions[0], vector.revisions[0]);
            }
        }
        int capacity = Math.max(revisions.length, vector.revisions.length);
        List<Revision> pmax = newArrayListWithCapacity(capacity);
        PeekingIterator<Revision> it = peekingIterator(vector.iterator());
        for (Revision r : revisions) {
            while (it.hasNext() && it.peek().getClusterId() < r.getClusterId()) {
                pmax.add(it.next());
            }
            Revision other = peekRevision(it, r.getClusterId());
            if (other != null && other.getClusterId() == r.getClusterId()) {
                pmax.add(Utils.max(r, other));
                it.next();
            } else {
                // other does not have a revision with r.clusterId
                pmax.add(r);
            }
        }
        // add remaining
        Iterators.addAll(pmax, it);
        return new RevisionVector(toArray(pmax, Revision.class), false, false);
    }

    /**
     * Returns the difference of this and the other vector. The returned vector
     * contains all revisions of this vector that are not contained in the
     * other vector.
     *
     * @param vector the other vector.
     * @return the difference of the two vectors.
     */
    public RevisionVector difference(RevisionVector vector) {
        List<Revision> diff = newArrayListWithCapacity(revisions.length);
        PeekingIterator<Revision> it = peekingIterator(vector.iterator());
        for (Revision r : revisions) {
            Revision other = peekRevision(it, r.getClusterId());
            if (!r.equals(other)) {
                diff.add(r);
            }
        }
        return new RevisionVector(toArray(diff, Revision.class), false, false);
    }

    /**
     * Returns {@code true} if the given revision is newer than the revision
     * element with the same clusterId in the vector. The given revision is
     * also considered newer if there is no revision element with the same
     * clusterId in this vector.
     *
     * @param revision the revision to check.
     * @return {@code true} if considered newer, {@code false} otherwise.
     */
    public boolean isRevisionNewer(@Nonnull Revision revision) {
        checkNotNull(revision);
        for (Revision r : revisions) {
            if (r.getClusterId() == revision.getClusterId()) {
                return r.compareRevisionTime(revision) < 0;
            } else if (r.getClusterId() > revision.getClusterId()) {
                // revisions are sorted by clusterId ascending
                break;
            }
        }
        return true;
    }

    /**
     * @return {@code true} if any of the revisions in this vector is a branch
     *          revision, {@code false} otherwise.
     */
    public boolean isBranch() {
        for (Revision r : revisions) {
            if (r.isBranch()) {
                return true;
            }
        }
        return false;
    }

    /**
     * @return the first branch revision in this vector.
     * @throws IllegalStateException if this vector does not contain a branch
     *          revision.
     */
    @Nonnull
    public Revision getBranchRevision() {
        for (Revision r : revisions) {
            if (r.isBranch()) {
                return r;
            }
        }
        throw new IllegalStateException(
                "This vector does not contain a branch revision: " + this);
    }

    /**
     * Returns the revision element with the given clusterId or {@code null}
     * if there is no such revision in this vector.
     *
     * @param clusterId a clusterId.
     * @return the revision element with the given clusterId or {@code null}
     *      if none exists.
     */
    public Revision getRevision(int clusterId) {
        for (Revision r : revisions) {
            int cmp = Ints.compare(r.getClusterId(), clusterId);
            if (cmp == 0) {
                return r;
            } else if (cmp > 0) {
                break;
            }
        }
        return null;
    }

    /**
     * Returns a string representation of this revision vector, which can be
     * parsed again by {@link #fromString(String)}.
     *
     * @return a string representation of this revision vector.
     */
    public String asString() {
        int len = revisions.length;
        if (len == 0) {
            return "";
        }
        StringBuilder sb = new StringBuilder(len * Revision.REV_STRING_APPROX_SIZE + len - 1);
        return toStringBuilder(sb).toString();
    }

    /**
     * Appends the string representation of this revision vector to the passed
     * {@code StringBuilder}. The string representation is the same as returned
     * by {@link #asString()}.
     *
     * @param sb the {@code StringBuilder} this revision vector is appended to.
     * @return the passed {@code StringBuilder} object.
     */
    public StringBuilder toStringBuilder(StringBuilder sb) {
        for (int i = 0; i < revisions.length; i++) {
            if (i > 0) {
                sb.append(',');
            }
            revisions[i].toStringBuilder(sb);
        }
        return sb;
    }

    /**
     * Creates a revision vector from a string representation as returned by
     * {@link #asString()}.
     *
     * @param s the string representation of a revision vector.
     * @return the revision vector.
     * @throws IllegalArgumentException if the string is malformed
     */
    public static RevisionVector fromString(String s) {
        String[] list = s.split(",");
        Revision[] revisions = new Revision[list.length];
        for (int i = 0; i < list.length; i++) {
            revisions[i] = Revision.fromString(list[i]);
        }
        return new RevisionVector(revisions);
    }

    /**
     * Returns a revision vector where all revision elements are turned into
     * trunk revisions.
     *
     * @return a trunk representation of this revision vector.
     */
    public RevisionVector asTrunkRevision() {
        if (!isBranch()) {
            return this;
        }
        Revision[] revs = new Revision[revisions.length];
        for (int i = 0; i < revisions.length; i++) {
            revs[i] = revisions[i].asTrunkRevision();
        }
        return new RevisionVector(revs, false, false);
    }

    /**
     * A clone of this revision vector with the revision for the given
     * clusterId set to a branch revision.
     *
     * @param clusterId the clusterId of the revision to be turned into a branch
     *                  revision.
     * @return the revision vector with the branch revision.
     * @throws IllegalArgumentException if there is no revision element with the
     *      given clusterId.
     */
    public RevisionVector asBranchRevision(int clusterId) {
        boolean found = false;
        Revision[] revs = new Revision[revisions.length];
        for (int i = 0; i < revisions.length; i++) {
            Revision r = revisions[i];
            if (r.getClusterId() == clusterId) {
                r = r.asBranchRevision();
                found = true;
            }
            revs[i] = r;
        }
        if (!found) {
            throw new IllegalArgumentException("RevisionVector [" + asString() +
                    "] does not have a revision for clusterId " + clusterId);
        }
        return new RevisionVector(revs, false, false);
    }

    /**
     * Returns the dimensions of this revision vector. That is, the number of
     * revision elements in this vector.
     *
     * @return the number of revision elements in this vector.
     */
    public int getDimensions() {
        return revisions.length;
    }

    //------------------------< CacheValue >------------------------------------

    @Override
    public int getMemory() {
        long size = 32 // shallow size
                      + (long)revisions.length * (Revision.SHALLOW_MEMORY_USAGE + 4);
        if (size > Integer.MAX_VALUE) {
            log.debug("Estimated memory footprint larger than Integer.MAX_VALUE: {}.", size);
            size = Integer.MAX_VALUE;
        }
        return (int) size;
    }

    //------------------------< Comparable >------------------------------------

    @Override
    public int compareTo(@Nonnull RevisionVector other) {
        Iterator<Revision> it = other.iterator();
        for (Revision r : revisions) {
            if (!it.hasNext()) {
                return 1;
            }
            Revision otherRev = it.next();
            int cmp = -Ints.compare(r.getClusterId(), otherRev.getClusterId());
            if (cmp != 0) {
                return cmp;
            }
            cmp = r.compareTo(otherRev);
            if (cmp != 0) {
                return cmp;
            }
        }
        return it.hasNext() ? -1 : 0;
    }

    //-------------------------< Iterable >-------------------------------------

    @Override
    public Iterator<Revision> iterator() {
        return new AbstractIterator<Revision>() {
            int i = 0;
            @Override
            protected Revision computeNext() {
                if (i >= revisions.length) {
                    return endOfData();
                } else {
                    return revisions[i++];
                }
            }
        };
    }

    //--------------------------< Object >--------------------------------------

    @Override
    public String toString() {
        return asString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RevisionVector other = (RevisionVector) o;
        return Arrays.equals(revisions, other.revisions);
    }

    @Override
    public int hashCode() {
        if (hash == 0){
            hash = Arrays.hashCode(revisions);
        }
        return hash;
    }

    //-------------------------< internal >-------------------------------------

    @CheckForNull
    private Revision peekRevision(PeekingIterator<Revision> it,
                                  int minClusterId) {
        while (it.hasNext() && it.peek().getClusterId() < minClusterId) {
            it.next();
        }
        return it.hasNext() ? it.peek() : null;
    }

    private static void checkUniqueClusterIds(Revision[] revisions)
            throws IllegalArgumentException {
        if (revisions.length < 2) {
            return;
        }
        Set<Integer> known = Sets.newHashSetWithExpectedSize(revisions.length);
        for (Revision revision : revisions) {
            if (!known.add(revision.getClusterId())) {
                throw new IllegalArgumentException(
                        "Multiple revisions with clusterId " + revision.getClusterId());
            }
        }
    }

    /**
     * Compares revisions according to their clusterId.
     */
    private static final class RevisionComparator implements Comparator<Revision> {

        private static final Comparator<Revision> INSTANCE = new RevisionComparator();

        @Override
        public int compare(Revision o1, Revision o2) {
            return Ints.compare(o1.getClusterId(), o2.getClusterId());
        }
    }
}
