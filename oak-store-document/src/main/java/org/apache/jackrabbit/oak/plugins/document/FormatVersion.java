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

import java.util.List;

import javax.annotation.Nonnull;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.plugins.document.Collection.SETTINGS;

/**
 * The format version currently in use by the DocumentNodeStore and written
 * to the underlying DocumentStore. A version {@link #canRead} the current or
 * older versions.
 */
public final class FormatVersion implements Comparable<FormatVersion> {

    /**
     * A dummy version when none is available.
     */
    static final FormatVersion V0 = new FormatVersion(0, 0, 0);

    /**
     * Format version for Oak 1.0.
     */
    static final FormatVersion V1_0 = new FormatVersion(1, 0, 0);

    /**
     * Format version for Oak 1.2.
     * <p>
     * Changes introduced with this version:
     * <ul>
     *     <li>_lastRev entries are only updated for implicit changes (OAK-2131)</li>
     * </ul>
     */
    static final FormatVersion V1_2 = new FormatVersion(1, 2, 0);

    /**
     * Format version for Oak 1.4.
     * <p>
     * Changes introduced with this version:
     * <ul>
     *     <li>journalGC in settings collection (OAK-4528)</li>
     *     <li>startTime in clusterNode entries, revision vector in checkpoint (OAK-3646)</li>
     *     <li>discovery lite with clusterView in settings collection (OAK-2844)</li>
     * </ul>
     */
    static final FormatVersion V1_4 = new FormatVersion(1, 4, 0);

    /**
     * Format version for Oak 1.6.
     * <p>
     * Changes introduced with this version:
     * <ul>
     *     <li>bundle nodes into document (OAK-1312)</li>
     *     <li>journal entries with change set summary for JCR observation (OAK-5101)</li>
     * </ul>
     */
    static final FormatVersion V1_6 = new FormatVersion(1, 6, 0);

    /**
     * Format version for Oak 1.8.
     * <p>
     * Changes introduced with this version:
     * <ul>
     *     <li>SplitDocType.DEFAULT_NO_BRANCH (OAK-5869)</li>
     *     <li>journal entries with invalidate-only changes (OAK-5964)</li>
     * </ul>
     */
    static final FormatVersion V1_8 = new FormatVersion(1, 8, 0);

    /**
     * The ID of the document in the settings collection that contains the
     * version information.
     */
    private static final String VERSION_ID = "version";

    /**
     * @return well known format versions.
     */
    public static Iterable<FormatVersion> values() {
        return ImmutableList.of(V0, V1_0, V1_2, V1_4, V1_6, V1_8);
    }

    /**
     * Name of the version property.
     */
    private static final String PROP_VERSION = "_v";

    private final int major, minor, micro;

    private FormatVersion(int major, int minor, int micro) {
        this.major = major;
        this.minor = minor;
        this.micro = micro;
    }

    /**
     * Returns {@code true} if {@code this} version can read data written by the
     * {@code other} version.
     *
     * @param other the version the data was written in.
     * @return {@code true} if this version can read, {@code false} otherwise.
     */
    public boolean canRead(FormatVersion other) {
        return compareTo(checkNotNull(other)) >= 0;
    }

    /**
     * Reads the {@link FormatVersion} from the given store. This method
     * returns {@link FormatVersion#V0} if the store currently does not have a
     * version set.
     *
     * @param store the store to read from.
     * @return the format version of the store.
     * @throws DocumentStoreException if an error occurs while reading from the
     *          store.
     */
    @Nonnull
    public static FormatVersion versionOf(@Nonnull DocumentStore store)
            throws DocumentStoreException {
        checkNotNull(store);
        FormatVersion v = V0;
        Document d = store.find(SETTINGS, VERSION_ID);
        if (d != null) {
            Object p = d.get(PROP_VERSION);
            if (p != null) {
                try {
                    v = valueOf(p.toString());
                } catch (IllegalArgumentException e) {
                    throw new DocumentStoreException(e);
                }
            }
        }
        return v;
    }

    /**
     * Writes this version to the given document store. The write operation will
     * fail with a {@link DocumentStoreException} if the version change is
     * considered incompatible or cannot be applied for some other reason. This
     * includes:
     * <ul>
     *     <li>An attempt to downgrade the existing version</li>
     *     <li>There are active cluster nodes using an existing version</li>
     *     <li>The version was changed concurrently</li>
     * </ul>
     *
     * @param store the document store.
     * @return {@code true} if the version in the store was updated,
     *      {@code false} otherwise. This method will also return {@code false}
     *      if the version in the store equals this version and now update was
     *      required.
     * @throws DocumentStoreException if the write operation fails. Reasons
     *      include: 1) an attempt to downgrade the existing version, 2) there
     *      are active cluster nodes using an existing version, 3) the version
     *      was changed concurrently.
     */
    public boolean writeTo(@Nonnull DocumentStore store)
            throws DocumentStoreException {
        checkNotNull(store);
        FormatVersion v = versionOf(store);
        if (v == this) {
            // already on this version
            return false;
        }
        if (!canRead(v)) {
            // never downgrade
            throw unableToWrite("Version " + this + " cannot read " + v);
        }
        List<Integer> active = Lists.newArrayList();
        for (ClusterNodeInfoDocument d : ClusterNodeInfoDocument.all(store)) {
            if (d.isActive()) {
                active.add(d.getClusterId());
            }
        }
        if (!active.isEmpty() && v != V0) {
            throw unableToWrite("There are active cluster nodes: " + active);
        }
        if (v == V0) {
            UpdateOp op = new UpdateOp(VERSION_ID, true);
            op.set(PROP_VERSION, toString());
            if (!store.create(SETTINGS, Lists.newArrayList(op))) {
                throw concurrentUpdate();
            }
        } else {
            UpdateOp op = new UpdateOp(VERSION_ID, false);
            op.equals(PROP_VERSION, v.toString());
            op.set(PROP_VERSION, toString());
            if (store.findAndUpdate(SETTINGS, op) == null) {
                throw concurrentUpdate();
            }
        }
        return true;
    }

    /**
     * Returns a format version for the given String representation. This method
     * either returns one of the well known versions or an entirely new version
     * if the version is not well known.
     *
     * @param s the String representation of a format version.
     * @return the parsed format version.
     * @throws IllegalArgumentException if the string is malformed.
     */
    public static FormatVersion valueOf(String s)
            throws IllegalArgumentException {
        String[] parts = s.split("\\.");
        if (parts.length != 3) {
            throw new IllegalArgumentException(s);
        }
        int[] elements = new int[parts.length];
        for (int i = 0; i < parts.length; i++) {
            try {
                elements[i] = Integer.parseInt(parts[i]);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(s);
            }
        }
        FormatVersion v = new FormatVersion(elements[0], elements[1], elements[2]);
        for (FormatVersion known : values()) {
            if (v.equals(known)) {
                v = known;
                break;
            }
        }
        return v;
    }

    @Override
    public String toString() {
        return major + "." + minor + "." + micro;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof FormatVersion
                && compareTo((FormatVersion) obj) == 0;
    }

    @Override
    public int compareTo(@Nonnull FormatVersion other) {
        checkNotNull(other);
        return ComparisonChain.start()
                .compare(major, other.major)
                .compare(minor, other.minor)
                .compare(micro, other.micro)
                .result();
    }

    private static DocumentStoreException concurrentUpdate() {
        return unableToWrite("Version was updated concurrently");
    }

    private static DocumentStoreException unableToWrite(String reason) {
        return new DocumentStoreException(
                "Unable to write format version. " + reason);
    }
}
