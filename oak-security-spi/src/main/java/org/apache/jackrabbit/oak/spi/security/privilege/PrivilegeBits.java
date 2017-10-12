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
package org.apache.jackrabbit.oak.spi.security.privilege;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.primitives.Longs;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Internal representation of JCR privileges.
 */
public final class PrivilegeBits implements PrivilegeConstants {

    private static final long NO_PRIVILEGE = 0;
    private static final long READ_NODES = 1;
    private static final long READ_PROPERTIES = READ_NODES << 1;
    private static final long ADD_PROPERTIES = READ_PROPERTIES << 1;
    private static final long ALTER_PROPERTIES = ADD_PROPERTIES << 1;
    private static final long REMOVE_PROPERTIES = ALTER_PROPERTIES << 1;
    private static final long ADD_CHILD_NODES = REMOVE_PROPERTIES << 1;
    private static final long REMOVE_CHILD_NODES = ADD_CHILD_NODES << 1;
    private static final long REMOVE_NODE = REMOVE_CHILD_NODES << 1;
    private static final long READ_AC = REMOVE_NODE << 1;
    private static final long MODIFY_AC = READ_AC << 1;
    private static final long NODE_TYPE_MNGMT = MODIFY_AC << 1;
    private static final long VERSION_MNGMT = NODE_TYPE_MNGMT << 1;
    private static final long LOCK_MNGMT = VERSION_MNGMT << 1;
    private static final long LIFECYCLE_MNGMT = LOCK_MNGMT << 1;
    private static final long RETENTION_MNGMT = LIFECYCLE_MNGMT << 1;
    private static final long WORKSPACE_MNGMT = RETENTION_MNGMT << 1;
    private static final long NODE_TYPE_DEF_MNGMT = WORKSPACE_MNGMT << 1;
    private static final long NAMESPACE_MNGMT = NODE_TYPE_DEF_MNGMT << 1;
    private static final long PRIVILEGE_MNGMT = NAMESPACE_MNGMT << 1;
    private static final long USER_MNGMT = PRIVILEGE_MNGMT << 1;
    private static final long INDEX_DEFINITION_MNGMT = USER_MNGMT << 1;

    private static final long READ = READ_NODES | READ_PROPERTIES;
    private static final long MODIFY_PROPERTIES = ADD_PROPERTIES | ALTER_PROPERTIES | REMOVE_PROPERTIES;
    private static final long WRITE = MODIFY_PROPERTIES | ADD_CHILD_NODES | REMOVE_CHILD_NODES | REMOVE_NODE;
    private static final long WRITE2 = WRITE | NODE_TYPE_MNGMT;

    public static final PrivilegeBits EMPTY = new PrivilegeBits(UnmodifiableData.EMPTY);

    public static final Map<String, PrivilegeBits> BUILT_IN = new HashMap<String, PrivilegeBits>();
    static {
        BUILT_IN.put(REP_READ_NODES, getInstance(READ_NODES));
        BUILT_IN.put(REP_READ_PROPERTIES, getInstance(READ_PROPERTIES));
        BUILT_IN.put(REP_ADD_PROPERTIES, getInstance(ADD_PROPERTIES));
        BUILT_IN.put(REP_ALTER_PROPERTIES, getInstance(ALTER_PROPERTIES));
        BUILT_IN.put(REP_REMOVE_PROPERTIES, getInstance(REMOVE_PROPERTIES));
        BUILT_IN.put(JCR_ADD_CHILD_NODES, getInstance(ADD_CHILD_NODES));
        BUILT_IN.put(JCR_REMOVE_CHILD_NODES, getInstance(REMOVE_CHILD_NODES));
        BUILT_IN.put(JCR_REMOVE_NODE, getInstance(REMOVE_NODE));
        BUILT_IN.put(JCR_READ_ACCESS_CONTROL, getInstance(READ_AC));
        BUILT_IN.put(JCR_MODIFY_ACCESS_CONTROL, getInstance(MODIFY_AC));
        BUILT_IN.put(JCR_NODE_TYPE_MANAGEMENT, getInstance(NODE_TYPE_MNGMT));
        BUILT_IN.put(JCR_VERSION_MANAGEMENT, getInstance(VERSION_MNGMT));
        BUILT_IN.put(JCR_LOCK_MANAGEMENT, getInstance(LOCK_MNGMT));
        BUILT_IN.put(JCR_LIFECYCLE_MANAGEMENT, getInstance(LIFECYCLE_MNGMT));
        BUILT_IN.put(JCR_RETENTION_MANAGEMENT, getInstance(RETENTION_MNGMT));
        BUILT_IN.put(JCR_WORKSPACE_MANAGEMENT, getInstance(WORKSPACE_MNGMT));
        BUILT_IN.put(JCR_NODE_TYPE_DEFINITION_MANAGEMENT, getInstance(NODE_TYPE_DEF_MNGMT));
        BUILT_IN.put(JCR_NAMESPACE_MANAGEMENT, getInstance(NAMESPACE_MNGMT));
        BUILT_IN.put(REP_PRIVILEGE_MANAGEMENT, getInstance(PRIVILEGE_MNGMT));
        BUILT_IN.put(REP_USER_MANAGEMENT, getInstance(USER_MNGMT));
        BUILT_IN.put(REP_INDEX_DEFINITION_MANAGEMENT, getInstance(INDEX_DEFINITION_MNGMT));

        BUILT_IN.put(JCR_READ, PrivilegeBits.getInstance(READ));
        BUILT_IN.put(JCR_MODIFY_PROPERTIES, PrivilegeBits.getInstance(MODIFY_PROPERTIES));
        BUILT_IN.put(JCR_WRITE, PrivilegeBits.getInstance(WRITE));
        BUILT_IN.put(REP_WRITE, PrivilegeBits.getInstance(WRITE2));
    }

    public static final PrivilegeBits NEXT_AFTER_BUILT_INS = getInstance(INDEX_DEFINITION_MNGMT).nextBits();

    private final Data d;

    /**
     * Private constructor.
     *
     * @param d The data that backs this instance.
     */
    private PrivilegeBits(Data d) {
        this.d = d;
    }

    /**
     * Creates a mutable instance of privilege bits.
     *
     * @return a new instance of privilege bits.
     */
    public static PrivilegeBits getInstance() {
        return new PrivilegeBits(new ModifiableData());
    }

    /**
     * Creates a mutable instance of privilege bits.
     *
     * @param base The base for this mutable instance.
     * @return a new instance of privilege bits.
     */
    @Nonnull
    public static PrivilegeBits getInstance(@Nonnull PrivilegeBits... base) {
        PrivilegeBits bts = getInstance();
        for (PrivilegeBits baseBits : base) {
            bts.add(baseBits);
        }
        return bts;
    }

    /**
     * Get or create an instance of privilege bits for a specific property that
     * stores privileges.
     *
     * @param property The property state storing privilege bits information.
     * @return an instance of {@code PrivilegeBits}
     */
    @Nonnull
    public static PrivilegeBits getInstance(@Nullable PropertyState property) {
        if (property == null) {
            return EMPTY;
        }

        int size = property.count();
        if (size == 1) {
            return getInstance(property.getValue(Type.LONG, 0));
        } else {
            long[] longs = new long[size];
            for (int i = 0; i < longs.length; i++) {
                longs[i] = property.getValue(Type.LONG, i);
            }
            return getInstance(longs);
        }
    }

    /**
     * Get or create an instance of privilege bits for a privilege definition.
     *
     * @param tree A privilege definition tree or the privileges root.
     * @return an instance of {@code PrivilegeBits}
     */
    @Nonnull
    public static PrivilegeBits getInstance(@Nullable Tree tree) {
        if (tree == null) {
            return EMPTY;
        }
        String privName = tree.getName();
        if (BUILT_IN.containsKey(privName)) {
            return BUILT_IN.get(privName);
        } else if (REP_PRIVILEGES.equals(privName)) {
            return getInstance(tree.getProperty(REP_NEXT));
        } else {
            return getInstance(tree.getProperty(REP_BITS));
        }
    }

    /**
     * Internal method to get or create an instance of privilege bits for the
     * specified long value.
     *
     * @param bits A long value.
     * @return an instance of {@code PrivilegeBits}
     */
    @Nonnull
    private static PrivilegeBits getInstance(long bits) {
        if (bits == NO_PRIVILEGE) {
            return EMPTY;
        } else {
            checkArgument(bits > NO_PRIVILEGE);
            return new PrivilegeBits(new UnmodifiableData(bits));
        }
    }

    /**
     * Internal method to create a new instance of {@code PrivilegeBits}.
     *
     * @param bits A long array.
     * @return an instance of {@code PrivilegeBits}
     */
    @Nonnull
    private static PrivilegeBits getInstance(long[] bits) {
        return new PrivilegeBits(new UnmodifiableData(bits));
    }

    /**
     * Calculate the granted permissions by evaluating the given privileges. Note,
     * that only built-in privileges can be mapped to permissions. Any other
     * privileges will be ignored.
     *
     * @param bits The set of privileges present at given tree.
     * @param parentBits The privileges present on the parent tree. These are
     * required in order to determine permissions that include a modification
     * of the parent tree (add_child_nodes, remove_child_nodes).
     * @param isAllow {@code true} if the privileges are granted; {@code false}
     * otherwise.
     * @return the resulting permissions.
     */
    public static long calculatePermissions(@Nonnull PrivilegeBits bits,
                                            @Nonnull PrivilegeBits parentBits,
                                            boolean isAllow) {
        long privs = bits.d.longValue();
        long parentPrivs = parentBits.d.longValue();
        long perm = Permissions.NO_PERMISSION;
        if ((privs & READ) == READ) {
            perm |= Permissions.READ;
        } else {
            if ((privs & READ_NODES) == READ_NODES) {
                perm |= Permissions.READ_NODE;
            } else if (((privs & READ_PROPERTIES) == READ_PROPERTIES)) {
                perm |= Permissions.READ_PROPERTY;
            }
        }
        if ((privs & MODIFY_PROPERTIES) == MODIFY_PROPERTIES) {
            perm |= Permissions.SET_PROPERTY;
        } else {
            if ((privs & ADD_PROPERTIES) == ADD_PROPERTIES) {
                perm |= Permissions.ADD_PROPERTY;
            }
            if ((privs & ALTER_PROPERTIES) == ALTER_PROPERTIES) {
                perm |= Permissions.MODIFY_PROPERTY;
            }
            if ((privs & REMOVE_PROPERTIES) == REMOVE_PROPERTIES) {
                perm |= Permissions.REMOVE_PROPERTY;
            }
        }

        // add_node permission is granted through privilege on the parent.
        if ((parentPrivs & ADD_CHILD_NODES) == ADD_CHILD_NODES) {
            perm |= Permissions.ADD_NODE;
        }

        /*
         remove_node is
         allowed: only if remove_child_nodes privilege is present on
                  the parent AND remove_node is present on the node itself
         denied : if either remove_child_nodes is denied on the parent
                  OR remove_node is denied on the node itself.
        */
        if (isAllow) {
            if ((parentPrivs & REMOVE_CHILD_NODES) == REMOVE_CHILD_NODES &&
                    (privs & REMOVE_NODE) == REMOVE_NODE) {
                perm |= Permissions.REMOVE_NODE;
            }
        } else {
            if ((parentPrivs & REMOVE_CHILD_NODES) == REMOVE_CHILD_NODES ||
                    (privs & REMOVE_NODE) == REMOVE_NODE) {
                perm |= Permissions.REMOVE_NODE;
            }
        }

        // modify_child_node_collection permission
        if ((privs & ADD_CHILD_NODES) == ADD_CHILD_NODES &&
                (privs & REMOVE_CHILD_NODES) == REMOVE_CHILD_NODES) {
            perm |= Permissions.MODIFY_CHILD_NODE_COLLECTION;
        }

        // the remaining (special) permissions are simply defined on the node
        if ((privs & READ_AC) == READ_AC) {
            perm |= Permissions.READ_ACCESS_CONTROL;
        }
        if ((privs & MODIFY_AC) == MODIFY_AC) {
            perm |= Permissions.MODIFY_ACCESS_CONTROL;
        }
        if ((privs & LIFECYCLE_MNGMT) == LIFECYCLE_MNGMT) {
            perm |= Permissions.LIFECYCLE_MANAGEMENT;
        }
        if ((privs & LOCK_MNGMT) == LOCK_MNGMT) {
            perm |= Permissions.LOCK_MANAGEMENT;
        }
        if ((privs & NODE_TYPE_MNGMT) == NODE_TYPE_MNGMT) {
            perm |= Permissions.NODE_TYPE_MANAGEMENT;
        }
        if ((privs & RETENTION_MNGMT) == RETENTION_MNGMT) {
            perm |= Permissions.RETENTION_MANAGEMENT;
        }
        if ((privs & VERSION_MNGMT) == VERSION_MNGMT) {
            perm |= Permissions.VERSION_MANAGEMENT;
        }
        if ((privs & WORKSPACE_MNGMT) == WORKSPACE_MNGMT) {
            perm |= Permissions.WORKSPACE_MANAGEMENT;
        }
        if ((privs & NODE_TYPE_DEF_MNGMT) == NODE_TYPE_DEF_MNGMT) {
            perm |= Permissions.NODE_TYPE_DEFINITION_MANAGEMENT;
        }
        if ((privs & NAMESPACE_MNGMT) == NAMESPACE_MNGMT) {
            perm |= Permissions.NAMESPACE_MANAGEMENT;
        }
        if ((privs & PRIVILEGE_MNGMT) == PRIVILEGE_MNGMT) {
            perm |= Permissions.PRIVILEGE_MANAGEMENT;
        }
        if ((privs & USER_MNGMT) == USER_MNGMT) {
            perm |= Permissions.USER_MANAGEMENT;
        }
        if ((privs & INDEX_DEFINITION_MNGMT) == INDEX_DEFINITION_MNGMT) {
            perm |= Permissions.INDEX_DEFINITION_MANAGEMENT;
        }
        return perm;
    }

    /**
     * Returns {@code true} if this privilege bits includes no privileges
     * at all.
     *
     * @return {@code true} if this privilege bits includes no privileges
     *         at all; {@code false} otherwise.
     * @see org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions#NO_PERMISSION
     */
    public boolean isEmpty() {
        return d.isEmpty();
    }

    /**
     * Returns an unmodifiable instance.
     *
     * @return an unmodifiable {@code PrivilegeBits} instance.
     */
    @Nonnull
    public PrivilegeBits unmodifiable() {
        if (d instanceof ModifiableData) {
            if (d.isSimple()) {
                return getInstance(d.longValue());
            } else {
                long[] bits = d.longValues();
                long[] copy = new long[bits.length];
                System.arraycopy(bits, 0, copy, 0, bits.length);
                return getInstance(copy);
            }
        } else {
            return this;
        }
    }

    @Nonnull
    public PrivilegeBits modifiable() {
        if (d instanceof ModifiableData) {
            return this;
        } else {
            return getInstance(this);
        }
    }

    /**
     * Returns {@code true} if all privileges defined by the specified
     * {@code otherBits} are present in this instance.
     *
     * @param otherBits
     * @return {@code true} if all privileges defined by the specified
     *         {@code otherBits} are included in this instance; {@code false}
     *         otherwise.
     */
    public boolean includes(@Nonnull PrivilegeBits otherBits) {
        return d.includes(otherBits.d);
    }

    /**
     * Adds the other privilege bits to this instance.
     *
     * @param other The other privilege bits to be added.
     * @return The updated instance.
     * @throws UnsupportedOperationException if this instance is immutable.
     */
    @Nonnull
    public PrivilegeBits add(@Nonnull PrivilegeBits other) {
        if (d instanceof ModifiableData) {
            ((ModifiableData) d).add(other.d);
            return this;
        } else {
            throw unsupported();
        }
    }

    /**
     * Subtracts the other PrivilegeBits from the this.<br>
     * If the specified bits do not intersect with this, it isn't modified.<br>
     * If {@code this} is included in {@code other} {@link #EMPTY empty}
     * privilege bits is returned.
     *
     * @param other The other privilege bits to be subtracted from this instance.
     * @return The updated instance.
     * @throws UnsupportedOperationException if this instance is immutable.
     */
    @Nonnull
    public PrivilegeBits diff(@Nonnull PrivilegeBits other) {
        if (d instanceof ModifiableData) {
            ((ModifiableData) d).diff(other.d);
            return this;
        } else {
            throw unsupported();
        }
    }

    /**
     * Subtracts the {@code b} from {@code a} and adds the result (diff)
     * to this instance.
     *
     * @param a An instance of privilege bits.
     * @param b An instance of privilege bits.
     * @return The updated instance.
     * @throws UnsupportedOperationException if this instance is immutable.
     */
    @Nonnull
    public PrivilegeBits addDifference(@Nonnull PrivilegeBits a, @Nonnull PrivilegeBits b) {
        if (d instanceof ModifiableData) {
            ((ModifiableData) d).addDifference(a.d, b.d);
            return this;
        } else {
            throw unsupported();
        }
    }

    /**
     * Retains the elements in this {@code PrivilegeBits} that are contained in
     * the specified other {@code PrivilegeBits}.
     *
     * @param other Other privilege bits.
     * @return This modifiable instance of privilege bits modified such it contains
     * only privileges that were also contained in the {@code other} instance.
     */
    @Nonnull
    public PrivilegeBits retain(@Nonnull PrivilegeBits other) {
        if (d instanceof ModifiableData) {
            ((ModifiableData) d).retain(other.d);
            return this;
        }  else {
            throw unsupported();
        }
    }

    @Nonnull
    public PropertyState asPropertyState(String name) {
        return PropertyStates.createProperty(name, Longs.asList(d.longValues()), Type.LONGS);
    }

    /**
     * Method to calculate the next privilege bits associated with this instance.
     *
     * @return an new instance of {@code PrivilegeBits}
     */
    @Nonnull
    public PrivilegeBits nextBits() {
        if (this == EMPTY) {
            return EMPTY;
        } else {
            return new PrivilegeBits(d.next());
        }
    }

    /**
     * Write this instance as property to the specified tree.
     *
     * @param tree The target tree.
     */
    public void writeTo(@Nonnull Tree tree) {
        String name = (REP_PRIVILEGES.equals(tree.getName())) ? REP_NEXT : REP_BITS;
        tree.setProperty(asPropertyState(name));
    }

    private static UnsupportedOperationException unsupported() {
        return new UnsupportedOperationException("immutable privilege bits");
    }

    //-------------------------------------------------------------< Object >---
    @Override
    public int hashCode() {
        return d.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        } else if (o instanceof PrivilegeBits) {
            return d.equals(((PrivilegeBits) o).d);
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("PrivilegeBits: ");
        if (d.isSimple()) {
            sb.append(d.longValue());
        } else {
            sb.append(Arrays.toString(d.longValues()));
        }
        return sb.toString();
    }

    //------------------------------------------------------< inner classes >---

    /**
     * Base class for the internal privilege bits representation and handling.
     */
    private abstract static class Data {

        abstract boolean isEmpty();

        abstract long longValue();

        abstract long[] longValues();

        abstract boolean isSimple();

        abstract Data next();

        abstract boolean includes(Data other);

        /**
         * Checks if all {@code otherBits} is already included in {@code bits}.
         * <p>
         * Truth table:
         * <pre>
         * | b\o | 0 | 1 |
         * |  0  | 1 | 0 |
         * |  1 |  1 | 1 |
         * </pre>
         * @param bits the super set of bits
         * @param otherBits the bits to check against
         * @return {@code true} if all other bits are included in bits.
         */
        static boolean includes(long bits, long otherBits) {
            return (bits | ~otherBits) == -1;
        }

        /**
         * Checks if all {@code otherBits} is already included in {@code bits}.
         * <p>
         * Truth table:
         * <pre>
         * | b\o | 0 | 1 |
         * |  0  | 1 | 0 |
         * |  1 |  1 | 1 |
         * </pre>
         * @param bits the super set of bits
         * @param otherBits the bits to check against
         * @return {@code true} if all other bits are included in bits.
         */
        static boolean includes(long[] bits, long[] otherBits) {
            if (otherBits.length <= bits.length) {
                // test for each long if is included
                for (int i = 0; i < otherBits.length; i++) {
                    if ((bits[i] | ~otherBits[i]) != -1) {
                        return false;
                    }
                }
                return true;
            } else {
                // otherbits array is longer > cannot be included in bits
                return false;
            }
        }
    }

    /**
     * Immutable Data object
     */
    private static final class UnmodifiableData extends Data {

        private static final long MAX = Long.MAX_VALUE / 2;
        private static final UnmodifiableData EMPTY = new UnmodifiableData(NO_PRIVILEGE);

        private final long bits;
        private final long[] bitsArr;
        private final boolean isSimple;

        private UnmodifiableData(long bits) {
            this.bits = bits;
            bitsArr = new long[]{bits};
            isSimple = true;
        }

        private UnmodifiableData(long[] bitsArr) {
            bits = NO_PRIVILEGE;
            this.bitsArr = bitsArr;
            isSimple = false;
        }

        @Override
        boolean isEmpty() {
            return this == EMPTY;
        }

        @Override
        long longValue() {
            return bits;
        }

        @Override
        long[] longValues() {
            return bitsArr;
        }

        @Override
        boolean isSimple() {
            return isSimple;
        }

        @Override
        Data next() {
            if (this == EMPTY) {
                return EMPTY;
            } else if (isSimple) {
                if (bits < MAX) {
                    long b = bits << 1;
                    return new UnmodifiableData(b);
                } else {
                    return new UnmodifiableData(new long[]{bits}).next();
                }
            } else {
                long[] bts;
                long last = bitsArr[bitsArr.length - 1];
                if (last < MAX) {
                    bts = new long[bitsArr.length];
                    System.arraycopy(bitsArr, 0, bts, 0, bitsArr.length);
                    bts[bts.length - 1] = last << 1;
                } else {
                    bts = new long[bitsArr.length + 1];
                    bts[bts.length - 1] = 1;
                }
                return new UnmodifiableData(bts);
            }
        }

        @Override
        boolean includes(Data other) {
            if (isSimple) {
                return (other.isSimple()) && includes(bits, other.longValue());
            } else {
                return includes(bitsArr, other.longValues());
            }
        }

        //---------------------------------------------------------< Object >---
        @Override
        public int hashCode() {
            return (isSimple) ? Long.valueOf(bits).hashCode() : Arrays.hashCode(bitsArr);
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) {
                return true;
            } else if (o instanceof UnmodifiableData) {
                UnmodifiableData d = (UnmodifiableData) o;
                if (isSimple != d.isSimple) {
                    return false;
                }
                if (isSimple) {
                    return bits == d.bits;
                } else {
                    return Arrays.equals(bitsArr, d.bitsArr);
                }
            } else {
                return false;
            }
        }
    }

    /**
     * Mutable implementation of the Data base class.
     */
    private static final class ModifiableData extends Data {

        private long[] bits;

        private ModifiableData() {
            bits = new long[]{NO_PRIVILEGE};
        }

        @Override
        boolean isEmpty() {
            return bits.length == 1 && bits[0] == NO_PRIVILEGE;
        }

        @Override
        long longValue() {
            return (bits.length == 1) ? bits[0] : NO_PRIVILEGE;
        }

        @Override
        long[] longValues() {
            return bits;
        }

        @Override
        boolean isSimple() {
            return bits.length == 1;
        }

        @Override
        Data next() {
            throw new UnsupportedOperationException("Not implemented.");
        }

        @Override
        boolean includes(Data other) {
            if (bits.length == 1) {
                return other.isSimple() && includes(bits[0], other.longValue());
            } else {
                return includes(bits, other.longValues());
            }
        }

        /**
         * Add the other Data to this instance.
         *
         * @param other
         */
        private void add(Data other) {
            if (other != this) {
                if (bits.length == 1 && other.isSimple()) {
                    bits[0] |= other.longValue();
                } else {
                    or(other.longValues());
                }
            }
        }

        /**
         * Subtract the other Data from this instance.
         *
         * @param other
         */
        private void diff(Data other) {
            if (bits.length == 1 && other.isSimple()) {
                bits[0] = bits[0] & ~other.longValue();
            } else {
                bits = diff(bits, other.longValues());
            }
        }

        /**
         * Add the diff between the specified Data a and b.
         *
         * @param a
         * @param b
         */
        private void addDifference(Data a, Data b) {
            if (a.isSimple() && b.isSimple()) {
                bits[0] |= a.longValue() & ~b.longValue();
            } else {
                long[] diff = diff(a.longValues(), b.longValues());
                or(diff);
            }
        }

        private void or(long[] b) {
            if (b.length > bits.length) {
                // enlarge the array
                long[] res = new long[b.length];
                System.arraycopy(bits, 0, res, 0, bits.length);
                bits = res;
            }
            for (int i = 0; i < b.length; i++) {
                bits[i] |= b[i];
            }
        }

        private void retain(Data other) {
            if (isSimple()) {
                bits[0] &= other.longValue();
            } else {
                long[] lvs = longValues();
                long[] bLvs = other.longValues();

                long[] res = (lvs.length <= bLvs.length) ? new long[lvs.length] : new long[bLvs.length];
                int compactSize = -1;
                for (int i = 0; i < res.length; i++) {
                    res[i] = (lvs[i] & bLvs[i]);
                    if (res[i] == 0) {
                        if (compactSize == -1) {
                            compactSize = i+1;
                        }
                    } else {
                        compactSize = -1;
                    }
                }
                if (compactSize != -1 && res.length > compactSize) {
                    bits = Arrays.copyOfRange(res, 0, compactSize);
                } else {
                    bits = res;
                }
            }
        }

        private static long[] diff(long[] a, long[] b) {
            int index = -1;
            long[] res = new long[((a.length > b.length) ? a.length : b.length)];
            for (int i = 0; i < res.length; i++) {
                if (i < a.length && i < b.length) {
                    res[i] = a[i] & ~b[i];
                } else {
                    res[i] = (i < a.length) ? a[i] : 0;
                }
                // remember start of trailing 0 array entries
                if (res[i] != 0) {
                    index = -1;
                } else if (index == -1) {
                    index = i;
                }
            }
            switch (index) {
                case -1:
                    // no need to remove trailing 0-long from the array
                    return res;
                case 0:
                    // array consisting of one or multiple 0
                    return new long[]{NO_PRIVILEGE};
                default:
                    // remove trailing 0-long entries from the array
                    long[] r2 = new long[index];
                    System.arraycopy(res, 0, r2, 0, index);
                    return r2;
            }
        }
    }
}
