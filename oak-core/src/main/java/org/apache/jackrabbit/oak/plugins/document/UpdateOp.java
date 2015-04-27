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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.Maps;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A DocumentStore "update" operation for one document.
 */
public final class UpdateOp {

    final String id;

    private boolean isNew;
    private boolean isDelete;

    private final Map<Key, Operation> changes;
    private Map<Key, Condition> conditions;

    /**
     * Create an update operation for the document with the given id. The commit
     * root is assumed to be the path, unless this is changed later on.
     *
     * @param id the primary key
     * @param isNew whether this is a new document
     */
    UpdateOp(String id, boolean isNew) {
        this(id, isNew, false, new HashMap<Key, Operation>());
    }

    private UpdateOp(String id, boolean isNew, boolean isDelete,
                     Map<Key, Operation> changes) {
        this.id = id;
        this.isNew = isNew;
        this.isDelete = isDelete;
        this.changes = changes;
    }

    static UpdateOp combine(String id, Iterable<UpdateOp> ops) {
        Map<Key, Operation> changes = Maps.newHashMap();
        for (UpdateOp op : ops) {
            changes.putAll(op.getChanges());
        }
        return new UpdateOp(id, false, false, changes);
    }

    /**
     * Creates an update operation for the document with the given id. The
     * changes are shared with the this update operation.
     *
     * @param id the primary key.
     */
    public UpdateOp shallowCopy(String id) {
        return new UpdateOp(id, isNew, isDelete, changes);
    }

    /**
     * Creates a deep copy of this update operation. Changes to the returned
     * {@code UpdateOp} do not affect this object.
     *
     * @return a copy of this operation.
     */
    public UpdateOp copy() {
        return new UpdateOp(id, isNew, isDelete,
                new HashMap<Key, Operation>(changes));
    }

    public String getId() {
        return id;
    }

    public boolean isNew() {
        return isNew;
    }

    public void setNew(boolean isNew) {
        this.isNew = isNew;
    }

    void setDelete(boolean isDelete) {
        this.isDelete = isDelete;
    }

    boolean isDelete() {
        return isDelete;
    }

    public Map<Key, Operation> getChanges() {
        return changes;
    }

    public Map<Key, Condition> getConditions() {
        if (conditions == null) {
            return Collections.emptyMap();
        } else {
            return conditions;
        }
    }

    /**
     * Checks if the UpdateOp has any change operation is registered with
     * current update operation
     *
     * @return true if any change operation is created
     */
    public boolean hasChanges() {
        return !changes.isEmpty();
    }

    /**
     * Add a new or update an existing map entry.
     * The property is a map of revisions / values.
     *
     * @param property the property
     * @param revision the revision
     * @param value the value
     */
    void setMapEntry(@Nonnull String property, @Nonnull Revision revision, String value) {
        Operation op = new Operation(Operation.Type.SET_MAP_ENTRY, value);
        changes.put(new Key(property, checkNotNull(revision)), op);
    }

    /**
     * Remove a map entry.
     * The property is a map of revisions / values.
     *
     * @param property the property
     * @param revision the revision
     */
    public void removeMapEntry(@Nonnull String property, @Nonnull Revision revision) {
        Operation op = new Operation(Operation.Type.REMOVE_MAP_ENTRY, null);
        changes.put(new Key(property, checkNotNull(revision)), op);
    }

    /**
     * Set the property to the given value.
     *
     * @param property the property name
     * @param value the value
     */
    void set(String property, Object value) {
        Operation op = new Operation(Operation.Type.SET, value);
        changes.put(new Key(property, null), op);
    }

    /**
     * Set the property to the given value if the new value is higher than the
     * existing value. The property is also set to the given value if the
     * property does not yet exist.
     * <p>
     * The result of a max operation with different types of values is
     * undefined.
     *
     * @param property the name of the property to set.
     * @param value the new value for the property.
     */
    <T> void max(String property, Comparable<T> value) {
        Operation op = new Operation(Operation.Type.MAX, value);
        changes.put(new Key(property, null), op);
    }

    /**
     * Do not set the property entry (after it has been set).
     * The property is a map of revisions / values.
     *
     * @param property the property name
     * @param revision the revision
     */
    void unsetMapEntry(@Nonnull String property, @Nonnull Revision revision) {
        changes.remove(new Key(property, checkNotNull(revision)));
    }

    /**
     * Checks if the named key exists or is absent in the MongoDB document. This
     * method can be used to make a conditional update.
     *
     * @param property the property name
     * @param revision the revision
     */
    void containsMapEntry(@Nonnull String property,
                          @Nonnull Revision revision,
                          boolean exists) {
        if (isNew) {
            throw new IllegalStateException("Cannot use containsMapEntry() on new document");
        }
        Condition c = exists ? Condition.EXISTS : Condition.MISSING;
        getOrCreateConditions().put(new Key(property, checkNotNull(revision)), c);
    }

    /**
     * Checks if the property or map entry is equal to the given value.
     *
     * @param property the name of the property or map.
     * @param revision the revision within the map or {@code null} if this check
     *                 is for a property.
     * @param value the value to compare to.
     */
    void equals(@Nonnull String property,
                @Nullable Revision revision,
                @Nonnull Object value) {
        if (isNew) {
            throw new IllegalStateException("Cannot perform equals check on new document");
        }
        getOrCreateConditions().put(new Key(property, revision),
                Condition.newEqualsCondition(value));
    }

    /**
     * Increment the value.
     *
     * @param property the key
     * @param value the increment
     */
    public void increment(@Nonnull String property, long value) {
        Operation op = new Operation(Operation.Type.INCREMENT, value);
        changes.put(new Key(property, null), op);
    }

    public UpdateOp getReverseOperation() {
        UpdateOp reverse = new UpdateOp(id, isNew);
        for (Entry<Key, Operation> e : changes.entrySet()) {
            Operation r = e.getValue().getReverse();
            if (r != null) {
                reverse.changes.put(e.getKey(), r);
            }
        }
        return reverse;
    }

    @Override
    public String toString() {
        return "key: " + id + " " + (isNew ? "new" : "update") + " " + changes;
    }

    private Map<Key, Condition> getOrCreateConditions() {
        if (conditions == null) {
            conditions = Maps.newHashMap();
        }
        return conditions;
    }

    /**
     * A DocumentStore operation for a given key within a document.
     */
    public static final class Operation {

        /**
         * The DocumentStore operation type.
         */
        public enum Type {

            /**
             * Set the value.
             * The sub-key is not used.
             */
            SET,

            /**
             * Set the value if the new value is higher than the existing value.
             * The new value is also considered higher, when there is no
             * existing value.
             * The sub-key is not used.
             */
            MAX,

            /**
             * Increment the Long value with the provided Long value.
             * The sub-key is not used.
             */
            INCREMENT,

            /**
             * Add the sub-key / value pair.
             * The value in the stored node is a map.
             */
            SET_MAP_ENTRY,

            /**
             * Remove the sub-key / value pair.
             * The value in the stored node is a map.
             */
            REMOVE_MAP_ENTRY

         }


        /**
         * The operation type.
         */
        public final Type type;

        /**
         * The value, if any.
         */
        public final Object value;

        Operation(Type type, Object value) {
            this.type = checkNotNull(type);
            this.value = value;
        }

        @Override
        public String toString() {
            return type + " " + value;
        }

        public Operation getReverse() {
            Operation reverse = null;
            switch (type) {
            case INCREMENT:
                reverse = new Operation(Type.INCREMENT, -(Long) value);
                break;
            case SET:
            case MAX:
            case REMOVE_MAP_ENTRY:
                // nothing to do
                break;
            case SET_MAP_ENTRY:
                reverse = new Operation(Type.REMOVE_MAP_ENTRY, null);
                break;
            }
            return reverse;
        }

    }

    /**
     * A condition to check before an update is applied.
     */
    public static final class Condition {

        /**
         * Check if a sub-key exists in a map.
         */
        public static final Condition EXISTS = new Condition(Type.EXISTS, true);

        /**
         * Check if a sub-key is missing in a map.
         */
        public static final Condition MISSING = new Condition(Type.EXISTS, false);

        public enum Type {

            /**
             * Checks if the sub-key is present in a map or not.
             */
            EXISTS,

            /**
             * Checks if a map entry equals a given value.
             */
            EQUALS

        }

        /**
         * The condition type.
         */
        public final Type type;

        /**
         * The value.
         */
        public final Object value;

        private Condition(Type type, Object value) {
            this.type = type;
            this.value = value;
        }

        /**
         * Creates a new equals condition with the given value.
         *
         * @param value the value to compare to.
         * @return the equals condition.
         */
        public static Condition newEqualsCondition(@Nonnull Object value) {
            return new Condition(Type.EQUALS, checkNotNull(value));
        }

        @Override
        public String toString() {
            return type + " " + value;
        }
    }

    /**
     * A key for an operation consists of a property name and an optional
     * revision. The revision is only set if the value for the operation is
     * set for a certain revision.
     */
    public static final class Key {

        private final String name;
        private final Revision revision;

        public Key(@Nonnull String name, @Nullable Revision revision) {
            this.name = checkNotNull(name);
            this.revision = revision;
        }

        @Nonnull
        public String getName() {
            return name;
        }

        @CheckForNull
        public Revision getRevision() {
            return revision;
        }

        @Override
        public String toString() {
            String s = name;
            if (revision != null) {
                s += "." + revision.toString();
            }
            return s;
        }

        @Override
        public int hashCode() {
            int hash = name.hashCode();
            if (revision != null) {
                hash ^= revision.hashCode();
            }
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Key) {
                Key other = (Key) obj;
                return name.equals(other.name) &&
                        (revision != null ? revision.equals(other.revision) : other.revision == null);
            }
            return false;
        }
    }

}
