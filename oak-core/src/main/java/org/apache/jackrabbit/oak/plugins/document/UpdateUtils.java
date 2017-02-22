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

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import javax.annotation.Nonnull;

import com.google.common.base.Objects;

import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Condition;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Key;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Operation;

/**
 * Provides convenience methods for applying {@link UpdateOp}s to
 * {@link Document}s.
 */
public class UpdateUtils {

    /**
     * Apply the changes to the in-memory document.
     *
     * @param doc
     *            the target document.
     * @param update
     *            the changes to apply.
     */
    public static void applyChanges(@Nonnull Document doc,
                                    @Nonnull UpdateOp update) {
        doc.put(Document.ID, update.getId());
        for (Entry<Key, Operation> e : checkNotNull(update).getChanges().entrySet()) {
            Key k = e.getKey();
            Operation op = e.getValue();
            switch (op.type) {
                case SET: {
                    doc.put(k.toString(), op.value);
                    break;
                }
                case REMOVE: {
                    doc.remove(k.toString());
                    break;
                }
                case MAX: {
                    Comparable newValue = (Comparable) op.value;
                    Object old = doc.get(k.toString());
                    //noinspection unchecked
                    if (old == null || newValue.compareTo(old) > 0) {
                        doc.put(k.toString(), op.value);
                    }
                    break;
                }
                case INCREMENT: {
                    Object old = doc.get(k.toString());
                    Long x = (Long) op.value;
                    if (old == null) {
                        old = 0L;
                    }
                    doc.put(k.toString(), ((Long) old) + x);
                    break;
                }
                case SET_MAP_ENTRY: {
                    Object old = doc.get(k.getName());
                    @SuppressWarnings("unchecked")
                    Map<Revision, Object> m = (Map<Revision, Object>) old;
                    if (m == null) {
                        m = new TreeMap<Revision, Object>(StableRevisionComparator.REVERSE);
                        doc.put(k.getName(), m);
                    }
                    if (k.getRevision() == null) {
                        throw new IllegalArgumentException("Cannot set map entry " + k.getName() + " with null revision");
                    }
                    m.put(k.getRevision(), op.value);
                    break;
                }
                case REMOVE_MAP_ENTRY: {
                    Object old = doc.get(k.getName());
                    @SuppressWarnings("unchecked")
                    Map<Revision, Object> m = (Map<Revision, Object>) old;
                    if (m != null) {
                        m.remove(k.getRevision());
                    }
                    break;
                }
            }
        }
    }

    public static boolean checkConditions(@Nonnull Document doc,
                                          @Nonnull Map<Key, Condition> conditions) {
        for (Map.Entry<Key, Condition> entry : conditions.entrySet()) {
            Condition c = entry.getValue();
            Key k = entry.getKey();
            Object value = doc.get(k.getName());
            Revision r = k.getRevision();
            if (c.type == Condition.Type.EXISTS) {
                if (r == null) {
                    throw new IllegalStateException("EXISTS must not contain null revision");
                }
                if (value == null) {
                    if (Boolean.TRUE.equals(c.value)) {
                        return false;
                    }
                } else {
                    if (value instanceof Map) {
                        Map<?, ?> map = (Map<?, ?>) value;
                        if (Boolean.TRUE.equals(c.value)) {
                            if (!map.containsKey(r)) {
                                return false;
                            }
                        } else {
                            if (map.containsKey(r)) {
                                return false;
                            }
                        }
                    } else {
                        return false;
                    }
                }
            } else if (c.type == Condition.Type.EQUALS || c.type == Condition.Type.NOTEQUALS) {
                if (r != null) {
                    if (value instanceof Map) {
                        value = ((Map) value).get(r);
                    } else {
                        value = null;
                    }
                }
                boolean equal = Objects.equal(value, c.value);
                if (c.type == Condition.Type.EQUALS && !equal) {
                    return false;
                } else if (c.type == Condition.Type.NOTEQUALS && equal) {
                    return false;
                }
            } else {
                throw new IllegalArgumentException("Unknown condition: " + c.type);
            }
        }
        return true;
    }

    /**
     * Ensures that the given {@link UpdateOp} is unconditional
     * @param up the update operation
     * @throws IllegalArgumentException when the operations is conditional
     */
    public static void assertUnconditional(@Nonnull UpdateOp up) {
        Map<Key, Condition> conditions = up.getConditions();
        if (!conditions.isEmpty()) {
            throw new IllegalArgumentException(
                    "This DocumentStore method does not support conditional updates, but the UpdateOp contained: " + conditions);
        }
    }
}
