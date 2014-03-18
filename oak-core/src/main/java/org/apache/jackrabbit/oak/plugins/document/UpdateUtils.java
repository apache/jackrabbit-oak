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

import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import javax.annotation.Nonnull;

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
     * @param comparator
     *            the revision comparator.
     */
    public static void applyChanges(@Nonnull Document doc, @Nonnull UpdateOp update, @Nonnull Comparator<Revision> comparator) {
        for (Entry<Key, Operation> e : checkNotNull(update).getChanges().entrySet()) {
            Key k = e.getKey();
            Operation op = e.getValue();
            switch (op.type) {
                case SET: {
                    doc.put(k.toString(), op.value);
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
                        m = new TreeMap<Revision, Object>(comparator);
                        doc.put(k.getName(), m);
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
                case CONTAINS_MAP_ENTRY:
                    // no effect
                    break;
            }
        }
    }

    public static boolean checkConditions(@Nonnull Document doc, @Nonnull UpdateOp update) {
        for (Map.Entry<Key, Operation> change : update.getChanges().entrySet()) {
            Operation op = change.getValue();
            if (op.type == Operation.Type.CONTAINS_MAP_ENTRY) {
                Key k = change.getKey();
                Revision r = k.getRevision();
                if (r == null) {
                    throw new IllegalStateException("CONTAINS_MAP_ENTRY must not contain null revision");
                }
                Object value = doc.get(k.getName());
                if (value == null) {
                    if (Boolean.TRUE.equals(op.value)) {
                        return false;
                    }
                } else {
                    if (value instanceof Map) {
                        Map<?, ?> map = (Map<?, ?>) value;
                        if (Boolean.TRUE.equals(op.value)) {
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
            }
        }
        return true;
    }
}
