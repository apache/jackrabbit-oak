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

package org.apache.jackrabbit.oak.plugins.commit;

import static org.apache.jackrabbit.JcrConstants.JCR_LASTMODIFIED;
import static org.apache.jackrabbit.JcrConstants.JCR_CREATED;
import static org.apache.jackrabbit.util.ISO8601.parse;

import java.util.Calendar;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Conflict Handler that merges concurrent updates to
 * {@code org.apache.jackrabbit.JcrConstants.JCR_LASTMODIFIED} by picking the
 * older of the 2 conflicting dates and
 * {@code org.apache.jackrabbit.JcrConstants.JCR_CREATED} by picking the newer
 * of the 2 conflicting dates.
 */
public class JcrLastModifiedConflictHandler extends DefaultThreeWayConflictHandler {

    public JcrLastModifiedConflictHandler() {
        super(Resolution.IGNORED);
    }

    @NotNull
    @Override
    public Resolution addExistingProperty(@NotNull NodeBuilder parent, @NotNull PropertyState ours, @NotNull PropertyState theirs) {
        if (isModifiedOrCreated(ours.getName()) && merge(parent, ours, theirs)) {
            return Resolution.MERGED;
        } else {
            return Resolution.IGNORED;
        }
    }

    @NotNull
    @Override
    public Resolution changeChangedProperty(@NotNull NodeBuilder parent, @NotNull PropertyState ours, @NotNull PropertyState theirs,
                                            @NotNull PropertyState base) {
        if (isModifiedOrCreated(ours.getName()) && merge(parent, ours, theirs)) {
            return Resolution.MERGED;
        } else {
            return Resolution.IGNORED;
        }
    }

    /**
     * Tries to merge two properties. The respective property of the parent is
     * set if merging is successful. The the earlier value is used if
     * jcr:created is true; the later is used if it is not jcr:created.
     *
     * @param parent the parent node
     * @param ours our value
     * @param theirs their value
     * @return if merging is successful
     */
    private static boolean merge(@NotNull NodeBuilder parent, @NotNull PropertyState ours, @NotNull PropertyState theirs) {
        Calendar o = parse(ours.getValue(Type.DATE));
        Calendar t = parse(theirs.getValue(Type.DATE));
        Calendar value = pick(o, t, JCR_CREATED.equals(ours.getName()));
        if (value != null) {
            parent.setProperty(ours.getName(), value);
            return true;
        } else {
            return false;
        }
    }

    /**
     * Pick "a" or "b", depending on which one is earlier (if jcrCreated = true) or later (if jcrCreated = false).
     *
     * @param ours the property state from our change set to have a conflict resolved.
     * @param theirs the property state from their change set to have a conflict resolved.
     * @param jcrCreated if true and 'ours' is before 'theirs', 'ours' is returned, otherwise 'theirs'
     * @return the calendar; either "ours" or "theirs". It will return {@code null} if both are {@code null}
     */
    @Nullable
    private static Calendar pick(@Nullable Calendar ours, @Nullable Calendar theirs, boolean jcrCreated) {
        if (ours == null) {
            return theirs;
        }
        if (theirs == null) {
            return ours;
        }
        if (ours.before(theirs)) {
            return jcrCreated ? ours : theirs;
        } else {
            return jcrCreated ? theirs : ours;
        }
    }

    private static boolean isModifiedOrCreated(@NotNull String name) {
        return JCR_LASTMODIFIED.equals(name) || JCR_CREATED.equals(name);
    }
}
