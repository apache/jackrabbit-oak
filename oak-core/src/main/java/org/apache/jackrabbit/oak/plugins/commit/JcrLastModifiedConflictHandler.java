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

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;

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

    @Nonnull
    @Override
    public Resolution addExistingProperty(NodeBuilder parent, PropertyState ours, PropertyState theirs) {
        if (isModifiedOrCreated(ours.getName())) {
            merge(parent, ours, theirs);
            return Resolution.MERGED;
        }
        return Resolution.IGNORED;
    }

    @Nonnull
    @Override
    public Resolution changeChangedProperty(NodeBuilder parent, PropertyState ours, PropertyState theirs,
            PropertyState base) {
        if (isModifiedOrCreated(ours.getName())) {
            merge(parent, ours, theirs);
            return Resolution.MERGED;
        }
        return Resolution.IGNORED;
    }

    private static void merge(NodeBuilder parent, PropertyState ours, PropertyState theirs) {
        Calendar o = parse(ours.getValue(Type.DATE));
        Calendar t = parse(theirs.getValue(Type.DATE));
        if (JCR_CREATED.equals(ours.getName())) {
            parent.setProperty(ours.getName(), pick(o, t, true));
        } else {
            parent.setProperty(ours.getName(), pick(o, t, false));
        }
    }

    private static Calendar pick(Calendar a, Calendar b, boolean jcrCreated) {
        if (a.before(b)) {
            return jcrCreated ? a : b;
        } else {
            return jcrCreated ? b : a;
        }
    }

    private static boolean isModifiedOrCreated(String name) {
        return JCR_LASTMODIFIED.equals(name) || JCR_CREATED.equals(name);
    }
}
