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
package org.apache.jackrabbit.oak.spi.commit;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.spi.state.NodeStateUtils.isHidden;

import javax.annotation.CheckForNull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Editor wrapper that passes only changes to non-hidden nodes and properties
 * (i.e. ones whose names don't start with a colon) to the given delegate
 * editor.
 *
 * @since Oak 0.7
 */
public class VisibleEditor implements Editor {

    @CheckForNull
    public static Editor wrap(@CheckForNull Editor editor) {
        if (editor != null && !(editor instanceof VisibleEditor)) {
            return new VisibleEditor(editor);
        }
        return editor;
    }

    private final Editor editor;

    public VisibleEditor(Editor editor) {
        this.editor = checkNotNull(editor);
    }

    @Override
    public void enter(NodeState before, NodeState after)
            throws CommitFailedException {
        editor.enter(before, after);
    }

    @Override
    public void leave(NodeState before, NodeState after)
            throws CommitFailedException {
        editor.leave(before, after);
    }

    @Override
    public void propertyAdded(PropertyState after) throws CommitFailedException {
        if (!isHidden(after.getName())) {
            editor.propertyAdded(after);
        }
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after)
            throws CommitFailedException {
        if (!isHidden(after.getName())) {
            editor.propertyChanged(before, after);
        }
    }

    @Override
    public void propertyDeleted(PropertyState before)
            throws CommitFailedException {
        if (!isHidden(before.getName())) {
            editor.propertyDeleted(before);
        }
    }

    @Override @CheckForNull
    public Editor childNodeAdded(String name, NodeState after)
            throws CommitFailedException {
        if (!isHidden(name)) {
            return wrap(editor.childNodeAdded(name, after));
        } else {
            return null;
        }
    }

    @Override @CheckForNull
    public Editor childNodeChanged(
            String name, NodeState before, NodeState after)
            throws CommitFailedException {
        if (!isHidden(name)) {
            return wrap(editor.childNodeChanged(name, before, after));
        } else {
            return null;
        }
    }

    @Override @CheckForNull
    public Editor childNodeDeleted(String name, NodeState before)
            throws CommitFailedException {
        if (!isHidden(name)) {
            return wrap(editor.childNodeDeleted(name, before));
        } else {
            return null;
        }
    }

}
