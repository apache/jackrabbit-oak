/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.spi.commit;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

public class EditorDiff implements NodeStateDiff {

    /**
     * Validates and possibly edits the given subtree by diffing
     * and recursing through it.
     *
     * @param editor editor for the root of the subtree
     * @param before state of the original subtree
     * @param after state of the modified subtree
     * @return exception if the processing failed, {@code null} otherwise
     */
    @CheckForNull
    public static CommitFailedException process(
            @CheckForNull Editor editor,
            @Nonnull NodeState before, @Nonnull NodeState after) {
        checkNotNull(before);
        checkNotNull(after);
        if (editor != null) {
            EditorDiff diff = new EditorDiff(editor);
            return diff.process(before, after);
        } else {
            return null;
        }
    }

    private final Editor editor;

    /**
     * Checked exceptions don't compose. So we need to hack around.
     * See http://markmail.org/message/ak67n5k7mr3vqylm and
     * http://markmail.org/message/bhocbruikljpuhu6
     */
    private CommitFailedException exception;

    private EditorDiff(Editor editor) {
        this.editor = editor;
    }

    private CommitFailedException process(NodeState before, NodeState after) {
        try {
            editor.enter(before, after);
        } catch (CommitFailedException e) {
            return e;
        }

        after.compareAgainstBaseState(before, this);
        if (exception != null) {
            return exception;
        }

        try {
            editor.leave(before, after);
        } catch (CommitFailedException e) {
            return e;
        }

        return null;
    }

    //-------------------------------------------------< NodeStateDiff >--

    @Override
    public boolean propertyAdded(PropertyState after) {
        try {
            editor.propertyAdded(after);
            return true;
        } catch (CommitFailedException e) {
            exception = e;
            return false;
        }
    }

    @Override
    public boolean propertyChanged(PropertyState before, PropertyState after) {
        try {
            editor.propertyChanged(before, after);
            return true;
        } catch (CommitFailedException e) {
            exception = e;
            return false;
        }
    }

    @Override
    public boolean propertyDeleted(PropertyState before) {
        try {
            editor.propertyDeleted(before);
            return true;
        } catch (CommitFailedException e) {
            exception = e;
            return false;
        }
    }

    @Override
    public boolean childNodeAdded(String name, NodeState after) {
        try {
            Editor e = editor.childNodeAdded(name, after);
            exception = process(e, MISSING_NODE, after);
            return exception == null;
        } catch (CommitFailedException e) {
            exception = e;
            return false;
        }
    }

    @Override
    public boolean childNodeChanged(
            String name, NodeState before, NodeState after) {
        try {
            Editor e = editor.childNodeChanged(name, before, after);
            exception = process(e, before, after);
            return exception == null;
        } catch (CommitFailedException e) {
            exception = e;
            return false;
        }
    }

    @Override
    public boolean childNodeDeleted(String name, NodeState before) {
        try {
            Editor e = editor.childNodeDeleted(name, before);
            exception = process(e, before, MISSING_NODE);
            return exception == null;
        } catch (CommitFailedException e) {
            exception = e;
            return false;
        }
    }

}