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

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.plugins.memory.MemoryNodeState.EMPTY_NODE;

/**
 * This commit hook implementation processes changes to be committed
 * using the {@link Editor} instance provided by the {@link EditorProvider}
 * passed to the constructor.
 *
 * @since Oak 0.7
 */
public class EditorHook implements CommitHook {

    private final EditorProvider provider;

    public EditorHook(@Nonnull EditorProvider provider) {
        this.provider = checkNotNull(provider);
    }

    @Override @Nonnull
    public NodeState processCommit(
            @Nonnull NodeState before, @Nonnull NodeState after)
            throws CommitFailedException {
        checkNotNull(before);
        checkNotNull(after);
        NodeBuilder builder = after.builder();
        Editor editor = provider.getRootEditor(before, after, builder);
        CommitFailedException exception = process(editor, before, after);
        if (exception == null) {
            return builder.getNodeState();
        } else {
            throw exception;
        }
    }

    //------------------------------------------------------------< private >---

    /**
     * Validates and possibly edits the given subtree by diffing and recursing through it.
     *
     * @param editor editor for the root of the subtree
     * @param before state of the original subtree
     * @param after state of the modified subtree
     * @return exception if the processing failed, {@code null} otherwise
     */
    @CheckForNull
    private static CommitFailedException process(
            @CheckForNull Editor editor,
            @Nonnull NodeState before, @Nonnull NodeState after) {
        checkNotNull(before);
        checkNotNull(after);
        if (editor != null) {
            EditorDiff diff = new EditorDiff(editor);
            after.compareAgainstBaseState(before, diff);
            return diff.exception;
        } else {
            return null;
        }
    }

    private static class EditorDiff implements NodeStateDiff {

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

        //-------------------------------------------------< NodeStateDiff >--

        @Override
        public void propertyAdded(PropertyState after) {
            if (exception == null) {
                try {
                    editor.propertyAdded(after);
                } catch (CommitFailedException e) {
                    exception = e;
                }
            }
        }

        @Override
        public void propertyChanged(PropertyState before, PropertyState after) {
            if (exception == null) {
                try {
                    editor.propertyChanged(before, after);
                } catch (CommitFailedException e) {
                    exception = e;
                }
            }
        }

        @Override
        public void propertyDeleted(PropertyState before) {
            if (exception == null) {
                try {
                    editor.propertyDeleted(before);
                } catch (CommitFailedException e) {
                    exception = e;
                }
            }
        }

        @Override
        public void childNodeAdded(String name, NodeState after) {
            if (exception == null) {
                try {
                    Editor e = editor.childNodeAdded(name, after);
                    exception = process(e, EMPTY_NODE, after);
                } catch (CommitFailedException e) {
                    exception = e;
                }
            }
        }

        @Override
        public void childNodeChanged(
                String name, NodeState before, NodeState after) {
            if (exception == null) {
                try {
                    Editor e = editor.childNodeChanged(name, before, after);
                    exception = process(e, before, after);
                } catch (CommitFailedException e) {
                    exception = e;
                }
            }
        }

        @Override
        public void childNodeDeleted(String name, NodeState before) {
            if (exception == null) {
                try {
                    Editor e = editor.childNodeDeleted(name, before);
                    exception = process(e, before, EMPTY_NODE);
                } catch (CommitFailedException e) {
                    exception = e;
                }
            }
        }

    }

}
