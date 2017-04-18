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

import static com.google.common.base.Preconditions.checkElementIndex;
import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.CheckForNull;

import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Editor wrapper that passes only changes in the specified subtree to
 * the given delegate editor.
 *
 * @since Oak 0.7
 */
public class SubtreeEditor extends DefaultEditor {

    private final Editor editor;

    private final String[] path;

    private final int depth;

    private SubtreeEditor(Editor editor, String[] path, int depth) {
        this.editor = checkNotNull(editor);
        this.path = checkNotNull(path);
        checkElementIndex(depth, path.length);
        this.depth = depth;
    }

    public SubtreeEditor(Editor editor, String... path) {
        this(editor, path, 0);
    }

    private Editor descend(String name) {
        if (!name.equals(path[depth])) {
            return null;
        } else if (depth + 1 < path.length) {
            return new SubtreeEditor(editor, path, depth + 1);
        } else {
            return editor;
        }
    }

    @Override @CheckForNull
    public Editor childNodeAdded(String name, NodeState after) {
        return descend(name);
    }

    @Override @CheckForNull
    public Editor childNodeChanged(
            String name, NodeState before, NodeState after) {
        return descend(name);
    }

    @Override @CheckForNull
    public Editor childNodeDeleted(String name, NodeState before) {
        return descend(name);
    }

}
