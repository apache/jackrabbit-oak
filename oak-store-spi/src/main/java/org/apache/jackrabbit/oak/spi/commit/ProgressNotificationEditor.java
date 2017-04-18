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

package org.apache.jackrabbit.oak.spi.commit;

import static org.apache.jackrabbit.oak.commons.PathUtils.concat;

import javax.annotation.CheckForNull;
import javax.annotation.Nullable;

import com.google.common.base.Function;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;

/**
 * This {@code Editor} instance logs invocations to the logger
 * passed to its constructor after each 10000 calls to it
 * {@code enter()} method.
 */
public class ProgressNotificationEditor implements Editor {
    private final Editor editor;
    private final String path;
    private final Function<String, Void> onProgress;

    @CheckForNull
    public static Editor wrap(@CheckForNull Editor editor, final Logger logger, final String message) {
        if (editor != null && !(editor instanceof ProgressNotificationEditor)) {
            return new ProgressNotificationEditor(editor, "/", new Function<String, Void>() {
                int count;

                @Nullable
                @Override
                public Void apply(String path) {
                    if (++count % 10000 == 0) {
                        logger.info(message + " Traversed #" + count + ' ' + path);
                    }
                    return null;
                }
            });
        }
        return editor;
    }

    private ProgressNotificationEditor(Editor editor, String path, Function<String, Void> onProgress) {
        this.editor = editor;
        this.path = path;
        this.onProgress = onProgress;
    }

    @Override
    public void enter(NodeState before, NodeState after) throws CommitFailedException {
        onProgress.apply(path);
        editor.enter(before, after);
    }

    @Override
    public void leave(NodeState before, NodeState after) throws CommitFailedException {
        editor.leave(before, after);
    }

    @Override
    public void propertyAdded(PropertyState after) throws CommitFailedException {
        editor.propertyAdded(after);
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) throws CommitFailedException {
        editor.propertyChanged(before, after);
    }

    @Override
    public void propertyDeleted(PropertyState before) throws CommitFailedException {
        editor.propertyDeleted(before);
    }

    @Override
    public Editor childNodeAdded(String name, NodeState after) throws CommitFailedException {
        return createChildEditor(editor.childNodeAdded(name, after), name);
    }

    @Override
    public Editor childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
        return createChildEditor(editor.childNodeChanged(name, before, after), name);
    }

    @Override
    public Editor childNodeDeleted(String name, NodeState before) throws CommitFailedException {
        return createChildEditor(editor.childNodeDeleted(name, before), name);
    }

    private ProgressNotificationEditor createChildEditor(Editor editor, String name) {
        if (editor == null) {
            return null;
        } else {
            return new ProgressNotificationEditor(editor, concat(path, name), onProgress);
        }
    }
}
