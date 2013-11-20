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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.util.Text;

/**
 * MoveTracker... TODO
 */
public class MoveTracker {

    private List<MoveEntry> entries = new ArrayList<MoveEntry>();

    private Set<String> parentSourcePaths = new HashSet<String>();
    private Set<String> parentDestPaths = new HashSet<String>();

    /**
     * Create a new {@code MoveTracker}
     */
    public MoveTracker() {
    }

    public void addMove(@Nonnull String sourcePath, @Nonnull String destPath) {
        addMove(sourcePath, destPath, Tree.Status.EXISTING);
    }


    public void addMove(@Nonnull String sourcePath, @Nonnull String destPath, Tree.Status status) {
        // calculate original source path
        String originalSource = sourcePath;
        for (MoveEntry me : Lists.reverse(entries)) {
            if (Text.isDescendantOrEqual(me.destPath, sourcePath)) {
                String relPath = PathUtils.relativize(me.destPath, sourcePath);
                if (!relPath.isEmpty()) {
                    originalSource = me.originalSourcePath + '/' + relPath;
                } else {
                    originalSource = me.originalSourcePath;
                }
                break;
            }
        }

        MoveEntry moveEntry = new MoveEntry(sourcePath, destPath, status, originalSource);
        // TODO: clean up entry list
        entries.add(moveEntry);
    }

    public boolean isEmpty() {
        return entries.isEmpty();
    }

    @CheckForNull
    public String getSourcePath(String destPath) {
        for (MoveEntry me : entries) {
            if (me.destPath.equals(destPath)) {
                return me.sourcePath;
            }
        }
        return null;
    }

    @CheckForNull
    public String getOriginalSourcePath(String destPath) {
        for (MoveEntry me : Lists.reverse(entries)) {
            if (me.destPath.equals(destPath)) {
                return me.originalSourcePath;
            }
        }
        return null;
    }

    @CheckForNull
    public String getDestPath(String originalSource) {
        for (MoveEntry me : Lists.reverse(entries)) {
            if (me.originalSourcePath.equals(originalSource)) {
                return me.destPath;
            }
        }
        return null;
    }

    public boolean containsMove(@CheckForNull String path) {
        if (path != null) {
            for (String p : Iterables.concat(parentSourcePaths, parentDestPaths)) {
                if (Text.isDescendantOrEqual(path, p)) {
                    return true;
                }
            }
        }
        return false;
    }

    public void clear() {
        entries.clear();
        parentSourcePaths.clear();
        parentDestPaths.clear();
    }

    private final class MoveEntry {

        private final String sourcePath;
        private final String destPath;
        private final Tree.Status status;

        private final String originalSourcePath;

        private MoveEntry(@Nonnull String sourcePath, @Nonnull String destPath,
                          @Nonnull Tree.Status status) {
            this(sourcePath, destPath, status, sourcePath);
        }

        private MoveEntry(@Nonnull String sourcePath, @Nonnull String destPath,
                          @Nonnull Tree.Status status,
                          @Nonnull String originalSourcePath) {
            this.sourcePath = sourcePath;
            this.destPath = destPath;
            this.status = status;

            this.originalSourcePath = originalSourcePath;

            parentSourcePaths.add(Text.getRelativeParent(originalSourcePath, 1));
            parentDestPaths.add(Text.getRelativeParent(destPath, 1));
        }
    }
}