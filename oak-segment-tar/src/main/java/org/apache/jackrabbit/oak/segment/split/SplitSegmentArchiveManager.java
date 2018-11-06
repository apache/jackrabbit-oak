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
package org.apache.jackrabbit.oak.segment.split;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;

import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveReader;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveWriter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class SplitSegmentArchiveManager implements SegmentArchiveManager {

    private final SegmentArchiveManager roArchiveManager;

    private final SegmentArchiveManager rwArchiveManager;

    private final List<String> roArchiveList;

    public SplitSegmentArchiveManager(SegmentArchiveManager roArchiveManager, SegmentArchiveManager rwArchiveManager, String lastRoArchive) throws IOException {
        this.roArchiveManager = roArchiveManager;
        this.rwArchiveManager = rwArchiveManager;
        this.roArchiveList = getRoArchives(lastRoArchive);
    }

    private List<String> getRoArchives(String lastRoArchive) throws IOException {
        List<String> archives = roArchiveManager.listArchives();
        Collections.sort(archives);
        int index = archives.indexOf(lastRoArchive);
        if (index == -1) {
            throw new IllegalStateException("Can't find archive " + lastRoArchive + " in the read-only persistence");
        }
        return new ArrayList<>(archives.subList(0, index + 1));
    }

    @Override
    public @NotNull List<String> listArchives() throws IOException {
        List<String> result = new ArrayList<>();
        result.addAll(roArchiveList);
        result.addAll(rwArchiveManager.listArchives());
        return result;
    }

    @Override
    public @Nullable SegmentArchiveReader open(@NotNull String archiveName) throws IOException {
        if (roArchiveList.contains(archiveName)) {
            SegmentArchiveReader reader = null;
            try {
                reader = roArchiveManager.open(archiveName);
            } catch (IOException e) {
                // ignore
            }
            if (reader == null) {
                reader = roArchiveManager.forceOpen(archiveName);
            }
            return new UnclosedSegmentArchiveReader(reader);
        } else {
            return rwArchiveManager.open(archiveName);
        }
    }

    @Override
    public @Nullable SegmentArchiveReader forceOpen(String archiveName) throws IOException {
        if (roArchiveList.contains(archiveName)) {
            return roArchiveManager.forceOpen(archiveName);
        } else {
            return rwArchiveManager.forceOpen(archiveName);
        }
    }

    @Override
    public @NotNull SegmentArchiveWriter create(@NotNull String archiveName) throws IOException {
        return rwArchiveManager.create(archiveName);
    }

    @Override
    public boolean delete(@NotNull String archiveName) {
        if (roArchiveList.contains(archiveName)) {
            return false;
        } else {
            return rwArchiveManager.delete(archiveName);
        }
    }

    @Override
    public boolean renameTo(@NotNull String from, @NotNull String to) {
        if (roArchiveList.contains(from) || roArchiveList.contains(to)) {
            return false;
        } else {
            return rwArchiveManager.renameTo(from, to);
        }
    }

    @Override
    public void copyFile(@NotNull String from, @NotNull String to) throws IOException {
        if (roArchiveList.contains(to)) {
            throw new IOException("Can't overwrite the read-only " + to);
        } else if (roArchiveList.contains(from)) {
            throw new IOException("Can't copy the archive between persistence " + from + " -> " + to);
        } else {
            rwArchiveManager.copyFile(from, to);
        }
    }

    @Override
    public boolean exists(@NotNull String archiveName) {
        return roArchiveList.contains(archiveName) || rwArchiveManager.exists(archiveName);
    }

    @Override
    public void recoverEntries(@NotNull String archiveName, @NotNull LinkedHashMap<UUID, byte[]> entries) throws IOException {
        if (roArchiveList.contains(archiveName)) {
            roArchiveManager.recoverEntries(archiveName, entries);
        } else {
            rwArchiveManager.recoverEntries(archiveName, entries);
        }
    }

}
