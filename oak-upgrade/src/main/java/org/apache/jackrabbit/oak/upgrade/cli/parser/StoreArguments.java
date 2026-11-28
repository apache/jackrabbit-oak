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
package org.apache.jackrabbit.oak.upgrade.cli.parser;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.jackrabbit.oak.plugins.segment.SegmentVersion;
import org.apache.jackrabbit.oak.upgrade.cli.node.NodeStoreFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.upgrade.cli.parser.StoreType.SEGMENT;
import static org.apache.jackrabbit.oak.upgrade.cli.parser.StoreType.SEGMENT_TAR;
import static org.apache.jackrabbit.oak.upgrade.cli.parser.StoreType.getMatchingType;

public class StoreArguments {

    private static final String CLASSIC_MIGRATION_GUIDANCE =
            "For Jackrabbit Classic migration, use oak-upgrade from Oak 2.8.0.";

    public static final String SEGMENT_OLD_PREFIX = "segment-old:";

    public static final String SEGMENT_AZURE_PREFIX = "az:";

    private static final Logger log = LoggerFactory.getLogger(StoreArguments.class);

    private final MigrationOptions options;

    private final StoreDescriptor src;

    private final StoreDescriptor dst;

    private Boolean srcHasExternalBlobRefs;

    public StoreArguments(MigrationOptions options, List<String> arguments) throws CliArgumentException {
        if (arguments.size() != 2) {
            throw new CliArgumentException("Expected exactly two Oak node store arguments: source and destination. "
                    + CLASSIC_MIGRATION_GUIDANCE, 1);
        }
        this.options = options;
        src = new StoreDescriptor(arguments.get(0));
        dst = new StoreDescriptor(arguments.get(1));
        validateDescriptors(src, dst);

        if (options.getSrcExternalBlobs() != null) {
            srcHasExternalBlobRefs = options.getSrcExternalBlobs();
        }
    }

    public void logOptions() {
        log.info("Source: {}", src);
        log.info("Destination: {}", dst);

        if (dst.getType() == SEGMENT) {
            logSegmentVersion();
        }
    }

    public NodeStoreFactory getSrcStore() {
        return src.getFactory(MigrationDirection.SRC, options);
    }

    public NodeStoreFactory getDstStore() {
        return dst.getFactory(MigrationDirection.DST, options);
    }

    public StoreType getSrcType() {
        return src.getType();
    }

    public StoreType getDstType() {
        return dst.getType();
    }

    String getSrcDescriptor() {
        return src.toString();
    }

    String getDstDescriptor() {
        return dst.toString();
    }

    public boolean srcUsesEmbeddedDatastore() throws IOException {
        if (srcHasExternalBlobRefs == null) {
            srcHasExternalBlobRefs = src.getFactory(StoreArguments.MigrationDirection.SRC, options).hasExternalBlobReferences();
        }
        return !srcHasExternalBlobRefs;
    }

    /**
     * Check local paths before opening either store. An Oak journal takes precedence
     * over leftover Classic files, but a new destination does not need a journal.
     */
    private static void validateDescriptors(StoreDescriptor src, StoreDescriptor dst) throws CliArgumentException {
        if (src.getType() == dst.getType() && src.path.equals(dst.path)) {
            throw new CliArgumentException("The source and the destination is the same repository.", 1);
        }

        File source = src.getLocalPath();
        File destination = dst.getLocalPath();
        if ((source != null && source.isFile()) || (destination != null && destination.isFile())) {
            throw new CliArgumentException("Use repository directories, not files, for local Oak stores. "
                    + CLASSIC_MIGRATION_GUIDANCE, 1);
        }
        if (source != null) {
            validateLocalSource(source);
        }
        if (destination != null && !hasOakSegmentStore(destination) && hasClassicMarkers(destination)) {
            throw new CliArgumentException("Jackrabbit Classic destinations are not supported. "
                    + CLASSIC_MIGRATION_GUIDANCE, 1);
        }
    }

    private static void validateLocalSource(File source) throws CliArgumentException {
        if (hasOakSegmentStore(source)) {
            return;
        }
        if (hasClassicMarkers(source)) {
            throw new CliArgumentException("Jackrabbit Classic sources are not supported. "
                    + CLASSIC_MIGRATION_GUIDANCE, 1);
        }
        throw new CliArgumentException("The source does not contain an Oak segment store: expected segmentstore/journal.log.", 1);
    }

    private static boolean hasOakSegmentStore(File home) {
        return new File(home, "segmentstore/journal.log").isFile();
    }

    private static boolean hasClassicMarkers(File home) {
        return new File(home, "repository.xml").isFile() || new File(home, "workspaces").isDirectory();
    }

    private static void logSegmentVersion() {
        SegmentVersion[] versions = SegmentVersion.values();
        SegmentVersion lastVersion = versions[versions.length - 1];
        log.info("Using Oak segment format {} - please make sure your version of AEM supports that format",
                lastVersion);
        if (lastVersion == SegmentVersion.V_11) {
            log.info("Requires Oak 1.0.12, 1.1.7 or later");
        }
    }

    enum MigrationDirection {
        SRC, DST
    }

    private static class StoreDescriptor {

        private final String path;

        private final StoreType type;

        StoreDescriptor(String path) {
            this.path = path;
            this.type = getMatchingType(path);
        }

        public StoreType getType() {
            return type;
        }

        File getLocalPath() {
            if (type == SEGMENT_TAR) {
                return new File(path);
            } else if (type == SEGMENT) {
                return new File(path.substring(SEGMENT_OLD_PREFIX.length()));
            }
            return null;
        }

        public NodeStoreFactory getFactory(MigrationDirection direction, MigrationOptions options) {
            return type.createFactory(path, direction, options);
        }

        @Override
        public String toString() {
            return String.format("%s[%s]", type, path);
        }

    }
}
