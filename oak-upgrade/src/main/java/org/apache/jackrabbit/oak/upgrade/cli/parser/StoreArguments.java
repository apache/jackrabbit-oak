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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.jackrabbit.oak.plugins.segment.SegmentVersion;
import org.apache.jackrabbit.oak.upgrade.cli.node.StoreFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.upgrade.cli.parser.StoreType.JCR2_DIR;
import static org.apache.jackrabbit.oak.upgrade.cli.parser.StoreType.JCR2_DIR_XML;
import static org.apache.jackrabbit.oak.upgrade.cli.parser.StoreType.JCR2_XML;
import static org.apache.jackrabbit.oak.upgrade.cli.parser.StoreType.SEGMENT;
import static org.apache.jackrabbit.oak.upgrade.cli.parser.StoreType.SEGMENT_TAR;
import static org.apache.jackrabbit.oak.upgrade.cli.parser.StoreType.getMatchingType;

public class StoreArguments {

    private static final String DEFAULT_CRX2_REPO = "crx-quickstart/repository";

    private static final String REPOSITORY_XML = "repository.xml";

    public static final String SEGMENT_OLD_PREFIX = "segment-old:";

    private static final Logger log = LoggerFactory.getLogger(StoreArguments.class);

    private final MigrationOptions options;

    private final StoreDescriptor src;

    private final StoreDescriptor dst;

    private Boolean srcHasExternalBlobRefs;

    public StoreArguments(MigrationOptions options, List<String> arguments) throws CliArgumentException {
        this.options = options;
        List<StoreDescriptor> descriptors = createStoreDescriptors(arguments, options);

        src = descriptors.get(0);
        dst = descriptors.get(1);

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

    public StoreFactory getSrcStore() {
        return src.getFactory(MigrationDirection.SRC, options);
    }

    public StoreFactory getDstStore() {
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

    public boolean isInPlaceUpgrade() {
        if (src.getType() == JCR2_DIR_XML && dst.getType() == SEGMENT_TAR) {
            return src.getPath().equals(dst.getPath());
        }
        return false;
    }

    public String[] getSrcPaths() {
        return src.getPaths();
    }

    public boolean srcUsesEmbeddedDatastore() throws IOException {
        if (srcHasExternalBlobRefs == null) {
            srcHasExternalBlobRefs = src.getFactory(StoreArguments.MigrationDirection.SRC, options).hasExternalBlobReferences();
        }
        return !srcHasExternalBlobRefs;
    }

    private static List<StoreDescriptor> createStoreDescriptors(List<String> arguments, MigrationOptions options) throws CliArgumentException {
        List<StoreDescriptor> descriptors = mapToStoreDescriptors(arguments);
        mergeCrx2Descriptors(descriptors);
        addSegmentAsDestination(descriptors);
        validateDescriptors(descriptors, options);
        return descriptors;
    }

    private static List<StoreDescriptor> mapToStoreDescriptors(List<String> arguments) throws CliArgumentException {
        List<StoreDescriptor> descriptors = new ArrayList<StoreDescriptor>();
        boolean jcr2Dir = false;
        boolean jcr2Xml = false;
        for (String argument : arguments) {
            StoreType type = getMatchingType(argument);
            if (type == JCR2_DIR) {
                if (jcr2Dir) {
                    type = SEGMENT_TAR;
                }
                jcr2Dir = true;
            }
            if (type == JCR2_DIR_XML) {
                if (jcr2Xml) {
                    throw new CliArgumentException("Too many repository.xml files passed as arguments", 1);
                }
                jcr2Xml = true;
            }
            descriptors.add(new StoreDescriptor(type, argument));
        }
        return descriptors;
    }

    private static void mergeCrx2Descriptors(List<StoreDescriptor> descriptors) {
        int crx2DirIndex = -1;
        int crx2XmlIndex = -1;
        for (int i = 0; i < descriptors.size(); i++) {
            StoreType type = descriptors.get(i).getType();
            if (type == JCR2_DIR) {
                crx2DirIndex = i;
            } else if (type == JCR2_XML) {
                crx2XmlIndex = i;
            }
        }

        if (crx2DirIndex > -1 || crx2XmlIndex > -1) {
            String repoDir;
            if (crx2DirIndex > -1) {
                repoDir = descriptors.get(crx2DirIndex).getPath();
                descriptors.set(crx2DirIndex, null);
            } else {
                repoDir = DEFAULT_CRX2_REPO;
            }
            String repoXml;
            if (crx2XmlIndex > -1) {
                repoXml = descriptors.get(crx2XmlIndex).getPath();
                descriptors.set(crx2XmlIndex, null);
            } else {
                repoXml = repoDir + "/" + REPOSITORY_XML;
            }
            descriptors.add(0, new StoreDescriptor(JCR2_DIR_XML, repoDir, repoXml));

            Iterator<StoreDescriptor> it = descriptors.iterator();
            while (it.hasNext()) {
                if (it.next() == null) {
                    it.remove();
                }
            }
        }
    }

    private static void addSegmentAsDestination(List<StoreDescriptor> descriptors) {
        if (descriptors.size() == 1) {
            StoreType type = descriptors.get(0).getType();
            if (type == JCR2_DIR_XML) {
                String crx2Dir = descriptors.get(0).getPath();
                descriptors.add(new StoreDescriptor(SEGMENT_TAR, crx2Dir));
                log.info("In place migration between JCR2 and SegmentNodeStore in {}", crx2Dir);
            }
        }
    }

    private static void validateDescriptors(List<StoreDescriptor> descriptors, MigrationOptions options) throws CliArgumentException {
        if (descriptors.size() < 2) {
            throw new CliArgumentException("Not enough node store arguments: " + descriptors.toString(), 1);
        } else if (descriptors.size() > 2) {
            throw new CliArgumentException("Too much node store arguments: " + descriptors.toString(), 1);
        } else if (descriptors.get(1).getType() == JCR2_DIR_XML) {
            throw new CliArgumentException("Can't use CRX2 as a destination", 1);
        }
        StoreDescriptor src = descriptors.get(0);
        StoreDescriptor dst = descriptors.get(1);
        if (src.getType() == dst.getType() && src.getPath().equals(dst.getPath())) {
            throw new CliArgumentException("The source and the destination is the same repository.", 1);
        }
        if (src.getType() == StoreType.JCR2_DIR_XML && options.isSrcBlobStoreDefined()) {
            throw new CliArgumentException("The --src-datastore can't be used for the repository upgrade. Source datastore configuration is placed in the repository.xml file.", 1);
        }
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

        private final String[] paths;

        private final StoreType type;

        public StoreDescriptor(StoreType type, String... paths) {
            this.type = type;
            this.paths = paths;
        }

        public String[] getPaths() {
            return paths;
        }

        public String getPath() {
            return paths[0];
        }

        public StoreType getType() {
            return type;
        }

        public StoreFactory getFactory(MigrationDirection direction, MigrationOptions options) {
            return type.createFactory(paths, direction, options);
        }

        @Override
        public String toString() {
            if (paths.length == 1) {
                return String.format("%s[%s]", type, getPath());
            } else {
                return String.format("%s%s", type, Arrays.toString(getPaths()));
            }
        }

    }
}
