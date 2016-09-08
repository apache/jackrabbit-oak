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

import org.apache.commons.lang.text.StrSubstitutor;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.upgrade.cli.blob.BlobStoreFactory;
import org.apache.jackrabbit.oak.upgrade.cli.blob.ConstantBlobStoreFactory;
import org.apache.jackrabbit.oak.upgrade.cli.blob.DummyBlobStoreFactory;
import org.apache.jackrabbit.oak.upgrade.cli.blob.FileBlobStoreFactory;
import org.apache.jackrabbit.oak.upgrade.cli.blob.FileDataStoreFactory;
import org.apache.jackrabbit.oak.upgrade.cli.blob.MissingBlobStoreFactory;
import org.apache.jackrabbit.oak.upgrade.cli.blob.S3DataStoreFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;
import static org.apache.jackrabbit.oak.upgrade.cli.parser.OptionParserFactory.COPY_BINARIES;
import static org.apache.jackrabbit.oak.upgrade.cli.parser.OptionParserFactory.DST_FBS;
import static org.apache.jackrabbit.oak.upgrade.cli.parser.OptionParserFactory.DST_FDS;
import static org.apache.jackrabbit.oak.upgrade.cli.parser.OptionParserFactory.DST_S3;
import static org.apache.jackrabbit.oak.upgrade.cli.parser.OptionParserFactory.DST_S3_CONFIG;
import static org.apache.jackrabbit.oak.upgrade.cli.parser.OptionParserFactory.IGNORE_MISSING_BINARIES;
import static org.apache.jackrabbit.oak.upgrade.cli.parser.OptionParserFactory.SRC_FBS;
import static org.apache.jackrabbit.oak.upgrade.cli.parser.OptionParserFactory.SRC_FDS;
import static org.apache.jackrabbit.oak.upgrade.cli.parser.OptionParserFactory.SRC_S3;
import static org.apache.jackrabbit.oak.upgrade.cli.parser.OptionParserFactory.SRC_S3_CONFIG;
import static org.apache.jackrabbit.oak.upgrade.cli.parser.StoreType.JCR2_DIR_XML;

/**
 * This class parses the input provided by the user and analyses the given node stores
 * in order to find out which datastore combination should be used for the migration.
 *
 * The desired outcome for the combinations of user input can be found in the table below.
 * The table is a kind of heuristics that tries to match the user intentions.
 * <pre>
 * For sidegrade:
 || src blobstore defined || src blobs embedded || dst blobstore defined || --copy-binaries || outcome src blobstore || outcome action
 |   -                    |   -                 |  -                     |  -               |  missing               |  copy references¹
 |   -                    |   -                 |  -                     |  +               |  missing               |  (x) not supported
 |   -                    |   -                 |  +                     |  *               |  missing               |  (x) not supported
 |   -                    |   +                 |  -                     |  *               |  embedded              |  copy to embedded
 |   -                    |   +                 |  +                     |  *               |  embedded              |  copy to defined blobstore
 |   +                    |   *                 |  -                     |  -               |  as in src             |  copy references
 |   +                    |   *                 |  -                     |  +               |  as in src             |  copy to embedded
 |   +                    |   *                 |  +                     |  *               |  as in src             |  copy to defined blobstore

 ¹ - (x) not supported for SegmentMK -> MongoMK migration

 For upgrade:

 || dst blobstore defined || --copy-binaries || outcome src blobstore || outcome action
 |  -                     |  -               |  defined by JCR2       |  copy references
 |  -                     |  +               |  defined by JCR2       |  copy to embedded
 |  +                     |  *               |  defined by JCR2       |  copy to defined blobstore
 * </pre>
 */
public class DatastoreArguments {

    private static final Logger log = LoggerFactory.getLogger(DatastoreArguments.class);

    private final BlobStoreFactory definedSrcBlob;

    private final BlobStoreFactory definedDstBlob;

    private final StoreArguments storeArguments;

    private final BlobMigrationCase blobMigrationCase;

    private final MigrationCliArguments parser;

    public DatastoreArguments(MigrationCliArguments parser, StoreArguments storeArguments) throws CliArgumentException {
        this.storeArguments = storeArguments;
        this.parser = parser;

        try {
            blobMigrationCase = discoverBlobMigrationCase();
        } catch (IOException e) {
            log.error("Can't figure out the right blob migration path", e);
            throw new CliArgumentException(1);
        }

        if (blobMigrationCase == BlobMigrationCase.UNSUPPORTED) {
            throw new CliArgumentException("This combination of data- and node-stores is not supported", 1);
        }

        try {
            definedSrcBlob = isSrcBlobStoreDefined() ? getDefinedSrcBlobStore() : null;
            definedDstBlob = isDstBlobStoreDefined() ? getDefinedDstBlobStore() : null;
        } catch(IOException e) {
            log.error("Can't read the blob configuration", e);
            throw new CliArgumentException(1);
        }

        log.info(blobMigrationCase.getDescription(this));
    }

    public BlobStoreFactory getSrcBlobStore() throws IOException {
        BlobStoreFactory result;
        if (isSrcBlobStoreDefined()) {
            result = definedSrcBlob;
        } else if (blobMigrationCase == BlobMigrationCase.COPY_REFERENCES) {
            result = new MissingBlobStoreFactory();
        } else {
            result = new DummyBlobStoreFactory(); // embedded
        }
        log.info("Source blob store: {}", result);
        return result;
    }

    public BlobStoreFactory getDstBlobStore(BlobStore srcBlobStore) throws IOException {
        BlobStoreFactory result;
        if (isDstBlobStoreDefined()) {
            result = definedDstBlob;
        } else if (blobMigrationCase == BlobMigrationCase.COPY_REFERENCES && (isSrcBlobStoreDefined() || storeArguments.getSrcType() == JCR2_DIR_XML)) {
            result = new ConstantBlobStoreFactory(srcBlobStore);
        } else if (blobMigrationCase == BlobMigrationCase.COPY_REFERENCES) {
            result = new MissingBlobStoreFactory();
        } else {
            result = new DummyBlobStoreFactory(); // embedded
        }

        log.info("Destination blob store: {}", result);
        return result;
    }

    public boolean isSrcBlobStoreDefined() {
        return parser.hasOption(SRC_FBS) || (parser.hasOption(SRC_S3_CONFIG) && parser.hasOption(SRC_S3)) || (parser.hasOption(SRC_FDS));
    }

    public boolean isDstBlobStoreDefined() {
        return parser.hasOption(DST_FBS) || (parser.hasOption(DST_S3_CONFIG) && parser.hasOption(DST_S3)) || parser.hasOption(DST_FDS);
    }

    private BlobStoreFactory getDefinedSrcBlobStore() throws IOException {
        boolean ignoreMissingBinaries = parser.hasOption(IGNORE_MISSING_BINARIES);
        if (parser.hasOption(SRC_FBS)) {
            return new FileBlobStoreFactory(parser.getOption(SRC_FBS));
        } else if (parser.hasOption(SRC_S3_CONFIG) && parser.hasOption(SRC_S3)) {
            return new S3DataStoreFactory(parser.getOption(SRC_S3_CONFIG), parser.getOption(SRC_S3), ignoreMissingBinaries);
        } else if (parser.hasOption(SRC_FDS)) {
            return new FileDataStoreFactory(parser.getOption(SRC_FDS), ignoreMissingBinaries);
        } else {
            return null;
        }
    }

    private BlobStoreFactory getDefinedDstBlobStore() throws IOException {
        if (parser.hasOption(DST_FBS)) {
            return new FileBlobStoreFactory(parser.getOption(DST_FBS));
        } else if (parser.hasOption(DST_S3_CONFIG) && parser.hasOption(DST_S3)) {
            return new S3DataStoreFactory(parser.getOption(DST_S3_CONFIG), parser.getOption(DST_S3), false);
        } else if (parser.hasOption(DST_FDS)) {
            return new FileDataStoreFactory(parser.getOption(DST_FDS), false);
        } else {
            return null;
        }
    }

    public enum BlobMigrationCase {
        COPY_REFERENCES("Only blob references will be copied"),
        EMBEDDED_TO_EMBEDDED("Blobs embedded in ${srcnode} will be embedded in ${dstnode}"),
        EMBEDDED_TO_EXTERNAL("Blobs embedded in ${srcnode} will be copied to ${dstblob}"),
        EXTERNAL_TO_EMBEDDED("Blobs stored in ${srcblob} will be embedded in ${dstnode}"),
        EXTERNAL_TO_EXTERNAL("Blobs stored in ${srcblob} will be copied to ${dstblob}"),
        UNSUPPORTED("Unsupported case");

        private final String description;

        BlobMigrationCase(String description) {
            this.description = description;
        }

        private String getDescription(DatastoreArguments datastoreArguments) {
            Map<String, String> map = newHashMap();
            map.put("srcnode", datastoreArguments.storeArguments.getSrcDescriptor());
            map.put("dstnode", datastoreArguments.storeArguments.getDstDescriptor());

            if (datastoreArguments.storeArguments.getSrcType() == JCR2_DIR_XML) {
                map.put("srcblob", "CRX2 datastore");
            } else {
                map.put("srcblob", datastoreArguments.definedSrcBlob == null ? "?" : datastoreArguments.definedSrcBlob.toString());
            }
            map.put("dstblob", datastoreArguments.definedDstBlob == null ? "?" : datastoreArguments.definedDstBlob.toString());

            StrSubstitutor subst = new StrSubstitutor(map);
            return subst.replace(description);
        }

    }

    public BlobMigrationCase getBlobMigrationCase() {
        return blobMigrationCase;
    }

    private BlobMigrationCase discoverBlobMigrationCase() throws IOException {
        boolean srcDefined = isSrcBlobStoreDefined() || storeArguments.getSrcType() == JCR2_DIR_XML;
        boolean dstDefined = isDstBlobStoreDefined();
        boolean srcEmbedded = !storeArguments.srcHasExternalBlobReferences();
        boolean copyBinaries = parser.hasOption(COPY_BINARIES);

        boolean srcSegment = storeArguments.getSrcType().isSegment();
        boolean dstSegment = storeArguments.getDstType().isSegment();

        // default case, no datastore-related arguments given, but blobs are stored externally
        if (!srcDefined && !dstDefined && !srcEmbedded && !copyBinaries) {
            if (srcSegment && !dstSegment) { // segment -> document is not supported for this case
                return BlobMigrationCase.UNSUPPORTED;
            } else { // we try to copy references using MissingBlobStore
                return BlobMigrationCase.COPY_REFERENCES;
            }
            // can't copy binaries if they are stored externally and we don't know where
        } else if (!srcDefined && !dstDefined && !srcEmbedded && copyBinaries) {
            return BlobMigrationCase.UNSUPPORTED;
            // can't copy binaries if they are stored externally and we don't know where
            // (even if the destination datastore is defined)
        } else if (!srcDefined && !srcEmbedded && dstDefined) {
            return BlobMigrationCase.UNSUPPORTED;
            // source is embedded and no destination given
        } else if (!srcDefined && srcEmbedded && !dstDefined) {
            return BlobMigrationCase.EMBEDDED_TO_EMBEDDED;
            // source is embedded and the destination is given
        } else if (!srcDefined && srcEmbedded && dstDefined) {
            return BlobMigrationCase.EMBEDDED_TO_EXTERNAL;
            // source is given, no destination, but also no --copy-binaries -> copy references
        } else if (srcDefined && !dstDefined && !copyBinaries) {
            return BlobMigrationCase.COPY_REFERENCES;
            // source is given, no destination, but --copy-binaries -> copy to embedded
        } else if (srcDefined && !dstDefined && copyBinaries) {
            return BlobMigrationCase.EXTERNAL_TO_EMBEDDED;
            // source and destination is given
        } else if (srcDefined && dstDefined) {
            return BlobMigrationCase.EXTERNAL_TO_EXTERNAL;
        }
        return BlobMigrationCase.UNSUPPORTED;
    }
}
