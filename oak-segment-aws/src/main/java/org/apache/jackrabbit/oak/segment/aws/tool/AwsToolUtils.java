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
package org.apache.jackrabbit.oak.segment.aws.tool;

import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.defaultGCOptions;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.annotation.Annotation;
import java.net.URISyntaxException;
import java.text.MessageFormat;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;

import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.segment.aws.AwsContext;
import org.apache.jackrabbit.oak.segment.aws.AwsPersistence;
import org.apache.jackrabbit.oak.segment.aws.Configuration;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.file.tar.TarPersistence;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;

/**
 * Utility class for common stuff pertaining to tooling.
 */
public class AwsToolUtils {

    private AwsToolUtils() {
        // prevent instantiation
    }

    public enum SegmentStoreType {
        TAR("TarMK Segment Store"), AWS("AWS Segment Store");

        private String type;

        SegmentStoreType(String type) {
            this.type = type;
        }

        public String description(String pathOrUri) {
            String location = pathOrUri;
            if (pathOrUri.startsWith("aws:")) {
                location = pathOrUri.substring(3);
            }

            return type + "@" + location;
        }
    }

    public static FileStore newFileStore(SegmentNodeStorePersistence persistence, File directory,
            boolean strictVersionCheck, int segmentCacheSize, long gcLogInterval)
            throws IOException, InvalidFileStoreVersionException, URISyntaxException {
        FileStoreBuilder builder = FileStoreBuilder.fileStoreBuilder(directory)
                .withCustomPersistence(persistence)
                .withMemoryMapping(false)
                .withStrictVersionCheck(strictVersionCheck)
                .withSegmentCacheSize(segmentCacheSize)
                .withGCOptions(defaultGCOptions().setOffline().setGCLogInterval(gcLogInterval));

        return builder.build();
    }

    public static SegmentNodeStorePersistence newSegmentNodeStorePersistence(SegmentStoreType storeType,
            String pathOrUri) throws IOException {
        SegmentNodeStorePersistence persistence = null;

        switch (storeType) {
        case AWS:
            String[] parts = pathOrUri.substring(4).split(";");
            Configuration configuration = new Configuration() {
                @Override
                public Class<? extends Annotation> annotationType() {
                    return null;
                }
            
                @Override
                public String sessionToken() {
                    return null;
                }
            
                @Override
                public String secretKey() {
                    return null;
                }
            
                @Override
                public String rootDirectory() {
                    return parts[1];
                }
            
                @Override
                public String region() {
                    return null;
                }
            
                @Override
                public String lockTableName() {
                    return parts[3];
                }
            
                @Override
                public String journalTableName() {
                    return parts[2];
                }
            
                @Override
                public String bucketName() {
                    return parts[0];
                }
            
                @Override
                public String accessKey() {
                    return null;
                }
            };
            AwsContext awsContext = AwsContext.create(configuration);
            persistence = new AwsPersistence(awsContext);
            break;
        default:
            persistence = new TarPersistence(new File(pathOrUri));
        }

        return persistence;
    }

    public static SegmentStoreType storeTypeFromPathOrUri(String pathOrUri) {
        if (pathOrUri.startsWith("aws:")) {
            return SegmentStoreType.AWS;
        }

        return SegmentStoreType.TAR;
    }

    public static String storeDescription(SegmentStoreType storeType, String pathOrUri) {
        return storeType.description(pathOrUri);
    }

    public static String printableStopwatch(Stopwatch s) {
        return String.format("%s (%ds)", s, s.elapsed(TimeUnit.SECONDS));
    }

    public static void printMessage(PrintWriter pw, String format, Object... arg) {
        pw.println(MessageFormat.format(format, arg));
    }

    public static byte[] fetchByteArray(Buffer buffer) throws IOException {
        byte[] data = new byte[buffer.remaining()];
        buffer.get(data);
        return data;
    }
}
