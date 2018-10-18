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

import static org.apache.commons.lang.StringUtils.removeStart;
import static org.apache.jackrabbit.oak.segment.azure.util.AzureConfigurationParserUtils.KEY_ACCOUNT_NAME;
import static org.apache.jackrabbit.oak.segment.azure.util.AzureConfigurationParserUtils.KEY_CONNECTION_STRING;
import static org.apache.jackrabbit.oak.segment.azure.util.AzureConfigurationParserUtils.KEY_CONTAINER_NAME;
import static org.apache.jackrabbit.oak.segment.azure.util.AzureConfigurationParserUtils.KEY_DIR;
import static org.apache.jackrabbit.oak.segment.azure.util.AzureConfigurationParserUtils.KEY_STORAGE_URI;
import static org.apache.jackrabbit.oak.segment.azure.util.AzureConfigurationParserUtils.isCustomAzureConnectionString;
import static org.apache.jackrabbit.oak.segment.azure.util.AzureConfigurationParserUtils.parseAzureConfigurationFromCustomConnection;
import static org.apache.jackrabbit.oak.segment.azure.util.AzureConfigurationParserUtils.parseAzureConfigurationFromUri;
import static org.apache.jackrabbit.oak.upgrade.cli.node.Jackrabbit2Factory.isJcr2Repository;
import static org.apache.jackrabbit.oak.upgrade.cli.node.Jackrabbit2Factory.isRepositoryXml;
import static org.apache.jackrabbit.oak.upgrade.cli.parser.StoreArguments.SEGMENT_AZURE_PREFIX;
import static org.apache.jackrabbit.oak.upgrade.cli.parser.StoreArguments.SEGMENT_OLD_PREFIX;

import java.util.Map;

import org.apache.jackrabbit.oak.upgrade.cli.node.Jackrabbit2Factory;
import org.apache.jackrabbit.oak.upgrade.cli.node.JdbcFactory;
import org.apache.jackrabbit.oak.upgrade.cli.node.MongoFactory;
import org.apache.jackrabbit.oak.upgrade.cli.node.SegmentAzureFactory;
import org.apache.jackrabbit.oak.upgrade.cli.node.SegmentFactory;
import org.apache.jackrabbit.oak.upgrade.cli.node.SegmentTarFactory;
import org.apache.jackrabbit.oak.upgrade.cli.node.StoreFactory;
import org.apache.jackrabbit.oak.upgrade.cli.parser.StoreArguments.MigrationDirection;

public enum StoreType {
    JCR2_XML {
        @Override
        public boolean matches(String argument) {
            return isRepositoryXml(argument);
        }

        @Override
        public StoreFactory createFactory(String[] paths, MigrationDirection direction, MigrationOptions migrationOptions) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isSupportLongNames() {
            return true;
        }
    },
    JCR2_DIR {
        @Override
        public boolean matches(String argument) {
            return isJcr2Repository(argument);
        }

        @Override
        public StoreFactory createFactory(String[] paths, MigrationDirection direction, MigrationOptions migrationOptions) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isSupportLongNames() {
            return true;
        }
    },
    JCR2_DIR_XML {
        @Override
        public boolean matches(String argument) {
            return false;
        }

        @Override
        public StoreFactory createFactory(String[] paths, MigrationDirection direction, MigrationOptions migrationOptions) {
            return new StoreFactory(new Jackrabbit2Factory(paths[0], paths[1]));
        }

        @Override
        public boolean isSupportLongNames() {
            return true;
        }
    },
    JDBC {
        @Override
        public boolean matches(String argument) {
            return argument.startsWith("jdbc:");
        }

        @Override
        public StoreFactory createFactory(String[] paths, MigrationDirection direction, MigrationOptions migrationOptions) {
            String username, password;
            if (direction == MigrationDirection.SRC) {
                username = migrationOptions.getSrcUser();
                password = migrationOptions.getSrcPassword();
            } else {
                username = migrationOptions.getDstUser();
                password = migrationOptions.getDstPassword();
            }
            return new StoreFactory(
                    new JdbcFactory(paths[0], migrationOptions.getCacheSizeInMB(), username, password, direction == MigrationDirection.SRC));
        }

        @Override
        public boolean isSupportLongNames() {
            return false;
        }
    },
    MONGO {
        @Override
        public boolean matches(String argument) {
            return argument.startsWith("mongodb://");
        }

        @Override
        public StoreFactory createFactory(String[] paths, MigrationDirection direction, MigrationOptions migrationOptions) {
            return new StoreFactory(new MongoFactory(paths[0], migrationOptions.getCacheSizeInMB(), direction == MigrationDirection.SRC));
        }

        @Override
        public boolean isSupportLongNames() {
            return false;
        }
    },
    SEGMENT {
        @Override
        public boolean matches(String argument) {
            return argument.startsWith(SEGMENT_OLD_PREFIX);
        }

        @Override
        public StoreFactory createFactory(String[] paths, MigrationDirection direction, MigrationOptions migrationOptions) {
            String path = removeStart(paths[0], SEGMENT_OLD_PREFIX);
            return new StoreFactory(new SegmentFactory(path, migrationOptions.isDisableMmap(), direction == MigrationDirection.SRC));
        }

        @Override
        public boolean isSupportLongNames() {
            return true;
        }
    },
    SEGMENT_AZURE {
        @Override
        public boolean matches(String argument) {
            return argument.startsWith("az:");
        }

        @Override
        public StoreFactory createFactory(String[] paths, MigrationDirection direction, MigrationOptions migrationOptions) {
            String path = removeStart(paths[0], SEGMENT_AZURE_PREFIX);

            if (isCustomAzureConnectionString(path)) {
                // azure configuration specified through connection string
                Map<String, String> config = parseAzureConfigurationFromCustomConnection(path);
                return new StoreFactory(
                        new SegmentAzureFactory.Builder(config.get(KEY_DIR), migrationOptions.getCacheSizeInMB(),
                                direction == MigrationDirection.SRC)
                        .connectionString(config.get(KEY_CONNECTION_STRING))
                        .containerName(config.get(KEY_CONTAINER_NAME))
                        .build()
                );
            } else {
                // azure configuration specified through URI
                Map<String, String> config = parseAzureConfigurationFromUri(path);

                return new StoreFactory(new SegmentAzureFactory.Builder(config.get(KEY_DIR),
                        migrationOptions.getCacheSizeInMB(), direction == MigrationDirection.SRC)
                        .accountName(config.get(KEY_ACCOUNT_NAME))
                        .uri(config.get(KEY_STORAGE_URI))
                        .build()
                );
            }
        }

        @Override
        public boolean isSupportLongNames() {
            return true;
        }
    },
    SEGMENT_TAR {
        @Override
        public boolean matches(String argument) {
            return true;
        }

        @Override
        public StoreFactory createFactory(String[] paths, MigrationDirection direction, MigrationOptions migrationOptions) {
            return new StoreFactory(new SegmentTarFactory(paths[0], migrationOptions.isDisableMmap(),
                    migrationOptions.getCacheSizeInMB(), direction == MigrationDirection.SRC));
        }

        @Override
        public boolean isSupportLongNames() {
            return true;
        }
    };

    public static StoreType getMatchingType(String argument) {
        for (StoreType t : values()) {
            if (t.matches(argument)) {
                return t;
            }
        }
        return SEGMENT_TAR;
    }

    public abstract boolean matches(String argument);

    public abstract StoreFactory createFactory(String[] paths, MigrationDirection direction, MigrationOptions migrationOptions);

    public abstract boolean isSupportLongNames();

    public boolean isSegment() {
        return this == SEGMENT || this == SEGMENT_TAR || this == SEGMENT_AZURE;
    }
}