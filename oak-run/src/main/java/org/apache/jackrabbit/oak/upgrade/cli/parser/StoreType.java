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

import static org.apache.jackrabbit.oak.upgrade.cli.node.Crx2Factory.isCrx2Repository;
import static org.apache.jackrabbit.oak.upgrade.cli.node.Crx2Factory.isRepositoryXml;

import org.apache.jackrabbit.oak.upgrade.cli.node.Crx2Factory;
import org.apache.jackrabbit.oak.upgrade.cli.node.JdbcFactory;
import org.apache.jackrabbit.oak.upgrade.cli.node.MongoFactory;
import org.apache.jackrabbit.oak.upgrade.cli.node.SegmentFactory;
import org.apache.jackrabbit.oak.upgrade.cli.node.StoreFactory;
import org.apache.jackrabbit.oak.upgrade.cli.parser.StoreArguments.MigrationDirection;

enum StoreType {
    CRX2_XML {
        @Override
        public boolean matches(String argument) {
            return isRepositoryXml(argument);
        }

        @Override
        public StoreFactory createFactory(String[] paths, MigrationDirection direction, ArgumentParser arguments) {
            throw new UnsupportedOperationException();
        }
    },
    CRX2_DIR {
        @Override
        public boolean matches(String argument) {
            return isCrx2Repository(argument);
        }

        @Override
        public StoreFactory createFactory(String[] paths, MigrationDirection direction, ArgumentParser arguments) {
            throw new UnsupportedOperationException();
        }
    },
    CRX2_DIR_XML {
        @Override
        public boolean matches(String argument) {
            return false;
        }

        @Override
        public StoreFactory createFactory(String[] paths, MigrationDirection direction, ArgumentParser arguments) {
            return new StoreFactory(new Crx2Factory(paths[0], paths[1]));
        }
    },
    JDBC {
        @Override
        public boolean matches(String argument) {
            return argument.startsWith("jdbc:");
        }

        @Override
        public StoreFactory createFactory(String[] paths, MigrationDirection direction, ArgumentParser arguments) {
            final String username, password;
            if (direction == MigrationDirection.SRC) {
                username = arguments.getOption(ArgumentParser.SRC_USER);
                password = arguments.getOption(ArgumentParser.SRC_PASSWORD);
            } else {
                username = arguments.getOption(ArgumentParser.DST_USER);
                password = arguments.getOption(ArgumentParser.DST_PASSWORD);
            }
            return new StoreFactory(
                    new JdbcFactory(paths[0], arguments.getOptions().getCacheSizeInMB(), username, password));
        }
    },
    MONGO {
        @Override
        public boolean matches(String argument) {
            return argument.startsWith("mongodb://");
        }

        @Override
        public StoreFactory createFactory(String[] paths, MigrationDirection direction, ArgumentParser arguments) {
            return new StoreFactory(new MongoFactory(paths[0], arguments.getOptions().getCacheSizeInMB()));
        }
    },
    SEGMENT {
        @Override
        public boolean matches(String argument) {
            return true;
        }

        @Override
        public StoreFactory createFactory(String[] paths, MigrationDirection direction, ArgumentParser arguments) {
            return new StoreFactory(new SegmentFactory(paths[0], arguments.getOptions().isMmap()));
        }
    };

    public static StoreType getMatchingType(String argument) {
        for (StoreType t : values()) {
            if (t.matches(argument)) {
                return t;
            }
        }
        return SEGMENT;
    }

    public abstract boolean matches(String argument);

    public abstract StoreFactory createFactory(String[] paths, MigrationDirection direction, ArgumentParser arguments);
}