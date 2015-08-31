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

import static org.apache.jackrabbit.oak.upgrade.cli.node.Jackrabbit2Factory.isJcr2Repository;
import static org.apache.jackrabbit.oak.upgrade.cli.node.Jackrabbit2Factory.isRepositoryXml;

import org.apache.jackrabbit.oak.upgrade.cli.node.Jackrabbit2Factory;
import org.apache.jackrabbit.oak.upgrade.cli.node.JdbcFactory;
import org.apache.jackrabbit.oak.upgrade.cli.node.MongoFactory;
import org.apache.jackrabbit.oak.upgrade.cli.node.SegmentFactory;
import org.apache.jackrabbit.oak.upgrade.cli.node.StoreFactory;
import org.apache.jackrabbit.oak.upgrade.cli.parser.StoreArguments.MigrationDirection;

enum StoreType {
    JCR2_XML {
        @Override
        public boolean matches(String argument) {
            return isRepositoryXml(argument);
        }

        @Override
        public StoreFactory createFactory(String[] paths, MigrationDirection direction, ArgumentParser arguments) {
            throw new UnsupportedOperationException();
        }
    },
    JCR2_DIR {
        @Override
        public boolean matches(String argument) {
            return isJcr2Repository(argument);
        }

        @Override
        public StoreFactory createFactory(String[] paths, MigrationDirection direction, ArgumentParser arguments) {
            throw new UnsupportedOperationException();
        }
    },
    JCR2_DIR_XML {
        @Override
        public boolean matches(String argument) {
            return false;
        }

        @Override
        public StoreFactory createFactory(String[] paths, MigrationDirection direction, ArgumentParser arguments) {
            return new StoreFactory(new Jackrabbit2Factory(paths[0], paths[1]));
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
                username = arguments.getOption(OptionParserFactory.SRC_USER);
                password = arguments.getOption(OptionParserFactory.SRC_PASSWORD);
            } else {
                username = arguments.getOption(OptionParserFactory.DST_USER);
                password = arguments.getOption(OptionParserFactory.DST_PASSWORD);
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