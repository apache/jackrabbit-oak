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
package org.apache.jackrabbit.oak.run;

import joptsimple.OptionSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static java.util.List.of;

/**
 * GenerateFullGCCommand generates garbage nodes in the repository in order to allow for testing fullGC functionality.
 */
public class GenerateFullGCCommand {
    private static final Logger LOG = LoggerFactory.getLogger(GenerateFullGCCommand.class);

    private static final String USAGE = "generateFullGC {<jdbc-uri> | <mongodb-uri>} [options]";

    private static final List<String> LOGGER_NAMES = of(
    );

    private static class GenerateFullGCOptions extends Utils.NodeStoreOptions {

        final OptionSpec<Integer> createGarbageNodesCount;
        final OptionSpec<Integer> garbageNodesParentCount;
        final OptionSpec<Integer> garbageType;
        final OptionSpec<Long> maxRevisionAgeMillis;
        final OptionSpec<Integer> maxRevisionAgeDelaySeconds;
        final OptionSpec<Integer> numberOfRuns;
        final OptionSpec<Integer> generateIntervalSeconds;

        public GenerateFullGCOptions(String usage) {
            super(usage);
            createGarbageNodesCount = parser
                    .accepts("createGarbageNodesCount", "the total number of garbage nodes to create").withRequiredArg()
                    .ofType(Integer.class).defaultsTo(0);
            garbageNodesParentCount = parser
                    .accepts("garbageNodesParentCount", "total number of parent nodes under which to create garbage nodes").withRequiredArg()
                    .ofType(Integer.class).defaultsTo(1);
            garbageType = parser
                    .accepts("garbageType", "garbage type to be generated - must be a value from VersionGarbageCollector.fullGCMode").withRequiredArg()
                    .ofType(Integer.class).defaultsTo(1);
            maxRevisionAgeMillis = parser
                    .accepts("maxRevisionAgeMillis", "the time in the past that the revision must have. This should be identical to the maxRevisionAge " +
                            "used by the fullGC (24h right now) when collecting garbage").withRequiredArg()
                    .ofType(Long.class).defaultsTo(1036800L);
            maxRevisionAgeDelaySeconds = parser
                    .accepts("garbageType", "the time subtracted from  to the timestampAge when generating the timestamps for the garbage. " +
                            "This is in order for the garbage to be collected after a delay after insertion, not right away").withRequiredArg()
                    .ofType(Integer.class).defaultsTo(60);
            numberOfRuns = parser
                    .accepts("numberOfRuns", "the number of garbage generation runs to do. Only applies if greater than 1, " +
                            "otherwise a single run will be done.").withRequiredArg()
                    .ofType(Integer.class).defaultsTo(1);
            generateIntervalSeconds = parser
                    .accepts("generateIntervalSeconds", "the interval at which to generate a complete garbage count from createGarbageNotesCount. " +
                            "Applies only if numberOfRuns is greater than 1.").withRequiredArg()
                    .ofType(Integer.class).defaultsTo(60);

        }

        public GenerateFullGCOptions parse(String[] args) {
            super.parse(args);
            return this;
        }

        public int getCreateGarbageNodesCount() {
            return createGarbageNodesCount.value(options);
        }

        public int getGarbageNodesParentCount() {
            return garbageNodesParentCount.value(options);
        }

        public int getGarbageType() {
            return garbageType.value(options);
        }

        public long getMaxRevisionAgeMillis() {
            return maxRevisionAgeMillis.value(options);
        }

        public int getMaxRevisionAgeDelaySeconds() {
            return maxRevisionAgeDelaySeconds.value(options);
        }

        public int getNumberOfRuns() {
            return numberOfRuns.value(options);
        }

        public int getGenerateIntervalSeconds() {
            return generateIntervalSeconds.value(options);
        }
    }
}

