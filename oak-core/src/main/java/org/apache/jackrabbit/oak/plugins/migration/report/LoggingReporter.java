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
package org.apache.jackrabbit.oak.plugins.migration.report;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.slf4j.Logger;

import javax.annotation.Nonnull;

/**
 * A Reporter implementation that logs every nth node
 * and/or any nth property to the given logger on {@code info}
 * level.
 */
public class LoggingReporter extends PeriodicReporter {

    private final Logger logger;

    private final String verb;

    /**
     * Constructor that allows setting the intervals to log node and property
     * accesses to a given logger.
     *
     * @param logger              The logger to log the progress to.
     * @param nodeLogInterval     Every how many nodes a log message should be written.
     * @param propertyLogInterval Every how many properties a log message should be written.
     */
    public LoggingReporter(final Logger logger, final int nodeLogInterval, final int propertyLogInterval) {
        this(logger, "Loading", nodeLogInterval, propertyLogInterval);
    }

    /**
     * Like {@link #LoggingReporter(Logger, int, int)}, however this constructor allow
     * to customize the verb of the log message.
     * <br>
     * The messages are of the format: "{verb} node #100: /path/to/the/node
     *
     * @param logger              The logger to log the progress to.
     * @param verb                The verb to use for logging.
     * @param nodeLogInterval     Every how many nodes a log message should be written.
     * @param propertyLogInterval Every how many properties a log message should be written.
     */
    public LoggingReporter(final Logger logger, final String verb, final int nodeLogInterval, final int propertyLogInterval) {
        super(nodeLogInterval, propertyLogInterval);
        this.logger = logger;
        this.verb = verb;
    }

    @Override
    protected void reportPeriodicNode(final long count, @Nonnull final ReportingNodeState nodeState) {
        logger.info("{} node #{}: {}", verb, count, nodeState.getPath());
    }

    @Override
    protected void reportPeriodicProperty(final long count, @Nonnull final ReportingNodeState parent, @Nonnull final String propertyName) {
        logger.info("{} properties #{}: {}", verb, count, PathUtils.concat(parent.getPath(), propertyName));
    }
}
