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
package org.apache.jackrabbit.oak.upgrade;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.slf4j.Logger;

public class LoggingEqualsDiff implements NodeStateDiff {

    private final Logger log;

    private final String path;

    private boolean pathDisplayed;

    public LoggingEqualsDiff(Logger log, String path) {
        this.log = log;
        this.path = path;
    }

    @Override
    public boolean propertyAdded(PropertyState after) {
        displayPath();
        log.info("  + {}<{}>", after.getName(), after.getType());
        return false;
    }

    @Override
    public boolean propertyChanged(PropertyState before, PropertyState after) {
        displayPath();
        log.info("  ^ {}<{}>", before.getName(), before.getType());
        return false;
    }

    @Override
    public boolean propertyDeleted(PropertyState before) {
        displayPath();
        log.info("  - {}<{}>", before.getName(), before.getType());
        return false;
    }

    @Override
    public boolean childNodeAdded(String name, NodeState after) {
        String childPath = PathUtils.concat(path, name);
        log.info("+ {}", childPath);
        return false;
    }

    @Override
    public boolean childNodeChanged(String name, NodeState before, NodeState after) {
        String childPath = PathUtils.concat(path, name);
        return after.compareAgainstBaseState(before, new LoggingEqualsDiff(log, childPath));
    }

    @Override
    public boolean childNodeDeleted(String name, NodeState before) {
        String childPath = PathUtils.concat(path, name);
        log.info("- {}", childPath);
        return false;
    }

    private void displayPath() {
        if (!pathDisplayed) {
            log.info("^ {}", path);
            pathDisplayed = true;
        }
    }
}
