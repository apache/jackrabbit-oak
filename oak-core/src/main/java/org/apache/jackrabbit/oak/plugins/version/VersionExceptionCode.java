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
package org.apache.jackrabbit.oak.plugins.version;

/**
 * {@code VersionExceptionCode} contains the codes for version related
 * commit failures.
 */
enum VersionExceptionCode {

    UNEXPECTED_REPOSITORY_EXCEPTION("Unexpected RepositoryException"),
    NODE_CHECKED_IN("Node is checked in"),
    NO_SUCH_VERSION("No such Version"),
    OPV_ABORT_ITEM_PRESENT("Item with OPV ABORT action present"),
    NO_VERSION_TO_RESTORE("No suitable version to restore"),
    LABEL_EXISTS("Version label already exists"),
    NO_SUCH_VERSION_LABEL("No such version label"),
    ROOT_VERSION_REMOVAL("Attempt to remove root version");

    private final String desc;

    VersionExceptionCode(String desc) {
        this.desc = desc;
    }

    public String getDescription() {
        return desc;
    }
}