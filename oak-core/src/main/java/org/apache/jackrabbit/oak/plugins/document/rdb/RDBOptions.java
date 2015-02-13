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
package org.apache.jackrabbit.oak.plugins.document.rdb;

/**
 * Options applicable to RDB persistence
 */
public class RDBOptions {

    private boolean dropTablesOnClose = false;
    private String tablePrefix = "";

    public RDBOptions() {
    }

    /**
     * Whether to drop the tables on close (in case they have been auto-created)
     */
    public RDBOptions dropTablesOnClose(boolean dropTablesOnClose) {
        this.dropTablesOnClose = dropTablesOnClose;
        return this;
    }

    /**
     * Prefix for table names.
     */
    public RDBOptions tablePrefix(String tablePrefix) {
        this.tablePrefix = tablePrefix;
        return this;
    }

    public String getTablePrefix() {
        return this.tablePrefix;
    }

    public boolean isDropTablesOnClose() {
        return this.dropTablesOnClose;
    }
}
