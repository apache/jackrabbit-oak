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

import javax.annotation.Nonnull;

/**
 * Options applicable to RDB persistence
 */
public class RDBOptions {

    private boolean dropTablesOnClose = false;
    private String tablePrefix = "";
    private int initialSchema = Integer.getInteger("org.apache.jackrabbit.oak.plugins.document.rdb.RDBOptions.INITIALSCHEMA", 2);
    private int upgradeToSchema = Integer.getInteger("org.apache.jackrabbit.oak.plugins.document.rdb.RDBOptions.UPGRADETOSCHEMA",
            2);

    public RDBOptions() {
    }

    /**
     * Whether to drop the tables on close (in case they have been auto-created)
     */
    public RDBOptions dropTablesOnClose(boolean dropTablesOnClose) {
        this.dropTablesOnClose = dropTablesOnClose;
        return this;
    }

    public boolean isDropTablesOnClose() {
        return this.dropTablesOnClose;
    }

    /**
     * Prefix for table names.
     */
    public RDBOptions tablePrefix(@Nonnull String tablePrefix) {
        this.tablePrefix = tablePrefix;
        return this;
    }

    public @Nonnull String getTablePrefix() {
        return this.tablePrefix;
    }

    /**
     * Control over initial DB schema
     */
    public RDBOptions initialSchema(int initialSchema) {
        this.initialSchema = initialSchema;
        return this;
    }

    public int getInitialSchema() {
        return this.initialSchema;
    }

    /**
     * Control over DB schema to upgrade to
     */
    public RDBOptions upgradeToSchema(int upgradeToSchema) {
        this.upgradeToSchema = upgradeToSchema;
        return this;
    }

    public int getUpgradeToSchema() {
        return this.upgradeToSchema;
    }
}
