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
package org.apache.jackrabbit.core.util.db;

import javax.sql.DataSource;


/**
 * The connection helper for PSQL databases. It has special fetch size handling.
 */
public final class PostgreSQLConnectionHelper extends ConnectionHelper {

    /**
     * @param dataSrc the {@code DataSource} on which this helper acts
     * @param block whether to block on connection loss until the db is up again
     */
    public PostgreSQLConnectionHelper(DataSource dataSrc, boolean block) {
        super(dataSrc, false, block, 10000);
    }

}
