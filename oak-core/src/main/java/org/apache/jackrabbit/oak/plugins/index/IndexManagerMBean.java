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
package org.apache.jackrabbit.oak.plugins.index;

import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.TabularData;

import org.apache.jackrabbit.oak.api.jmx.Description;
import org.apache.jackrabbit.oak.api.jmx.Name;

/**
 * Manages index definitions: disables, enables and deletes them, and gives read
 * access to the audit trail of index definition changes (which is stored in a
 * hidden node and therefore not visible with the JCR API). The operations that
 * change index definitions are recorded in the audit trail, with the given caller
 * as the user. They are only available if the audit is enabled.
 *
 * @see <a href="https://issues.apache.org/jira/browse/OAK-12458">OAK-12458</a>
 */
public interface IndexManagerMBean {

    String TYPE = "IndexManager";

    @Description("All indexes with an audit trail, sorted by path.")
    TabularData getIndexes() throws OpenDataException;

    @Description("Audit trail of an index (also of a removed index), newest entry first.")
    TabularData getAuditTrail(
            @Name("indexPath")
            @Description("Index path, e.g. /oak:index/damAssetLucene-12")
            String indexPath) throws OpenDataException;

    @Description("Disables an index: sets the type to 'disabled' and keeps the previous type in 'originalType'. "
            + "Does nothing if the index is already disabled.")
    String disableIndex(
            @Name("indexPath")
            @Description("Index path, e.g. /oak:index/damAssetLucene-12")
            String indexPath,
            @Name("by")
            @Description("Who disables the index, e.g. a user id (optional; default: the JMX caller if known, else 'unknown')")
            String by,
            @Name("reason")
            @Description("Why the index is disabled (optional)")
            String reason,
            @Name("force")
            @Description("Also disable a synchronous index (without async lane, e.g. uuid, nodetype)")
            boolean force);

    @Description("Enables a disabled index: restores the type kept in 'originalType', removes the "
            + "disable metadata, and reindexes it (a disabled index is not updated, and its data may have "
            + "been removed). A synchronous index is reindexed by this call.")
    String enableIndex(
            @Name("indexPath")
            @Description("Index path, e.g. /oak:index/damAssetLucene-12")
            String indexPath,
            @Name("by")
            @Description("Who enables the index, e.g. a user id (optional; default: the JMX caller if known, else 'unknown')")
            String by,
            @Name("force")
            @Description("Also enable (and so reindex in this call) a synchronous index (without async lane)")
            boolean force);

    @Description("Deletes a disabled index, including its index data. The index data is deleted in batches, "
            + "so that indexes of any size can be deleted. Fails if the index is enabled meanwhile. "
            + "Remote index data (e.g. Elasticsearch) is not deleted.")
    String deleteIndex(
            @Name("indexPath")
            @Description("Index path, e.g. /oak:index/damAssetLucene-12")
            String indexPath,
            @Name("by")
            @Description("Who deletes the index, e.g. a user id (optional; default: the JMX caller if known, else 'unknown')")
            String by,
            @Name("reason")
            @Description("Why the index is deleted (optional)")
            String reason,
            @Name("force")
            @Description("Also delete a synchronous index (without async lane)")
            boolean force);
}
