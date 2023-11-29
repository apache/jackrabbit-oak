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
package org.apache.jackrabbit.oak.api.jmx;

import org.jetbrains.annotations.NotNull;
import org.osgi.annotation.versioning.ProviderType;

@ProviderType
public interface QueryEngineSettingsMBean {
    String TYPE = "QueryEngineSettings";
    
    /**
     * Get the limit on how many nodes a query may read at most into memory, for
     * "order by" and "distinct" queries. If this limit is exceeded, the query
     * throws an exception.
     * 
     * @return the limit
     */
    @Description("Get the limit on how many nodes a query may read at most into memory, for " +
            "\"order by\" and \"distinct\" queries. If this limit is exceeded, the query throws an exception.")    
    long getLimitInMemory();
    
    /**
     * Change the limit.
     * 
     * @param limitInMemory the new limit
     */
    void setLimitInMemory(long limitInMemory);
    
    /**
     * Get the limit on how many nodes a query may read at most (raw read
     * operations, including skipped nodes). If this limit is exceeded, the
     * query throws an exception.
     * 
     * @return the limit
     */
    @Description("Get the limit on how many nodes a query may read at most (raw read " +
            "operations, including skipped nodes). If this limit is exceeded, the " +
            "query throws an exception.")    
    long getLimitReads();
    
    /**
     * Change the limit.
     * 
     * @param limitReads the new limit
     */
    void setLimitReads(long limitReads);

    /**
     * Change the prefetch count.
     *
     * @param prefetchCount the new count
     */
    void setPrefetchCount(int prefetchCount);

    /**
     * Get the prefetch count.
     *
     * @return the count
     */
    @Description("Get the prefetch count. This is the number of entries pre-fetched from the node store at a time.")
    int getPrefetchCount();

    /**
     * Change the automatic query options mapping.
     *
     * @param json the new mapping, in Json format
     */
    void setAutoOptionsMappingJson(String json);

    /**
     * Get the automatic query options mapping.
     *
     * @return the mapping, in Json format
     */
    @Description("Get the automatic query options mapping, in Json format.")
    String getAutoOptionsMappingJson();

    /**
     * Whether queries that don't use an index will fail (throw an exception).
     * The default is false.
     * 
     * @return true if they fail
     */
    @Description("Whether queries that don't use an index will fail (throw an exception). " +
            "The default is false.")    
    boolean getFailTraversal();

    /**
     * Set whether queries that don't use an index will fail (throw an exception).
     * 
     * @param failTraversal the new value for this setting
     */
    void setFailTraversal(boolean failTraversal);

    /**
     * Whether the query result size should return an estimation for large queries.
     *
     * @return true if enabled
     */
    @Description("Whether the query result size should return an estimation for large queries.")    
    boolean isFastQuerySize();

    void setFastQuerySize(boolean fastQuerySize);

    /**
     * Whether Path restrictions are enabled while figuring out index plan
     *
     * @return true if enabled
     */
    String getStrictPathRestriction();

    /**
     *  Whether path restrictions of indexes (excludedPaths / includedPaths) are taken into account during query execution,
     *  for Lucene indexes. When enabled, only indexes are considered if the index path restriction is compatible with the
     *  query path restrictions. When disabled, only the queryPaths of the index is taken into account.
     *
     * @param pathRestriction Set path restriction: Expected value is either of ENABLE/DISABLE/WARN
     *                        ENABLE: enable path restriction- Index won't be used if index definition path restrictions are not compatible with query's path restriction
     *                        DISABLE: path restrictions are not taken into account while querying
     *                        WARN: path restrictions are not taken into account but a warning will be logged if query path restrictions are not compatible with index path restrictions 
     */
     @Description("Set path restriction: Expected value is either of ENABLE/DISABLE/WARN.   " +
                    "ENABLE: enable path restriction- Index won't be used if index definition path restrictions are not compatible with query's path restriction.  " +
                    "DISABLE: path restrictions are not taken into account while querying.  " +
                    "WARN: path restrictions are not taken into account but a warning will be logged if query path restrictions are not compatible with index path restrictions."
                    )
    void setStrictPathRestriction(
            @Name("pathRestriction")
                    String pathRestriction);

    /**
     * Set or remove a query validator pattern.
     *
     * @param key the key
     * @param pattern the regular expression pattern (empty to remove the
     *            pattern)
     * @param comment a comment
     * @param failQuery whether matching queries should fail (true) or just log
     *            a warning (false)
     */
    @Description("Set or remove a query validator pattern.")
    void setQueryValidatorPattern(
            @Description("the key")
            @Name("key")
            String key,
            @Description("the regular expression pattern (empty to remove the pattern)")
            @Name("pattern")
            String pattern,
            @Description("a comment")
            @Name("comment")
            String comment,
            @Description("whether matching queries should fail (true) or just log a warning (false)")
            @Name("failQuery")
            boolean failQuery);

    @Description("Get the query validator data as a JSON string.")
    String getQueryValidatorJson();

    /**
     * Set or remove java package/class names which are ignored from finding the 
     * invoking class for queries.
     * 
     * It can be either Java package names or fully-qualified class names (package + class name).
     * 
     * @param classNames the class names to be ignored.
     */
    @Description("Set or remove Java package / fully qualified class names to ignore in Call Trace analysis")
    void setIgnoredClassNamesInCallTrace(
            @Description("package or fully qualified class names")
            @Name("class names")
            @NotNull String[] classNames);
    
    // @Description("Get the Java package / fully qualified class names to ignore when finding the caller of query")
    @NotNull
    String[] getIgnoredClassNamesInCallTrace();
}
