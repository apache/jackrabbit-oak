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
package org.apache.jackrabbit.api.query;

import javax.jcr.query.QueryResult;

/**
 * The Jackrabbit query result interface. This interface contains the
 * Jackrabbit-specific extensions to the JCR {@link QueryResult} interface.
 *
 * @since Jackrabbit 2.6
 */
public interface JackrabbitQueryResult extends QueryResult {

    /**
     * Returns the total number of hits. This is the number of results you
     * would get without any limit or offset settings. This method may return
     * <code>-1</code> if the total size is unknown.
     *
     * @return the total number of hits, or <code>-1</code>
     */
    int getTotalSize();

}
