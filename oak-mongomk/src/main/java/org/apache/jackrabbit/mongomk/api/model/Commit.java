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
package org.apache.jackrabbit.mongomk.api.model;

import java.util.List;

/**
 * A higher level object representing a commit.
 *
 * @author <a href="mailto:pmarx@adobe.com>Philipp Marx</a>
 */
public interface Commit {

    /**
     * Returns the <a href="http://wiki.apache.org/jackrabbit/Jsop">JSOP</a> diff of this commit.
     *
     * @return The {@link String} representing the diff.
     */
    String getDiff();

    /**
     * Returns the {@link List} of {@link Instruction}s which were created from the diff.
     *
     * @see #getDiff()
     *
     * @return The {@link List} of {@link Instruction}s.
     */
    List<Instruction> getInstructions();

    /**
     * Returns the message of the commit.
     *
     * @return The message.
     */
    String getMessage();

    /**
     * Returns the path of the root node of this commit.
     *
     * @return The path of the root node.
     */
    String getPath();

    /**
     * Returns the revision id of this commit if known already, else this will return {@code null}. The revision
     * id will be determined only after the commit has been successfully performed.
     *
     * @see #setRevisionId(String)
     *
     * @return The revision id of this commit or {@code null}.
     */
    String getRevisionId();

    /**
     * Sets the revision id of this commit.
     *
     * @see #getRevisionId()
     *
     * @param revisionId The revision id to set.
     */
    void setRevisionId(String revisionId);


    /**
     * Returns the timestamp of this commit.
     *
     * @return The timestamp of this commit.
     */
    long getTimestamp();
}
