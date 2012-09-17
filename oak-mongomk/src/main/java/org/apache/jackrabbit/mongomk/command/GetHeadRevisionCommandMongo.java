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
package org.apache.jackrabbit.mongomk.command;

import org.apache.jackrabbit.mongomk.MongoConnection;
import org.apache.jackrabbit.mongomk.api.command.AbstractCommand;
import org.apache.jackrabbit.mongomk.query.FetchHeadRevisionQuery;

/**
 * A {@code Command} for getting the head revision from {@code MongoDB}.
 *
 * @author <a href="mailto:pmarx@adobe.com>Philipp Marx</a>
 */
public class GetHeadRevisionCommandMongo extends AbstractCommand<String> {

    private final MongoConnection mongoConnection;

    /**
     * Constructs a new {@code GetHeadRevisionCommandMongo}.
     *
     * @param mongoConnection The {@link MongoConnection}.
     */
    public GetHeadRevisionCommandMongo(MongoConnection mongoConnection) {
        this.mongoConnection = mongoConnection;
    }

    @Override
    public String execute() throws Exception {
        long headRevision = fetchHeadRevision();
        String revisionId = convertToRevisionId(headRevision);

        return revisionId;
    }

    private String convertToRevisionId(long headRevision) {
        return String.valueOf(headRevision);
    }

    private long fetchHeadRevision() throws Exception {
        return new FetchHeadRevisionQuery(mongoConnection).execute();
    }
}
