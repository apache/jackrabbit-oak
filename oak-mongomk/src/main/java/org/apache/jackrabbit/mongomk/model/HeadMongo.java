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
package org.apache.jackrabbit.mongomk.model;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * The {@code MongoDB} representation of the head revision.
 *
 * @author <a href="mailto:pmarx@adobe.com>Philipp Marx</a>
 */
@SuppressWarnings("javadoc")
public class HeadMongo extends BasicDBObject {

    public static final String KEY_HEAD_REVISION_ID = "headRevId";
    public static final String KEY_NEXT_REVISION_ID = "nextRevId";
    private static final long serialVersionUID = 3541425042129003691L;

    public static HeadMongo fromDBObject(DBObject dbObject) {
        HeadMongo headMongo = new HeadMongo();
        headMongo.putAll(dbObject);

        return headMongo;
    }

    public long getHeadRevisionId() {
        return getLong(KEY_HEAD_REVISION_ID);
    }

    public long getNextRevisionId() {
        return getLong(KEY_NEXT_REVISION_ID);
    }

    public void setHeadRevisionId(long revisionId) {
        put(KEY_HEAD_REVISION_ID, revisionId);
    }

    public void setNextRevisionId(long revisionId) {
        put(KEY_NEXT_REVISION_ID, revisionId);
    }
}
