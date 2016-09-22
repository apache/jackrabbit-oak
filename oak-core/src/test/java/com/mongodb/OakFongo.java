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
package com.mongodb;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

import com.github.fakemongo.Fongo;
import com.mongodb.connection.ServerVersion;

public class OakFongo extends Fongo {

    private final Map<String, FongoDB> dbMap;

    public OakFongo(String name) throws Exception {
        super(name);
        this.dbMap = getDBMap();
    }

    @Override
    public FongoDB getDB(String dbname) {
        synchronized (dbMap) {
            FongoDB fongoDb = dbMap.get(dbname);
            if (fongoDb == null) {
                try {
                    fongoDb = new OakFongoDB(this, dbname);
                } catch (Exception e) {
                    throw new MongoException(e.getMessage(), e);
                }
                dbMap.put(dbname, fongoDb);
            }
            return fongoDb;
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, FongoDB> getDBMap() throws Exception {
        Field f = Fongo.class.getDeclaredField("dbMap");
        f.setAccessible(true);
        return (Map<String, FongoDB>) f.get(this);
    }

    protected void beforeInsert(List<? extends DBObject> documents,
                                InsertOptions insertOptions) {}

    protected void afterInsert(WriteResult result) {}

    protected void beforeFindAndModify(DBObject query,
                                       DBObject fields,
                                       DBObject sort,
                                       boolean remove,
                                       DBObject update,
                                       boolean returnNew,
                                       boolean upsert) {}

    protected void afterFindAndModify(DBObject result) {}

    protected void beforeUpdate(DBObject q,
                                DBObject o,
                                boolean upsert,
                                boolean multi,
                                WriteConcern concern,
                                DBEncoder encoder) {}

    protected void afterUpdate(WriteResult result) {}

    protected void beforeRemove(DBObject query, WriteConcern writeConcern) {}

    protected void afterRemove(WriteResult result) {}

    protected void beforeExecuteBulkWriteOperation(boolean ordered,
                                                   Boolean bypassDocumentValidation,
                                                   List<?> writeRequests,
                                                   WriteConcern aWriteConcern) {}

    protected void afterExecuteBulkWriteOperation(BulkWriteResult result) {}
    private class OakFongoDB extends FongoDB {

        private final Map<String, FongoDBCollection> collMap;

        public OakFongoDB(Fongo fongo, String name) throws Exception {
            super(fongo, name);
            this.collMap = getCollMap();
        }

        @SuppressWarnings("unchecked")
        private Map<String, FongoDBCollection> getCollMap() throws Exception {
            Field f = FongoDB.class.getDeclaredField("collMap");
            f.setAccessible(true);
            return (Map<String, FongoDBCollection>) f.get(this);
        }

        @Override
        public CommandResult command(DBObject cmd,
                                     ReadPreference readPreference,
                                     DBEncoder encoder) {
            if (cmd.containsField("serverStatus")) {
                CommandResult commandResult = okResult();
                commandResult.append("version", asString(getServerVersion()));
                return commandResult;
            } else {
                return super.command(cmd, readPreference, encoder);
            }
        }

        @Override
        public synchronized FongoDBCollection doGetCollection(String name,
                                                              boolean idIsNotUniq) {
            if (name.startsWith("system.")) {
                return super.doGetCollection(name, idIsNotUniq);
            }
            FongoDBCollection coll = collMap.get(name);
            if (coll == null) {
                coll = new OakFongoDBCollection(this, name, idIsNotUniq);
                collMap.put(name, coll);
            }
            return coll;
        }

        private String asString(ServerVersion serverVersion) {
            StringBuilder sb = new StringBuilder();
            for (int i : serverVersion.getVersionList()) {
                if (sb.length() != 0) {
                    sb.append('.');
                }
                sb.append(String.valueOf(i));
            }
            return sb.toString();
        }
    }

    private class OakFongoDBCollection extends FongoDBCollection {

        public OakFongoDBCollection(FongoDB db,
                                    String name,
                                    boolean idIsNotUniq) {
            super(db, name, idIsNotUniq);
        }

        @Override
        public WriteResult insert(List<? extends DBObject> documents,
                                               InsertOptions insertOptions) {
            beforeInsert(documents, insertOptions);
            WriteResult result = super.insert(documents, insertOptions);
            afterInsert(result);
            return result;
        }

        @Override
        public WriteResult remove(DBObject query, WriteConcern writeConcern) {
            beforeRemove(query, writeConcern);
            WriteResult result = super.remove(query, writeConcern);
            afterRemove(result);
            return result;
        }

        @Override
        public WriteResult update(DBObject q,
                                  DBObject o,
                                  boolean upsert,
                                  boolean multi,
                                  WriteConcern concern,
                                  DBEncoder encoder) {
            beforeUpdate(q, o, upsert, multi, concern, encoder);
            WriteResult result = super.update(q, o, upsert, multi, concern, encoder);
            afterUpdate(result);
            return result;
        }

        @Override
        public DBObject findAndModify(DBObject query,
                                      DBObject fields,
                                      DBObject sort,
                                      boolean remove,
                                      DBObject update,
                                      boolean returnNew,
                                      boolean upsert) {
            beforeFindAndModify(query, fields, sort, remove, update, returnNew, upsert);
            DBObject result = super.findAndModify(query, fields, sort, remove, update, returnNew, upsert);
            afterFindAndModify(result);
            return result;
        }

        @Override
        BulkWriteResult executeBulkWriteOperation(boolean ordered,
                                                  Boolean bypassDocumentValidation,
                                                  List<WriteRequest> writeRequests,
                                                  WriteConcern aWriteConcern) {
            beforeExecuteBulkWriteOperation(ordered, bypassDocumentValidation, writeRequests, aWriteConcern);
            BulkWriteResult result = super.executeBulkWriteOperation(ordered, bypassDocumentValidation, writeRequests, aWriteConcern);
            afterExecuteBulkWriteOperation(result);
            return result;
        }
    }
}
