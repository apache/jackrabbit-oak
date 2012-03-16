/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.mk.persistence;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.util.Iterator;

import org.apache.jackrabbit.mk.blobs.BlobStore;
import org.apache.jackrabbit.mk.fs.FilePath;
import org.apache.jackrabbit.mk.model.ChildNodeEntriesMap;
import org.apache.jackrabbit.mk.model.Commit;
import org.apache.jackrabbit.mk.model.Id;
import org.apache.jackrabbit.mk.model.Node;
import org.apache.jackrabbit.mk.model.StoredCommit;
import org.apache.jackrabbit.mk.store.BinaryBinding;
import org.apache.jackrabbit.mk.store.Binding;
import org.apache.jackrabbit.mk.store.IdFactory;
import org.apache.jackrabbit.mk.store.NotFoundException;
import org.apache.jackrabbit.mk.util.ExceptionFactory;
import org.apache.jackrabbit.mk.util.IOUtils;
import org.apache.jackrabbit.mk.util.StringUtils;
import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;

/**
 *
 */
public class MongoPersistence implements Persistence, BlobStore {

    private static final boolean BINARY_FORMAT = false;

    private static final String HEAD_COLLECTION = "head";
    private static final String NODES_COLLECTION = "nodes";
    private static final String COMMITS_COLLECTION = "commits";
    private static final String CNEMAPS_COLLECTION = "cneMaps";
    private static final String ID_FIELD = ":id";
    private static final String DATA_FIELD = ":data";

    private Mongo con;
    private DB db;
    private DBCollection nodes;
    private DBCollection commits;
    private DBCollection cneMaps;
    private GridFS fs;

    // TODO: make this configurable
    private IdFactory idFactory = IdFactory.getDigestFactory();
    
    public void initialize(File homeDir) throws Exception {
        con = new Mongo();
        //con = new Mongo("localhost", 27017);

        db = con.getDB("mk");
        db.setWriteConcern(WriteConcern.SAFE);

        if (!db.collectionExists(HEAD_COLLECTION)) {
            // capped collection of size 1
            db.createCollection(HEAD_COLLECTION, new BasicDBObject("capped", true).append("size", 256).append("max", 1));
        }

        nodes = db.getCollection(NODES_COLLECTION);
        nodes.ensureIndex(
                new BasicDBObject(ID_FIELD, 1),
                new BasicDBObject("unique", true));

        commits = db.getCollection(COMMITS_COLLECTION);
        commits.ensureIndex(
                new BasicDBObject(ID_FIELD, 1),
                new BasicDBObject("unique", true));

        cneMaps = db.getCollection(CNEMAPS_COLLECTION);
        cneMaps.ensureIndex(
                new BasicDBObject(ID_FIELD, 1),
                new BasicDBObject("unique", true));

        fs = new GridFS(db);
    }

    public void close() {
        con.close();
        con = null;
        db = null;
    }

    public Id readHead() throws Exception {
        DBObject entry = db.getCollection(HEAD_COLLECTION).findOne();
        if (entry == null) {
            return null;
        }
        return new Id((byte[]) entry.get(ID_FIELD));
    }

    public void writeHead(Id id) throws Exception {
        // capped collection of size 1
        db.getCollection(HEAD_COLLECTION).insert(new BasicDBObject(ID_FIELD, id.getBytes()));
    }

    public Binding readNodeBinding(Id id) throws NotFoundException, Exception {
        BasicDBObject key = new BasicDBObject();
        if (BINARY_FORMAT) {
            key.put(ID_FIELD, id.getBytes());
        } else {
            key.put(ID_FIELD, id.toString());
        }
        final BasicDBObject nodeObject = (BasicDBObject) nodes.findOne(key);
        if (nodeObject != null) {
            if (BINARY_FORMAT) {
                byte[] bytes = (byte[]) nodeObject.get(DATA_FIELD);
                return new BinaryBinding(new ByteArrayInputStream(bytes));
            } else {
                return new DBObjectBinding(nodeObject);
            }
        } else {
            throw new NotFoundException(id.toString());
        }
    }
    
    public Id writeNode(Node node) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        node.serialize(new BinaryBinding(out));
        byte[] bytes = out.toByteArray();
        Id id = new Id(idFactory.createContentId(bytes));

        BasicDBObject nodeObject;
        if (BINARY_FORMAT) {
            nodeObject = new BasicDBObject(ID_FIELD, id.getBytes()).append(DATA_FIELD, bytes);
        } else {
            nodeObject = new BasicDBObject(ID_FIELD, id.toString());
            node.serialize(new DBObjectBinding(nodeObject));
        }
        try {
            nodes.insert(nodeObject);
        } catch (MongoException.DuplicateKey ignore) {
            // fall through
        }

        return id;
    }

    public StoredCommit readCommit(Id id) throws NotFoundException, Exception {
        BasicDBObject key = new BasicDBObject();
        
        if (BINARY_FORMAT) {
            key.put(ID_FIELD, id.getBytes());
        } else {
            key.put(ID_FIELD, id.toString());
        }
        BasicDBObject commitObject = (BasicDBObject) commits.findOne(key);
        if (commitObject != null) {
            if (BINARY_FORMAT) {
                byte[] bytes = (byte[]) commitObject.get(DATA_FIELD);
                return StoredCommit.deserialize(id, new BinaryBinding(new ByteArrayInputStream(bytes)));
            } else {
                return StoredCommit.deserialize(id, new DBObjectBinding(commitObject));
            }
        } else {
            throw new NotFoundException(id.toString());
        }
    }

    public void writeCommit(Id id, Commit commit) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        commit.serialize(new BinaryBinding(out));
        byte[] bytes = out.toByteArray();

        BasicDBObject commitObject;
        if (BINARY_FORMAT) {
            commitObject = new BasicDBObject(ID_FIELD, id.getBytes()).append(DATA_FIELD, bytes);
        } else {
            commitObject = new BasicDBObject(ID_FIELD, id.toString());
            commit.serialize(new DBObjectBinding(commitObject));
        }
        try {
            commits.insert(commitObject);
        } catch (MongoException.DuplicateKey ignore) {
            // fall through
        }
    }

    public ChildNodeEntriesMap readCNEMap(Id id) throws NotFoundException, Exception {
        BasicDBObject key = new BasicDBObject();
        if (BINARY_FORMAT) {
            key.put(ID_FIELD, id.getBytes());
        } else {
            key.put(ID_FIELD, id.toString());
        }
        BasicDBObject mapObject = (BasicDBObject) cneMaps.findOne(key);
        if (mapObject != null) {
            if (BINARY_FORMAT) {
                byte[] bytes = (byte[]) mapObject.get(DATA_FIELD);
                return ChildNodeEntriesMap.deserialize(new BinaryBinding(new ByteArrayInputStream(bytes)));
            } else {
                return ChildNodeEntriesMap.deserialize(new DBObjectBinding(mapObject));
            }
        } else {
            throw new NotFoundException(id.toString());
        }
    }

    public Id writeCNEMap(ChildNodeEntriesMap map) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        map.serialize(new BinaryBinding(out));
        byte[] bytes = out.toByteArray();
        Id id = new Id(idFactory.createContentId(bytes));

        BasicDBObject mapObject;
        if (BINARY_FORMAT) {
            mapObject = new BasicDBObject(ID_FIELD, id.getBytes()).append(DATA_FIELD, bytes);
        } else {
            mapObject = new BasicDBObject(ID_FIELD, id.toString());
            map.serialize(new DBObjectBinding(mapObject));
        }
        try {
            cneMaps.insert(mapObject);
        } catch (MongoException.DuplicateKey ignore) {
            // fall through
        }

        return id;
    }

    //------------------------------------------------------------< BlobStore >

    public String addBlob(String tempFilePath) throws Exception {
        try {
            FilePath file = FilePath.get(tempFilePath);
            try {
                InputStream in = file.newInputStream();
                return writeBlob(in);
            } finally {
                file.delete();
            }
        } catch (Exception e) {
            throw ExceptionFactory.convert(e);
        }
    }

    public String writeBlob(InputStream in) throws Exception {
        GridFSInputFile f = fs.createFile(in, true);
        //f.save(0x20000);   // save in 128k chunks
        f.save();

        return f.getId().toString();
    }

    public int readBlob(String blobId, long pos, byte[] buff, int off,
            int length) throws Exception {

        GridFSDBFile f = fs.findOne(new ObjectId(blobId));
        if (f == null) {
            throw new NotFoundException(blobId);
        }
        // todo provide a more efficient implementation
        InputStream in = f.getInputStream();
        try {
            in.skip(pos);
            return in.read(buff, off, length);
        } finally {
            IOUtils.closeQuietly(in);
        }
    }

    public long getBlobLength(String blobId) throws Exception {
        GridFSDBFile f = fs.findOne(new ObjectId(blobId));
        if (f == null) {
            throw new NotFoundException(blobId);
        }

        return f.getLength();
    }
    
    //-------------------------------------------------------< implementation >

    protected final static String ENCODED_DOT = "_x46_";
    protected final static String ENCODED_DOLLAR_SIGN = "_x36_";

    /**
     * see <a href="http://www.mongodb.org/display/DOCS/Legal+Key+Names">http://www.mongodb.org/display/DOCS/Legal+Key+Names</a>
     *
     * @param name
     */
    protected static String encodeName(String name) {
        StringBuilder buf = null;
        for (int i = 0; i < name.length(); i++) {
            if (i == 0 && name.charAt(i) == '$') {
                // mongodb field names must not start with '$'
                buf = new StringBuilder();
                buf.append(ENCODED_DOLLAR_SIGN);
            } else if (name.charAt(i) == '.') {
                // . is a reserved char for mongodb field names
                if (buf == null) {
                    buf = new StringBuilder(name.substring(0, i));
                }
                buf.append(ENCODED_DOT);
            } else {
                if (buf != null) {
                    buf.append(name.charAt(i));
                }
            }
        }

        return buf == null ? name : buf.toString();
    }

    protected static String decodeName(String name) {
        StringBuilder buf = null;

        int lastPos = 0;
        if (name.startsWith(ENCODED_DOLLAR_SIGN)) {
            buf = new StringBuilder("$");
            lastPos = ENCODED_DOLLAR_SIGN.length();
        }

        int pos;
        while ((pos = name.indexOf(ENCODED_DOT, lastPos)) != -1) {
            if (buf == null) {
                buf = new StringBuilder();
            }
            buf.append(name.substring(lastPos, pos));
            buf.append('.');
            lastPos = pos + ENCODED_DOT.length();
        }

        if (buf != null) {
            buf.append(name.substring(lastPos));
            return buf.toString();
        } else {
            return name;
        }
    }

    //--------------------------------------------------------< inner classes >

    protected class DBObjectBinding implements Binding {

        BasicDBObject obj;

        protected DBObjectBinding(BasicDBObject obj) {
            this.obj = obj;
        }

        @Override
        public void write(String key, String value) throws Exception {
            obj.append(encodeName(key), value);
        }

        @Override
        public void write(String key, byte[] value) throws Exception {
            obj.append(encodeName(key), StringUtils.convertBytesToHex(value));
        }

        @Override
        public void write(String key, long value) throws Exception {
            obj.append(encodeName(key), value);
        }

        @Override
        public void write(String key, int value) throws Exception {
            obj.append(encodeName(key), value);
        }

        @Override
        public void writeMap(String key, int count, StringEntryIterator iterator) throws Exception {
            BasicDBObject childObj = new BasicDBObject();
            while (iterator.hasNext()) {
                StringEntry entry = iterator.next();
                childObj.append(encodeName(entry.getKey()), entry.getValue());
            }
            obj.append(encodeName(key), childObj);
        }

        @Override
        public void writeMap(String key, int count, BytesEntryIterator iterator) throws Exception {
            BasicDBObject childObj = new BasicDBObject();
            while (iterator.hasNext()) {
                BytesEntry entry = iterator.next();
                childObj.append(encodeName(entry.getKey()), StringUtils.convertBytesToHex(entry.getValue()));
            }
            obj.append(encodeName(key), childObj);
        }

        @Override
        public String readStringValue(String key) throws Exception {
            return obj.getString(encodeName(key));
        }

        @Override
        public byte[] readBytesValue(String key) throws Exception {
            return StringUtils.convertHexToBytes(obj.getString(encodeName(key)));
        }

        @Override
        public long readLongValue(String key) throws Exception {
            return obj.getLong(encodeName(key));
        }

        @Override
        public int readIntValue(String key) throws Exception {
            return obj.getInt(encodeName(key));
        }

        @Override
        public StringEntryIterator readStringMap(String key) throws Exception {
            final BasicDBObject childObj = (BasicDBObject) obj.get(encodeName(key));
            final Iterator<String> it = childObj.keySet().iterator();
            return new StringEntryIterator() {
                @Override
                public boolean hasNext() {
                    return it.hasNext();
                }

                @Override
                public StringEntry next() {
                    String key = it.next();
                    return new StringEntry(decodeName(key), childObj.getString(key));
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }

        @Override
        public BytesEntryIterator readBytesMap(String key) throws Exception {
            final BasicDBObject childObj = (BasicDBObject) obj.get(encodeName(key));
            final Iterator<String> it = childObj.keySet().iterator();
            return new BytesEntryIterator() {
                @Override
                public boolean hasNext() {
                    return it.hasNext();
                }

                @Override
                public BytesEntry next() {
                    String key = it.next();
                    return new BytesEntry(
                            decodeName(key),
                            StringUtils.convertHexToBytes(childObj.getString(key)));
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }
    }
}
