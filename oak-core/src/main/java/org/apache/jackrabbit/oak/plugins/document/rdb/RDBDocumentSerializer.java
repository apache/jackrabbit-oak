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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.zip.GZIPInputStream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.commons.json.JsopReader;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreException;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.apache.jackrabbit.oak.plugins.document.StableRevisionComparator;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Key;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Operation;

/**
 * Serialization/Parsing of documents.
 */
public class RDBDocumentSerializer {

    private final DocumentStore store;
    private final Set<String> columnProperties;

    private static final String MODIFIED = "_modified";
    private static final String MODCOUNT = "_modCount";
    private static final String CMODCOUNT = "_collisionsModCount";
    private static final String ID = "_id";
    private static final String HASBINARY = NodeDocument.HAS_BINARY_FLAG;
    private static final String DELETEDONCE = NodeDocument.DELETED_ONCE;

    private final Comparator<Revision> comparator = StableRevisionComparator.REVERSE;

    public RDBDocumentSerializer(DocumentStore store, Set<String> columnProperties) {
        this.store = store;
        this.columnProperties = columnProperties;
    }

    /**
     * Serializes all non-column properties of the {@link Document} into a JSON
     * string.
     */
    public String asString(@Nonnull Document doc) {
        StringBuilder sb = new StringBuilder(32768);
        sb.append("{");
        boolean needComma = false;
        for (Map.Entry<String, Object> entry : doc.entrySet()) {
            String key = entry.getKey();
            if (!columnProperties.contains(key)) {
                if (needComma) {
                    sb.append(",");
                }
                appendMember(sb, key, entry.getValue());
                needComma = true;
            }
        }
        sb.append("}");
        return sb.toString();
    }

    /**
     * Serializes the changes in the {@link UpdateOp} into a JSON array; each
     * entry is another JSON array holding operation, key, revision, and value.
     */
    public String asString(UpdateOp update) {
        StringBuilder sb = new StringBuilder("[");
        boolean needComma = false;
        for (Map.Entry<Key, Operation> change : update.getChanges().entrySet()) {
            Operation op = change.getValue();
            Key key = change.getKey();

            // exclude properties that are serialized into special columns
            if (columnProperties.contains(key.getName()) && null == key.getRevision())
                continue;

            if (needComma) {
                sb.append(",");
            }
            sb.append("[");
            if (op.type == UpdateOp.Operation.Type.INCREMENT) {
                sb.append("\"+\",");
            } else if (op.type == UpdateOp.Operation.Type.SET || op.type == UpdateOp.Operation.Type.SET_MAP_ENTRY) {
                sb.append("\"=\",");
            } else if (op.type == UpdateOp.Operation.Type.MAX) {
                sb.append("\"M\",");
            } else if (op.type == UpdateOp.Operation.Type.REMOVE_MAP_ENTRY) {
                sb.append("\"*\",");
            } else {
                throw new DocumentStoreException("Can't serialize " + update.toString() + " for JSON append");
            }
            appendString(sb, key.getName());
            sb.append(",");
            Revision rev = key.getRevision();
            if (rev != null) {
                appendString(sb, rev.toString());
                sb.append(",");
            }
            appendValue(sb, op.value);
            sb.append("]");
            needComma = true;
        }
        return sb.append("]").toString();
    }

    private static void appendMember(StringBuilder sb, String key, Object value) {
        appendString(sb, key);
        sb.append(":");
        appendValue(sb, value);
    }

    private static void appendValue(StringBuilder sb, Object value) {
        if (value == null) {
            sb.append("null");
        } else if (value instanceof Number) {
            sb.append(value.toString());
        } else if (value instanceof Boolean) {
            sb.append(value.toString());
        } else if (value instanceof String) {
            appendString(sb, (String) value);
        } else if (value instanceof Map) {
            appendMap(sb, (Map<Object, Object>) value);
        } else {
            throw new DocumentStoreException("unexpected type: " + value.getClass());
        }
    }

    private static void appendMap(StringBuilder sb, Map<Object, Object> map) {
        sb.append("{");
        boolean needComma = false;
        for (Map.Entry<Object, Object> e : map.entrySet()) {
            if (needComma) {
                sb.append(",");
            }
            appendMember(sb, e.getKey().toString(), e.getValue());
            needComma = true;
        }
        sb.append("}");
    }

    private static void appendString(StringBuilder sb, String s) {
        sb.append('"');
        JsopBuilder.escape(s, sb);
        sb.append('"');
    }

    /**
     * Reconstructs a {@link Document) based on the persisted {@link RDBRow}.
     */
    public <T extends Document> T fromRow(@Nonnull Collection<T> collection, @Nonnull RDBRow row) throws DocumentStoreException {
        T doc = collection.newDocument(store);
        doc.put(ID, row.getId());
        doc.put(MODIFIED, row.getModified());
        doc.put(MODCOUNT, row.getModcount());
        if (RDBDocumentStore.USECMODCOUNT) {
            doc.put(CMODCOUNT, row.getCollisionsModcount());
        }
        if (row.hasBinaryProperties()) {
            doc.put(HASBINARY, NodeDocument.HAS_BINARY_VAL);
        }
        if (row.deletedOnce()) {
            doc.put(DELETEDONCE, Boolean.TRUE);
        }

        byte[] bdata = row.getBdata();
        boolean blobInUse = false;
        JsopTokenizer json;

        // case #1: BDATA (blob) contains base data, DATA (string) contains
        // update operations
        try {
            if (bdata != null && bdata.length != 0) {
                String s = fromBlobData(bdata);
                json = new JsopTokenizer(s);
                json.read('{');
                readDocumentFromJson(json, doc);
                json.read(JsopReader.END);
                blobInUse = true;
            }
        } catch (Exception ex) {
            throw new DocumentStoreException(ex);
        }

        // start processing the VARCHAR data
        try {
            json = new JsopTokenizer(row.getData());

            int next = json.read();

            if (next == '{') {
                if (blobInUse) {
                    throw new DocumentStoreException("expected literal \"blob\" but found: " + row.getData());
                }
                readDocumentFromJson(json, doc);
            } else if (next == JsopReader.STRING) {
                if (!blobInUse) {
                    throw new DocumentStoreException("did not expect \"blob\" here: " + row.getData());
                }
                if (!"blob".equals(json.getToken())) {
                    throw new DocumentStoreException("expected string literal \"blob\"");
                }
            } else {
                throw new DocumentStoreException("unexpected token " + next + " in " + row.getData());
            }

            next = json.read();
            if (next == ',') {
                do {
                    Object ob = readValueFromJson(json);
                    if (!(ob instanceof List)) {
                        throw new DocumentStoreException("expected array but got: " + ob);
                    }
                    List<List<Object>> update = (List<List<Object>>) ob;
                    for (List<Object> op : update) {
                        applyUpdate(doc, update, op);
                    }

                } while (json.matches(','));
            }
            json.read(JsopReader.END);

            return doc;
        } catch (Exception ex) {
            throw new DocumentStoreException(ex);
        }
    }

    private <T extends Document> void applyUpdate(T doc, List updateString, List<Object> op) {
        String opcode = op.get(0).toString();
        String key = op.get(1).toString();
        Revision rev = null;
        Object value = null;
        if (op.size() == 3) {
            value = op.get(2);
        } else {
            rev = Revision.fromString(op.get(2).toString());
            value = op.get(3);
        }
        Object old = doc.get(key);

        if ("=".equals(opcode)) {
            if (rev == null) {
                doc.put(key, value);
            } else {
                @SuppressWarnings("unchecked")
                Map<Revision, Object> m = (Map<Revision, Object>) old;
                if (m == null) {
                    m = new TreeMap<Revision, Object>(comparator);
                    doc.put(key, m);
                }
                m.put(rev, value);
            }
        } else if ("*".equals(opcode)) {
            if (rev == null) {
                throw new DocumentStoreException("unexpected operation " + op + " in: " + updateString);
            } else {
                @SuppressWarnings("unchecked")
                Map<Revision, Object> m = (Map<Revision, Object>) old;
                if (m != null) {
                    m.remove(rev);
                }
            }
        } else if ("+".equals(opcode)) {
            if (rev == null) {
                Long x = (Long) value;
                if (old == null) {
                    old = 0L;
                }
                doc.put(key, ((Long) old) + x);
            } else {
                throw new DocumentStoreException("unexpected operation " + op + " in: " + updateString);
            }
        } else if ("M".equals(opcode)) {
            if (rev == null) {
                Comparable newValue = (Comparable) value;
                if (old == null || newValue.compareTo(old) > 0) {
                    doc.put(key, value);
                }
            } else {
                throw new DocumentStoreException("unexpected operation " + op + " in: " + updateString);
            }
        } else {
            throw new DocumentStoreException("unexpected operation " + op + " in: " + updateString);
        }
    }

    /**
     * Reads from an opened JSON stream ("{" already consumed) into a document.
     */
    private static <T extends Document> void readDocumentFromJson(@Nonnull JsopTokenizer json, @Nonnull T doc) {
        if (!json.matches('}')) {
            do {
                String key = json.readString();
                json.read(':');
                Object value = readValueFromJson(json);
                doc.put(key, value);
            } while (json.matches(','));
            json.read('}');
        }
    }

    @Nullable
    private static Object readValueFromJson(@Nonnull JsopTokenizer json) {
        switch (json.read()) {
            case JsopReader.NULL:
                return null;
            case JsopReader.TRUE:
                return true;
            case JsopReader.FALSE:
                return false;
            case JsopReader.NUMBER:
                return Long.parseLong(json.getToken());
            case JsopReader.STRING:
                return json.getToken();
            case '{':
                TreeMap<Revision, Object> map = new TreeMap<Revision, Object>(StableRevisionComparator.REVERSE);
                while (true) {
                    if (json.matches('}')) {
                        break;
                    }
                    String k = json.readString();
                    if (k == null) {
                        throw new IllegalArgumentException("unexpected null revision");
                    }
                    json.read(':');
                    map.put(Revision.fromString(k), readValueFromJson(json));
                    json.matches(',');
                }
                return map;
            case '[':
                List<Object> list = new ArrayList<Object>();
                while (true) {
                    if (json.matches(']')) {
                        break;
                    }
                    list.add(readValueFromJson(json));
                    json.matches(',');
                }
                return list;
            default:
                throw new IllegalArgumentException(json.readRawValue());
        }
    }

    // low level operations

    private static byte[] GZIPSIG = { 31, -117 };

    private static String fromBlobData(byte[] bdata) {
        try {
            if (bdata.length >= 2 && bdata[0] == GZIPSIG[0] && bdata[1] == GZIPSIG[1]) {
                // GZIP
                ByteArrayInputStream bis = new ByteArrayInputStream(bdata);
                GZIPInputStream gis = new GZIPInputStream(bis, 65536);
                return IOUtils.toString(gis, "UTF-8");
            } else {
                return IOUtils.toString(bdata, "UTF-8");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
