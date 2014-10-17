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
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.zip.GZIPInputStream;

import javax.annotation.Nonnull;

import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreException;
import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.apache.jackrabbit.oak.plugins.document.StableRevisionComparator;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Key;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Operation;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * Serialization/Parsing of documents.
 */
public class RDBDocumentSerializer {

    private final DocumentStore store;
    private final Set<String> columnProperties;

    private static final String MODIFIED = "_modified";
    private static final String MODCOUNT = "_modCount";
    private static final String ID = "_id";
    private final Comparator<Revision> comparator = StableRevisionComparator.REVERSE;

    public RDBDocumentSerializer(DocumentStore store, Set<String> columnProperties) {
        this.store = store;
        this.columnProperties = columnProperties;
    }

    public String asString(@Nonnull Document doc) {
        StringBuilder sb = new StringBuilder(32768);
        sb.append("{");
        boolean needComma = false;
        for (String key : doc.keySet()) {
            if (!columnProperties.contains(key)) {
                if (needComma) {
                    sb.append(",");
                }
                appendMember(sb, key, doc.get(key));
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

            // already checked
            if (op.type == UpdateOp.Operation.Type.CONTAINS_MAP_ENTRY)
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
            if (key.getRevision() != null) {
                appendString(sb, key.getRevision().toString());
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

    private static String[] JSONCONTROLS = new String[] { "\\u0000", "\\u0001", "\\u0002", "\\u0003", "\\u0004", "\\u0005",
            "\\u0006", "\\u0007", "\\b", "\\t", "\\n", "\\u000b", "\\f", "\\r", "\\u000e", "\\u000f", "\\u0010", "\\u0011",
            "\\u0012", "\\u0013", "\\u0014", "\\u0015", "\\u0016", "\\u0017", "\\u0018", "\\u0019", "\\u001a", "\\u001b",
            "\\u001c", "\\u001d", "\\u001e", "\\u001f" };

    private static void appendString(StringBuilder sb, String s) {
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '"') {
                sb.append("\\\"");
            } else if (c == '\\') {
                sb.append("\\\\");
            } else if (c >= 0 && c < 0x20) {
                sb.append(JSONCONTROLS[c]);
            } else {
                sb.append(c);
            }
        }
        sb.append('"');
    }

    // JSON simple serializer
    // private static String asString(@Nonnull Document doc) {
    // JSONObject obj = new JSONObject();
    // for (String key : doc.keySet()) {
    // if (! COLUMNPROPERTIES.contains(key)) {
    // Object value = doc.get(key);
    // obj.put(key, value);
    // }
    // }
    // return obj.toJSONString();
    // }

    public <T extends Document> T fromRow(Collection<T> collection, RDBRow row) throws DocumentStoreException {
        T doc = collection.newDocument(store);
        doc.put(ID, row.getId());
        doc.put(MODIFIED, row.getModified());
        doc.put(MODCOUNT, row.getModcount());

        JSONParser jp = new JSONParser();

        Map<String, Object> baseData = null;
        byte[] bdata = row.getBdata();
        JSONArray arr = null;

        try {
            if (bdata != null && bdata.length != 0) {
                baseData = (Map<String, Object>) jp.parse(fromBlobData(bdata));
            }
            // TODO figure out a faster way
            arr = (JSONArray) new JSONParser().parse("[" + row.getData() + "]");
        } catch (ParseException ex) {
            throw new DocumentStoreException(ex);
        }

        int updatesStartAt = 0;
        if (baseData == null) {
            // if we do not have a blob, the first part of the string data is
            // the base JSON
            baseData = (Map<String, Object>) arr.get(0);
            updatesStartAt = 1;
        }

        for (Map.Entry<String, Object> entry : baseData.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            if (value == null) {
                // ???
                doc.put(key, value);
            } else if (value instanceof Boolean || value instanceof Long || value instanceof String) {
                doc.put(key, value);
            } else if (value instanceof JSONObject) {
                doc.put(key, convertJsonObject((JSONObject) value));
            } else {
                throw new RuntimeException("unexpected type: " + value.getClass());
            }
        }

        for (int u = updatesStartAt; u < arr.size(); u++) {
            Object ob = arr.get(u);
            if (ob instanceof JSONArray) {
                JSONArray update = (JSONArray) ob;
                for (int o = 0; o < update.size(); o++) {
                    JSONArray op = (JSONArray) update.get(o);
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
                            throw new RuntimeException("unexpected operation " + op + "in: " + ob);
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
                            throw new RuntimeException("unexpected operation " + op + "in: " + ob);
                        }
                    } else if ("M".equals(opcode)) {
                        if (rev == null) {
                            Comparable newValue = (Comparable) value;
                            if (old == null || newValue.compareTo(old) > 0) {
                                doc.put(key, value);
                            }
                        } else {
                            throw new RuntimeException("unexpected operation " + op + "in: " + ob);
                        }
                    } else {
                        throw new RuntimeException("unexpected operation " + op + "in: " + ob);
                    }
                }
            } else if (ob.toString().equals("blob") && u == 0) {
                // expected placeholder
            } else {
                throw new RuntimeException("unexpected: " + ob);
            }
        }
        return doc;
    }

    @Nonnull
    private Map<Revision, Object> convertJsonObject(@Nonnull JSONObject obj) {
        Map<Revision, Object> map = new TreeMap<Revision, Object>(comparator);
        Set<Map.Entry> entries = obj.entrySet();
        for (Map.Entry entry : entries) {
            // not clear why every persisted map is a revision map
            map.put(Revision.fromString(entry.getKey().toString()), entry.getValue());
        }
        return map;
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
