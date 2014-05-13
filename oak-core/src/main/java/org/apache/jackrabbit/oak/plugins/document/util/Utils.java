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
package org.apache.jackrabbit.oak.plugins.document.util;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Timestamp;
import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.codec.binary.Hex;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;

/**
 * Utility methods.
 */
public class Utils {
    private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

    /**
     * Approximate length of a Revision string.
     */
    private static final int REVISION_LENGTH =
            new Revision(System.currentTimeMillis(), 0, 0).toString().length();

    /**
     * The length of path (in characters), whose UTF-8 representation can not
     * possibly be too large to be used for the primary key for the document
     * store.
     */
    static final int PATH_SHORT = Integer.getInteger("oak.pathShort", 165);

    /**
     * The maximum length of the parent path, in bytes. If the parent path is
     * longer, then the id of a document is no longer the path, but the hash of
     * the parent, and then the node name.
     */
    static final int PATH_LONG = Integer.getInteger("oak.pathLong", 350);

    /**
     * The maximum size a node name, in bytes. This is only a problem for long path.
     */
    private static final int NODE_NAME_LIMIT = Integer.getInteger("oak.nodeNameLimit", 150);

    private static final Charset UTF_8 = Charset.forName("UTF-8");

    /**
     * Make sure the name string does not contain unnecessary baggage (shared
     * strings).
     * <p>
     * This is only needed for older versions of Java (before Java 7 update 6).
     * See also
     * http://mail.openjdk.java.net/pipermail/core-libs-dev/2012-May/010257.html
     *
     * @param x the string
     * @return the new string
     */
    public static String unshareString(String x) {
        return new String(x);
    }

    public static int pathDepth(String path) {
        if (path.equals("/")) {
            return 0;
        }
        int depth = 0;
        for (int i = 0; i < path.length(); i++) {
            if (path.charAt(i) == '/') {
                depth++;
            }
        }
        return depth;
    }

    @SuppressWarnings("unchecked")
    public static int estimateMemoryUsage(Map<?, Object> map) {
        if (map == null) {
            return 0;
        }
        int size = 0;

        for (Entry<?, Object> e : map.entrySet()) {
            if (e.getKey() instanceof Revision) {
                size += 32;
            } else {
                size += 48 + e.getKey().toString().length() * 2;
            }
            Object o = e.getValue();
            if (o instanceof String) {
                size += 48 + ((String) o).length() * 2;
            } else if (o instanceof Long) {
                size += 16;
            } else if (o instanceof Boolean) {
                size += 8;
            } else if (o instanceof Integer) {
                size += 8;
            } else if (o instanceof Map) {
                size += 8 + estimateMemoryUsage((Map<String, Object>) o);
            } else if (o == null) {
                // zero
            } else {
                throw new IllegalArgumentException("Can't estimate memory usage of " + o);
            }
        }

        if (map instanceof BasicDBObject) {
            // Based on empirical testing using JAMM
            size += 176;
            size += map.size() * 136;
        } else {
            // overhead for some other kind of map
            // TreeMap (80) + unmodifiable wrapper (32)
            size += 112;
            // 64 bytes per entry
            size += map.size() * 64;
        }
        return size;
    }

    /**
     * Generate a unique cluster id, similar to the machine id field in MongoDB ObjectId objects.
     *
     * @return the unique machine id
     */
    public static int getUniqueClusterId() {
        ObjectId objId = new ObjectId();
        return objId._machine();
    }

    public static String escapePropertyName(String propertyName) {
        int len = propertyName.length();
        if (len == 0) {
            return "_";
        }
        // avoid creating a buffer if escaping is not needed
        StringBuilder buff = null;
        char c = propertyName.charAt(0);
        int i = 0;
        if (c == '_' || c == '$') {
            buff = new StringBuilder(len + 1);
            buff.append('_').append(c);
            i++;
        }
        for (; i < len; i++) {
            c = propertyName.charAt(i);
            char rep;
            switch (c) {
            case '.':
                rep = 'd';
                break;
            case '\\':
                rep = '\\';
                break;
            default:
                rep = 0;
            }
            if (rep != 0) {
                if (buff == null) {
                    buff = new StringBuilder(propertyName.substring(0, i));
                }
                buff.append('\\').append(rep);
            } else if (buff != null) {
                buff.append(c);
            }
        }
        return buff == null ? propertyName : buff.toString();
    }

    public static String unescapePropertyName(String key) {
        int len = key.length();
        if (key.startsWith("_")
                && (key.startsWith("__") || key.startsWith("_$") || len == 1)) {
            key = key.substring(1);
            len--;
        }
        // avoid creating a buffer if escaping is not needed
        StringBuilder buff = null;
        for (int i = 0; i < len; i++) {
            char c = key.charAt(i);
            if (c == '\\') {
                if (buff == null) {
                    buff = new StringBuilder(key.substring(0, i));
                }
                c = key.charAt(++i);
                if (c == '\\') {
                    // ok
                } else if (c == 'd') {
                    c = '.';
                }
                buff.append(c);
            } else if (buff != null) {
                buff.append(c);
            }
        }
        return buff == null ? key : buff.toString();
    }

    public static boolean isPropertyName(String key) {
        return !key.startsWith("_") || key.startsWith("__") || key.startsWith("_$");
    }

    public static String getIdFromPath(String path) {
        if (isLongPath(path)) {
            MessageDigest digest;
            try {
                digest = MessageDigest.getInstance("SHA-256");
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
            int depth = Utils.pathDepth(path);
            String parent = PathUtils.getParentPath(path);
            byte[] hash = digest.digest(parent.getBytes(UTF_8));
            String name = PathUtils.getName(path);
            return depth + ":h" + Hex.encodeHexString(hash) + "/" + name;
        }
        int depth = Utils.pathDepth(path);
        return depth + ":" + path;
    }

    /**
     * Returns the parent id for given id if possible
     *
     * <p>It would return null in following cases
     * <ul>
     *     <li>If id is from long path</li>
     *     <li>If id is for root path</li>
     * </ul>
     *</p>
     * @param id id for which parent id needs to be determined
     * @return parent id. null if parent id cannot be determined
     */
    @CheckForNull
    public static String getParentId(String id){
        if(Utils.isIdFromLongPath(id)){
            return null;
        }
        String path = Utils.getPathFromId(id);
        if(PathUtils.denotesRoot(path)){
            return null;
        }
        String parentPath = PathUtils.getParentPath(path);
        return Utils.getIdFromPath(parentPath);
    }

    public static boolean isLongPath(String path) {
        // the most common case: a short path
        // avoid calculating the parent path
        if (path.length() < PATH_SHORT) {
            return false;
        }
        // check if name is too long
        String name = PathUtils.getName(path);
        if (name.getBytes(UTF_8).length > NODE_NAME_LIMIT) {
            throw new IllegalArgumentException("Node name is too long: " + path);
        }
        // check if the parent path is long
        byte[] parent = PathUtils.getParentPath(path).getBytes(UTF_8);
        if (parent.length < PATH_LONG) {
            return false;
        }
        return true;
    }
    
    public static boolean isIdFromLongPath(String id) {
        int index = id.indexOf(':');
        return id.charAt(index + 1) == 'h';
    }

    public static String getPathFromId(String id) {
        if (isIdFromLongPath(id)) {
            throw new IllegalArgumentException("Id is hashed: " + id);
        }
        int index = id.indexOf(':');
        return id.substring(index + 1);
    }

    public static String getPreviousPathFor(String path, Revision r, int height) {
        if (!PathUtils.isAbsolute(path)) {
            throw new IllegalArgumentException("path must be absolute: " + path);
        }
        StringBuilder sb = new StringBuilder(path.length() + REVISION_LENGTH + 3);
        sb.append("p").append(path);
        if (sb.charAt(sb.length() - 1) != '/') {
            sb.append('/');
        }
        r.toStringBuilder(sb).append("/").append(height);
        return sb.toString();
    }

    public static String getPreviousIdFor(String path, Revision r, int height) {
        return getIdFromPath(getPreviousPathFor(path, r, height));
    }

    /**
     * Deep copy of a map that may contain map values.
     *
     * @param source the source map
     * @param target the target map
     * @param <K> the type of the map key
     */
    public static <K> void deepCopyMap(Map<K, Object> source, Map<K, Object> target) {
        for (Entry<K, Object> e : source.entrySet()) {
            Object value = e.getValue();
            Comparator<? super K> comparator = null;
            if (value instanceof SortedMap) {
                @SuppressWarnings("unchecked")
                SortedMap<K, Object> map = (SortedMap<K, Object>) value;
                comparator = map.comparator();
            }
            if (value instanceof Map<?, ?>) {
                @SuppressWarnings("unchecked")
                Map<K, Object> old = (Map<K, Object>) value;
                Map<K, Object> c = new TreeMap<K, Object>(comparator);
                deepCopyMap(old, c);
                value = c;
            }
            target.put(e.getKey(), value);
        }
    }

    /**
     * Returns the lower key limit to retrieve the children of the given
     * <code>path</code>.
     *
     * @param path a path.
     * @return the lower key limit.
     */
    public static String getKeyLowerLimit(String path) {
        String from = PathUtils.concat(path, "a");
        from = getIdFromPath(from);
        from = from.substring(0, from.length() - 1);
        return from;
    }

    /**
     * Returns the upper key limit to retrieve the children of the given
     * <code>path</code>.
     *
     * @param path a path.
     * @return the upper key limit.
     */
    public static String getKeyUpperLimit(String path) {
        String to = PathUtils.concat(path, "z");
        to = getIdFromPath(to);
        to = to.substring(0, to.length() - 2) + "0";
        return to;
    }

    /**
     * Returns parentId extracted from the fromKey. fromKey is usually constructed
     * using Utils#getKeyLowerLimit
     *
     * @param fromKey key used as start key in queries
     * @return parentId if possible.
     */
    @CheckForNull
    public static String getParentIdFromLowerLimit(String fromKey){
        //If key just ends with slash 2:/foo/ then append a fake
        //name to create a proper id
        if(fromKey.endsWith("/")){
            fromKey = fromKey + "a";
        }
        return getParentId(fromKey);
    }

    /**
     * Returns <code>true</code> if a revision tagged with the given revision
     * should be considered committed, <code>false</code> otherwise. Committed
     * revisions have a tag, which equals 'c' or starts with 'c-'.
     *
     * @param tag the tag (may be <code>null</code>).
     * @return <code>true</code> if committed; <code>false</code> otherwise.
     */
    public static boolean isCommitted(@Nullable String tag) {
        return tag != null && (tag.equals("c") || tag.startsWith("c-"));
    }

    /**
     * Resolve the commit revision for the given revision <code>rev</code> and
     * the associated commit tag.
     *
     * @param rev a revision.
     * @param tag the associated commit tag.
     * @return the actual commit revision for <code>rev</code>.
     */
    @Nonnull
    public static Revision resolveCommitRevision(@Nonnull Revision rev,
                                                 @Nonnull String tag) {
        return checkNotNull(tag).startsWith("c-") ?
                Revision.fromString(tag.substring(2)) : rev;
    }

    /**
     * Closes the obj its of type {@link java.io.Closeable}. It is mostly
     * used to close Iterator/Iterables which are backed by say DBCursor
     *
     * @param obj object to close
     */
    public static void closeIfCloseable(Object obj) {
        if (obj instanceof Closeable) {
            try {
                ((Closeable) obj).close();
            } catch (IOException e) {
                LOG.warn("Error occurred while closing {}", obj, e);
            }
        }
    }

    /**
     * Provides a readable string for given timestamp
     */
    public static String timestampToString(long timestamp) {
        return (new Timestamp(timestamp) + "00").substring(0, 23);
    }
}
