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
package org.apache.jackrabbit.oak.plugins.index.old.mk.simple;

import org.apache.jackrabbit.mk.json.JsopBuilder;
import org.apache.jackrabbit.mk.json.JsopWriter;
import org.apache.jackrabbit.mk.util.Cache;
import org.apache.jackrabbit.mk.json.JsopTokenizer;

/**
 * A revision, including pointer to the root node of that revision.
 */
public class Revision implements Comparable<Revision>, Cache.Value {

    private NodeImpl node;
    private final long id;
    private final long nanos;
    private String diff;
    private String msg;

    Revision(long id, long nanos, String diff, String msg) {
        this.id = id;
        this.nanos = nanos;
        this.diff = diff;
        this.msg = msg == null ? "" : msg;
    }

    private Revision(long id, long nanos, NodeImpl node) {
        this.id = id;
        this.nanos = nanos;
        this.node = node;
    }

    static Revision get(NodeImpl node) {
        String rev = node.getProperty("rev");
        if (rev == null) {
            return null;
        }
        long id = parseId(JsopTokenizer.decodeQuoted(rev));
        long nanos = Long.parseLong(node.getProperty("nanos"));
        return new Revision(id, nanos, node);
    }

    long getId() {
        return id;
    }

    long getNanos() {
        return nanos;
    }

    private String getDiff() {
        if (diff == null) {
            String s = getCommitValue("diff");
            if (s.length() == 0) {
                s = "";
            } else if (s.charAt(0) == '[') {
                // remove the surrounding "[" and "]"
                s = s.substring(1, s.length() - 1);
            } else {
                s = JsopTokenizer.decodeQuoted(s);
            }
            diff = s;
        }
        return diff;
    }

    private String getMsg() {
        if (msg == null) {
            String m = getCommitValue("msg");
            msg = m.length() == 0 ? "" : JsopTokenizer.decodeQuoted(m);
        }
        return msg;
    }

    private String getCommitValue(String s) {
        if (node.exists("commit")) {
            String v = node.getNode("commit").getProperty(s);
            if (v != null) {
                return v;
            }
        }
        return "";
    }

    @Override
    public int compareTo(Revision o) {
        return id < o.id ? -1 : id > o.id ? 1 : 0;
    }

    @Override
    public String toString() {
        return new JsopBuilder().object().
            key("id").value(formatId(id)).
            key("ts").value(nanos / 1000000).
            key("msg").value(getMsg()).
        endObject().toString();
    }

    static long parseId(String revisionId) {
        return Long.parseLong(revisionId, 16);
    }

    static String formatId(long revId) {
        return Long.toHexString(revId);
    }

    NodeImpl store(NodeImpl head, NodeImpl commit) {
        head = head.cloneAndSetProperty("rev", JsopBuilder.encode(formatId(id)), id);
        head = head.cloneAndSetProperty("nanos", Long.toString(nanos), id);
        if (diff.indexOf(";\n") >= 0) {
            commit.setProperty("diff", JsopBuilder.encode(diff));
        } else {
            commit.setProperty("diff", "[" + diff + "]");
        }
        if (msg != null && msg.length() > 0) {
            commit.setProperty("msg", JsopBuilder.encode(msg));
        }
        return head.setChild("commit", commit, id);
    }

    void appendJournal(JsopWriter buff) {
        buff.object().
            key("id").value(Revision.formatId(id)).
            key("ts").value(nanos / 1000000).
            key("msg").value(getMsg()).
            key("changes").value(getDiff()).
        endObject().newline();
    }

    @Override
    public int getMemory() {
        return (getDiff().length() + getMsg().length()) * 2;
    }

}
