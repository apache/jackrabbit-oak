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

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

import org.apache.jackrabbit.mk.model.ChildNodeEntriesMap;
import org.apache.jackrabbit.mk.model.Commit;
import org.apache.jackrabbit.mk.model.Id;
import org.apache.jackrabbit.mk.model.Node;
import org.apache.jackrabbit.mk.model.StoredCommit;
import org.apache.jackrabbit.mk.store.BinaryBinding;
import org.apache.jackrabbit.mk.store.Binding;
import org.apache.jackrabbit.mk.store.IdFactory;
import org.apache.jackrabbit.mk.store.NotFoundException;
import org.apache.jackrabbit.mk.util.IOUtils;

/**
 *
 */
public class FSPersistence implements Persistence {

    private final File homeDir;
    private File dataDir;
    private File head;

    // TODO: make this configurable
    private IdFactory idFactory = IdFactory.getDigestFactory();
    
    
    public FSPersistence(File homeDir) {
        this.homeDir = homeDir;
    }
    
    public void initialize() throws Exception {
        dataDir = new File(homeDir, "data");
        if (!dataDir.exists()) {
            dataDir.mkdirs();
        }
        head = new File(homeDir, "HEAD");
        if (!head.exists()) {
            writeHead(null);
        }
    }

    public Id readHead() throws Exception {
        FileInputStream in = new FileInputStream(head);
        try {
            String s = IOUtils.readString(in);
            return s.equals("") ? null : Id.fromString(s);
        } finally {
            in.close();
        }
    }

    public void writeHead(Id id) throws Exception {
        FileOutputStream out = new FileOutputStream(head);
        try {
            IOUtils.writeString(out, id == null ? "" : id.toString());
        } finally {
            out.close();
        }
    }

    public Binding readNodeBinding(Id id) throws NotFoundException, Exception {
        File f = getFile(id);
        if (f.exists()) {
            BufferedInputStream in = new BufferedInputStream(new FileInputStream(f));
            try {
                return new BinaryBinding(in);
            } finally {
                in.close();
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
        writeFile(id, bytes);
        return id;
    }

    public StoredCommit readCommit(Id id) throws NotFoundException, Exception {
        File f = getFile(id);
        if (f.exists()) {
            BufferedInputStream in = new BufferedInputStream(new FileInputStream(f));
            try {
                return StoredCommit.deserialize(id, new BinaryBinding(in));
            } finally {
                in.close();
            }
        } else {
            throw new NotFoundException(id.toString());
        }
    }

    public void writeCommit(Id id, Commit commit) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        commit.serialize(new BinaryBinding(out));
        byte[] bytes = out.toByteArray();
        writeFile(id, bytes);
    }

    public ChildNodeEntriesMap readCNEMap(Id id) throws NotFoundException, Exception {
        File f = getFile(id);
        if (f.exists()) {
            BufferedInputStream in = new BufferedInputStream(new FileInputStream(f));
            try {
                return ChildNodeEntriesMap.deserialize(new BinaryBinding(in));
            } finally {
                in.close();
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
        writeFile(id, bytes);
        return id;
    }

    //-------------------------------------------------------< implementation >

    private File getFile(Id id) {
        String sId = id.toString();
        StringBuilder buf = new StringBuilder(sId.substring(0, 2));
        buf.append('/');
        buf.append(sId.substring(2));
        return new File(dataDir, buf.toString());
    }

    private void writeFile(Id id, byte[] data) throws Exception {
        File tmp = File.createTempFile("tmp", null, dataDir);

        try {
            FileOutputStream fos = new FileOutputStream(tmp);

            try {
                fos.write(data);
            } finally {
                //fos.getChannel().force(true);
                fos.close();
            }

            File dst = getFile(id);
            if (dst.exists()) {
                // already exists
                return;
            }
            // move tmp file
            tmp.setReadOnly();
            if (tmp.renameTo(dst)) {
                return;
            }
            // make sure parent dir exists and try again
            dst.getParentFile().mkdir();
            if (tmp.renameTo(dst)) {
                return;
            }
            throw new Exception("failed to create " + dst);
        } finally {
            tmp.delete();
        }
    }
}
