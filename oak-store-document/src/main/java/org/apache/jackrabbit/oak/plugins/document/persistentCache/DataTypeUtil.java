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
package org.apache.jackrabbit.oak.plugins.document.persistentCache;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeState;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.NamePathRev;
import org.apache.jackrabbit.oak.plugins.document.Path;
import org.apache.jackrabbit.oak.plugins.document.PathRev;
import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.type.StringDataType;

/**
 * Utility class to write various types to a buffer and read it back again.
 */
class DataTypeUtil {

    static void booleanToBuffer(boolean b, WriteBuffer buffer) {
        buffer.put((byte) (b ? 1 : 0));
    }

    static boolean booleanFromBuffer(ByteBuffer buffer) {
        return buffer.get() != 0;
    }

    static void revisionVectorToBuffer(RevisionVector rv, WriteBuffer buffer) {
        buffer.putVarInt(rv.getDimensions());
        for (Revision r : rv) {
            buffer.putLong(r.getTimestamp());
            buffer.putVarInt(r.getCounter());
            buffer.putVarInt(r.getClusterId());
            booleanToBuffer(r.isBranch(), buffer);
        }
    }

    static RevisionVector revisionVectorFromBuffer(ByteBuffer buffer) {
        int dim  = DataUtils.readVarInt(buffer);
        List<Revision> revisions = new ArrayList<>();
        for (int i = 0; i < dim; i++) {
            revisions.add(new Revision(
                    buffer.getLong(),
                    DataUtils.readVarInt(buffer),
                    DataUtils.readVarInt(buffer),
                    booleanFromBuffer(buffer))
            );
        }
        return new RevisionVector(revisions);
    }

    static void pathToBuffer(Path p, WriteBuffer buffer) {
        int len = p.getDepth() + (p.isAbsolute() ? 1 : 0);
        buffer.putVarInt(len);
        // write path elements backwards
        while (p != null) {
            StringDataType.INSTANCE.write(buffer, p.getName());
            p = p.getParent();
        }
    }

    static Path pathFromBuffer(ByteBuffer buffer) {
        int numElements = DataUtils.readVarInt(buffer);
        List<String> elements = new ArrayList<>(numElements);
        for (int i = 0; i < numElements; i++) {
            elements.add(StringDataType.INSTANCE.read(buffer));
        }
        // elements are written backwards
        String firstElement = elements.get(elements.size() - 1);
        Path p;
        if (firstElement.isEmpty()) {
            p = Path.ROOT;
        } else {
            p = new Path(firstElement);
        }
        // construct path with remaining elements
        for (int i = elements.size() - 2; i >= 0; i--) {
            p = new Path(p, elements.get(i));
        }
        return p;
    }

    static void pathRevToBuffer(PathRev pr, WriteBuffer buffer) {
        pathToBuffer(pr.getPath(), buffer);
        revisionVectorToBuffer(pr.getRevision(), buffer);
    }

    static PathRev pathRevFromBuffer(ByteBuffer buffer) {
        return new PathRev(
                pathFromBuffer(buffer),
                revisionVectorFromBuffer(buffer)
        );
    }

    static void namePathRevToBuffer(NamePathRev pnr, WriteBuffer buffer) {
        StringDataType.INSTANCE.write(buffer, pnr.getName());
        pathToBuffer(pnr.getPath(), buffer);
        revisionVectorToBuffer(pnr.getRevision(), buffer);
    }

    static NamePathRev namePathRevFromBuffer(ByteBuffer buffer) {
        return new NamePathRev(
                StringDataType.INSTANCE.read(buffer),
                pathFromBuffer(buffer),
                revisionVectorFromBuffer(buffer)
        );
    }

    static void stateToBuffer(DocumentNodeState state, WriteBuffer buffer) {
        pathToBuffer(state.getPath(), buffer);
        revisionVectorToBuffer(state.getRootRevision(), buffer);
        RevisionVector lastRevision = state.getLastRevision();
        if (lastRevision == null) {
            lastRevision = RevisionVector.fromString("");
        }
        revisionVectorToBuffer(lastRevision, buffer);
        buffer.putVarInt(state.getMemory());
        booleanToBuffer(state.hasNoChildren(), buffer);
        Map<String, String> props = state.getAllBundledProperties();
        buffer.putVarInt(props.size());
        for (Map.Entry<String, String> e : props.entrySet()) {
            StringDataType.INSTANCE.write(buffer, e.getKey());
            StringDataType.INSTANCE.write(buffer, e.getValue());
        }
    }

    static DocumentNodeState stateFromBuffer(DocumentNodeStore store,
                                             ByteBuffer buffer) {
        Path p = pathFromBuffer(buffer);
        RevisionVector rootRevision = revisionVectorFromBuffer(buffer);
        RevisionVector lastRevision = revisionVectorFromBuffer(buffer);
        if (lastRevision.getDimensions() == 0) {
            lastRevision = null;
        }
        int mem = DataUtils.readVarInt(buffer);
        boolean noChildren = booleanFromBuffer(buffer);
        int numProps = DataUtils.readVarInt(buffer);
        Map<String, PropertyState> props = new HashMap<>(numProps);
        for (int i = 0; i < numProps; i++) {
            String name = StringDataType.INSTANCE.read(buffer);
            String value = StringDataType.INSTANCE.read(buffer);
            props.put(name, store.createPropertyState(name, value));
        }
        return new DocumentNodeState(store, p, rootRevision, props,
                !noChildren, mem, lastRevision, false);
    }
}
