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
package org.apache.jackrabbit.oak.checkpoint;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.io.Closer;

/**
 * A helper class to manage checkpoints on TarMK and DocumentMK.
 */
public abstract class Checkpoints {

    public static Checkpoints onSegmentTar(File path, Closer closer) throws IOException {
        return SegmentTarCheckpoints.create(path, closer);
    }

    public static Checkpoints onDocumentMK(DocumentNodeStore store) {
        return new DocumentCheckpoints(store);
    }

    /**
     * @return a list of all checkpoints.
     */
    public abstract List<CP> list();

    /**
     * Remove all checkpoints.
     *
     * @return the number of removed checkpoints or {@code -1} if the operation
     *          did not succeed.
     */
    public abstract long removeAll();

    /**
     * Remove all unreferenced checkpoints.
     *
     * @return the number of removed checkpoints or {@code -1} if the operation
     *          did not succeed.
     */
    public abstract long removeUnreferenced();

    /**
     * Removes the given checkpoint.
     *
     * @param cp a checkpoint string.
     * @return {@code 1} if the checkpoint was successfully remove, {@code 0} if
     *          there is no such checkpoint or {@code -1} if the operation did
     *          not succeed.
     */
    public abstract int remove(String cp);

    /**
     * Return checkpoint metadata
     *
     * @param cp a checkpoint string.
     * @return checkpoints metadata map or null if checkpoint can't be found
     */
    public abstract Map<String, String> getInfo(String cp);

    /**
     * Set the property in the checkpoint metadata.
     *
     * @param cp a checkpoint string.
     * @param name property name
     * @param value new value of the property. the property will be removed if the value is {@code null}
     * @return {@code 1} if the checkpoint was successfully remove, {@code 0} if
     *          there is no such checkpoint or {@code -1} if the operation did
     *          not succeed.
     */
    public abstract int setInfoProperty(String cp, String name, String value);

    @Nonnull
    static Set<String> getReferencedCheckpoints(NodeState root) {
        Set<String> cps = new HashSet<String>();
        for (PropertyState ps : root.getChildNode(":async").getProperties()) {
            String name = ps.getName();
            if (name.endsWith("async") && ps.getType().equals(Type.STRING)) {
                String ref = ps.getValue(Type.STRING);
                System.out.println("Referenced checkpoint from /:async@" + name
                        + " is " + ref);
                cps.add(ref);
            }
        }
        return cps;
    }

    public static final class CP {

        public final String id;
        public final long created;
        public final long expires;

        CP(String id, long created, long expires) {
            this.id = id;
            this.created = created;
            this.expires = expires;
        }

    }

}