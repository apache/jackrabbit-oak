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
package org.apache.jackrabbit.oak.spi.xml;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Helper class used to keep track of uuid mappings (e.g. if the uuid of an
 * imported or copied node is mapped to a new uuid) and processed (e.g. imported
 * or copied) reference properties that might need to be adjusted depending on
 * the UUID mapping resulting from the import.
 *
 * @see javax.jcr.ImportUUIDBehavior
 */
public class ReferenceChangeTracker {

    /**
     * mapping from original uuid to new uuid of mix:referenceable nodes
     */
    private final Map<String, String> uuidMap = new HashMap<String, String>();

    /**
     * list of processed reference properties that might need correcting
     */
    private final List<Object> references = new ArrayList<Object>();

    /**
     * Returns the new node id to which {@code oldUUID} has been mapped
     * or {@code null} if no such mapping exists.
     *
     * @param oldUUID old node id
     * @return mapped new id or {@code null} if no such mapping exists
     * @see #put(String, String)
     */
    public String get(String oldUUID) {
        return uuidMap.get(oldUUID);
    }

    /**
     * Store the given id mapping for later lookup using
     * {@link #get(String)}.
     *
     * @param oldUUID old node id
     * @param newUUID new node id
     */
    public void put(String oldUUID, String newUUID) {
        uuidMap.put(oldUUID, newUUID);
    }

    /**
     * Resets all internal state.
     */
    public void clear() {
        uuidMap.clear();
        references.clear();
    }

    /**
     * Store the given reference property for later retrieval using
     * {@link #getProcessedReferences()}.
     *
     * @param refProp reference property
     */
    public void processedReference(Object refProp) {
        references.add(refProp);
    }

    /**
     * Returns an iterator over all processed reference properties.
     *
     * @return an iterator over all processed reference properties
     * @see #processedReference(Object)
     */
    public Iterator<Object> getProcessedReferences() {
        return references.iterator();
    }

    /**
     * Remove the given references that have already been processed from the
     * references list.
     *
     * @param processedReferences
     * @return {@code true} if the internal list of references changed.
     */
    public boolean removeReferences(List<Object> processedReferences) {
        return references.removeAll(processedReferences);
    }
}
