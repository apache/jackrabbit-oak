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

package org.apache.jackrabbit.oak.plugins.document;

import java.util.List;
import java.util.Map;
/**
 *  This class is as a wrapper around DocumentStore that expose two methods used to clean garbage from NODES collection
 *  public int remove(Map<String, Long> orphanOrDeletedRemovalMap)
 *  public List<NodeDocument> findAndUpdate(List<UpdateOp> updateOpList)
 *  When enabled
 *  Each method saves the document ID or empty properties names (that will be deleted) to a separate _bin collection as a BinDocument then delegates deletion to DocumentStore
 *
 *  When disabled (default)
 *  Each method delegates directly to DocumentStore
 */
public interface FullGcNodeBin {

    static FullGcNodeBin noBin(DocumentStore store) {
        return new FullGcNodeBin() {
            @Override
            public int remove(Map<String, Long> orphanOrDeletedRemovalMap) {
                return store.remove(Collection.NODES, orphanOrDeletedRemovalMap);
            }

            @Override
            public List<NodeDocument> findAndUpdate(List<UpdateOp> updateOpList) {
                return store.findAndUpdate(Collection.NODES, updateOpList);
            }

            @Override
            public void setEnabled(boolean value) {
                // no-op
            }
        };
    }

    /**
     * Remove orphaned or deleted documents from the NODES collection
     * If bin is enabled, the document IDs are saved to the SETTINGS collection with ID prefixed with '/bin/'
     * If document ID cannot be saved then the removal of the document fails
     * If the bin is disabled, the document IDs are directly removed from the NODES collection
     *
     * @param orphanOrDeletedRemovalMap the keys of the documents to remove with the corresponding timestamps
     * @return the number of documents removed
     * @see DocumentStore#remove(Collection, Map)
     */
    int remove(Map<String, Long> orphanOrDeletedRemovalMap);

    /**
     * Performs a conditional update
     * If the bin is enabled, the removed properties are saved to the SETTINGS collection with ID prefixed with '/bin/' and empty value
     * If the document ID and properties  cannot be saved then the removal of the property fails
     * If bin is disabled, the removed properties are directly removed from the NODES collection
     *
     * @param updateOpList the update operation List
     * @return the list containing old documents
     * @see DocumentStore#findAndUpdate(Collection, List)
     */
    List<NodeDocument> findAndUpdate(List<UpdateOp> updateOpList);

    /**
     * Enable or disable the bin
     * @param value true to enable, false to disable
     */
    void setEnabled(boolean value);
}
