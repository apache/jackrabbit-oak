/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.search.spi.editor;

import java.io.IOException;
import java.util.List;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * A {@link DocumentMaker} is responsible for creating an instance of a document D to be indexed.
 * For Apache Lucene that would be a Lucene {@code Document}, for Apache Solr that might be a {@code SolrInputDocument}, etc.
 */
public interface DocumentMaker<D> {

  /**
   * create a document from the current state and list of modified properties
   * @param state the node state
   * @param isUpdate whether it is an update or not
   * @param propertiesModified the list of modified properties
   * @return a document to be indexed
   * @throws IOException whether node state read operations or document creation fail
   */
  D makeDocument(NodeState state, boolean isUpdate, List<PropertyState> propertiesModified) throws IOException;

}
