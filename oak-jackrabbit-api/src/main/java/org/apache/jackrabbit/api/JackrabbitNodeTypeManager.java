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
package org.apache.jackrabbit.api;

import java.io.IOException;
import java.io.InputStream;

import javax.jcr.RepositoryException;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.NodeTypeManager;

import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 * The Jackrabbit node type manager interface. This interface contains the
 * Jackrabbit-specific extensions to the JCR {@link NodeTypeManager} interface.
 * <p>
 * Currently Jackrabbit provides a mechanism to register new node types, but
 * it is not possible to modify or remove existing node types.
 *
 * @deprecated Use standard JCR 2.0 API methods defined by
 * {@link NodeTypeManager} instead.
 */
public interface JackrabbitNodeTypeManager extends NodeTypeManager {

    /**
     * The standard XML content type to be used with XML-formatted
     * node type streams.
     */
    String TEXT_XML = "text/xml";

    /**
     * The experimental content type for the compact node type definition
     * files.
     */
    String TEXT_X_JCR_CND = "text/x-jcr-cnd";

    /**
     * Registers node types from the given node type XML stream.
     *
     * @param in node type XML stream
     * @return registered node types
     * @throws SAXException if the XML stream could not be read or parsed
     * @throws RepositoryException if the node types are invalid or another
     *                             repository error occurs
     */
    NodeType[] registerNodeTypes(InputSource in)
        throws SAXException, RepositoryException;

    /**
     * Registers node types from the given input stream of the given type.
     *
     * @param in node type stream
     * @param contentType type of the input stream
     * @return registered node types
     * @throws IOException if the input stream could not be read or parsed
     * @throws RepositoryException if the node types are invalid or another
     *                             repository error occurs
     */
    NodeType[] registerNodeTypes(InputStream in, String contentType)
        throws IOException, RepositoryException;

    /**
     * Checks if a node type with the given name is registered.
     *
     * @param name node type name
     * @return <code>true</code> if the named node type is registered
     *         <code>false</code> otherwise
     * @throws RepositoryException if an error occurs
     */
    boolean hasNodeType(String name) throws RepositoryException;

}
