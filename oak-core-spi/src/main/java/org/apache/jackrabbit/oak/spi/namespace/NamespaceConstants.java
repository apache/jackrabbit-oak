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
package org.apache.jackrabbit.oak.spi.namespace;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import javax.jcr.NamespaceRegistry;

import org.apache.jackrabbit.JcrConstants;

/**
 * TODO document
 */
public interface NamespaceConstants {

    String REP_NAMESPACES = "rep:namespaces";

    String NAMESPACES_PATH = '/' + JcrConstants.JCR_SYSTEM + '/' + REP_NAMESPACES;

    // TODO: see http://java.net/jira/browse/JSR_333-50)
    String PREFIX_SV = "sv";
    String NAMESPACE_SV = "http://www.jcp.org/jcr/sv/1.0";

    String PREFIX_REP = "rep";
    String NAMESPACE_REP = "internal"; // TODO: see OAK-74

    // additional XML namespace
    String PREFIX_XMLNS = "xmlns";
    String NAMESPACE_XMLNS = "http://www.w3.org/2000/xmlns/";

    /**
     * Reserved namespace prefixes as defined in jackrabbit 2
     */
    Collection<String> RESERVED_PREFIXES = Collections.unmodifiableList(Arrays.asList(
            NamespaceRegistry.PREFIX_XML,
            NamespaceRegistry.PREFIX_JCR,
            NamespaceRegistry.PREFIX_NT,
            NamespaceRegistry.PREFIX_MIX,
            PREFIX_XMLNS,
            PREFIX_REP,
            PREFIX_SV
    ));

    /**
     * Reserved namespace URIs as defined in jackrabbit 2
     */
    Collection<String> RESERVED_URIS = Collections.unmodifiableList(Arrays.asList(
            NamespaceRegistry.NAMESPACE_XML,
            NamespaceRegistry.NAMESPACE_JCR,
            NamespaceRegistry.NAMESPACE_NT,
            NamespaceRegistry.NAMESPACE_MIX,
            NAMESPACE_XMLNS,
            NAMESPACE_REP,
            NAMESPACE_SV
    ));

    // index nodes for faster lookup

    String REP_NSDATA = "rep:nsdata";

    String REP_URIS = "rep:uris";

    String REP_PREFIXES = "rep:prefixes";

}
