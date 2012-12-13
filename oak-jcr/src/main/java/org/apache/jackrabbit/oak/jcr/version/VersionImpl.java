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
package org.apache.jackrabbit.oak.jcr.version;

import java.util.Calendar;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.version.Version;
import javax.jcr.version.VersionHistory;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.jcr.NodeDelegate;
import org.apache.jackrabbit.oak.jcr.NodeImpl;
import org.apache.jackrabbit.oak.util.TODO;

class VersionImpl extends NodeImpl<VersionDelegate> implements Version {

    public VersionImpl(VersionDelegate dlg) {
        super(dlg);
    }

    @Override
    public VersionHistory getContainingHistory() throws RepositoryException {
        return TODO.unimplemented().returnValue(null);
    }

    @Override
    public Calendar getCreated() throws RepositoryException {
        return TODO.unimplemented().returnValue(Calendar.getInstance());
    }

    @Override
    public Version getLinearPredecessor() throws RepositoryException {
        return TODO.unimplemented().returnValue(null);
    }

    @Override
    public Version getLinearSuccessor() throws RepositoryException {
        return TODO.unimplemented().returnValue(null);
    }

    @Override
    public Version[] getPredecessors() throws RepositoryException {
        return TODO.unimplemented().returnValue(new Version[0]);
    }

    @Override
    public Version[] getSuccessors() throws RepositoryException {
        return TODO.unimplemented().returnValue(new Version[0]);
    }

    @Override
    public Node getFrozenNode() throws RepositoryException {
        return TODO.unimplemented().returnValue(null);
    }

}
