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

import javax.jcr.Session;
import javax.transaction.xa.XAResource;

/**
 * The XASession interface extends the capability of {@link Session} by adding
 * access to a JCR repository's support for the Java Transaction API (JTA).
 * <p>
 * This support takes the form of a {@link javax.transaction.xa.XAResource}
 * object. The functionality of this object closely resembles that defined by
 * the standard X/Open XA Resource interface.
 * <p>
 * This interface is used by the transaction manager; an application does not
 * use it directly.
 *
 * @since 1.4
 * @deprecated An XA-enabled session should directly implement the
 *             {@link javax.transaction.xa.XAResource} interface
 */
public interface XASession extends Session {

    /**
     * Retrieves an {@link XAResource} object that the transaction manager
     * will use to manage this XASession object's participation in
     * a distributed transaction.
     *
     * @return the {@link XAResource} object.
     */
    XAResource getXAResource();

}
