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

/**
 * This package contains the delegates for the various JCR API entities.
 * While the JCR API implementation classes directly implement the client
 * facing JCR API, they do so without implementing much of the business
 * logic themselves but rather rely on the functionality provided by the
 * delegates. This separation of the JCR functionality into external and
 * internal variants guarantees that JCR API consumers cannot gain access
 * to internals by simply casting to an implementation class. Further it
 * makes internal round-tripping through the JCR API unnecessary since
 * all functionality is provided by the internal interface (i.e. delegates),
 * which simplifies maintaining and checking state invariants.<p>
 *
 * <b>Responsibilities of JCR API implementation classes:</b>
 * <ul>
 * <li>Prevent access to internals.</li>
 * <li>Name/path mapping for both method arguments and return values.</li>
 * <li>Tracking and instantiation of other JCR implementation objects.</li>
 * <li>Conversion from JCR Values to PropertyState instances and vice versa.</li>
 * <li>No direct access to the Oak API</li>
 * </ul>
 *
 * <b>Responsibilities of the delegate classes:</b>
 * <ul>
 * <li>Business logic associated with complex JCR operations.</li>
 * <li>Provide means for state invariant checking.</li>
 * <li>Must no refer to neither the JCR API nor its implementation classes.</li>
 * </ul>
 */
package org.apache.jackrabbit.oak.jcr.delegate;

