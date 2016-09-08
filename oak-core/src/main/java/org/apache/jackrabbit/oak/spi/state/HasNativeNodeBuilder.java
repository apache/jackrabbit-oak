/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.spi.state;

/**
 *  Interface signalling that a specific <tt>NodeBuilder</tt> wraps and exposes another <tt>NodeBuilder</tt>.
 * 
 * <p>This is typically useful for wrapping or composite node builders in cases where direct
 * access to the native builder is needed. One such scenario assumptions made regarding the
 * implementation of a builder which is expected to inherit from a certain superclass.</p>
 *
 */
public interface HasNativeNodeBuilder {

    public NodeBuilder getNativeRootBuilder();
}
