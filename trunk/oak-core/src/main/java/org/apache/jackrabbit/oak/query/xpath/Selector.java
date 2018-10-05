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
package org.apache.jackrabbit.oak.query.xpath;

/**
 * A selector.
 */
class Selector {

    /**
     * The selector name.
     */
    String name;
    
    /**
     * Whether this is the only selector in the query.
     */
    boolean onlySelector;
    
    /**
     * The node type, if set, or null.
     */
    String nodeType;
    
    /**
     * Whether this is a child node of the previous selector or a given path.
     * Examples:
     * <ul><li>/jcr:root/*
     * </li><li>/jcr:root/test/*
     * </li><li>/jcr:root/element()
     * </li><li>/jcr:root/element(*)
     * </li></ul>
     */
    boolean isChild;
    
    /**
     * Whether this is a parent node of the previous selector or given path.
     * Examples:
     * <ul><li>testroot//child/..[@foo1]
     * </li><li>/jcr:root/test/descendant/..[@test]
     * </li></ul>
     */
    boolean isParent;
    
    /**
     * Whether this is a descendant of the previous selector or a given path.
     * Examples:
     * <ul><li>/jcr:root//descendant
     * </li><li>/jcr:root/test//descendant
     * </li><li>/jcr:root[@x]
     * </li><li>/jcr:root (just by itself)
     * </li></ul>
     */
    boolean isDescendant;
    
    /**
     * The path (only used for the first selector).
     */
    String path = "";
    
    /**
     * The node name, if set.
     */
    String nodeName;
    
    /**
     * The condition for this selector.
     */
    Expression condition;
    
    /**
     * The join condition from the previous selector.
     */
    Expression joinCondition;
    
    public Selector() {
    }

    public Selector(Selector s) {
        this.name = s.name;
        this.onlySelector = s.onlySelector;
        this.nodeType = s.nodeType;
        this.isChild = s.isChild;
        this.isParent = s.isParent;
        this.isDescendant = s.isDescendant;
        this.path = s.path;
        this.nodeName = s.nodeName;
        this.condition = s.condition;
        this.joinCondition = s.joinCondition;
    }
    
}