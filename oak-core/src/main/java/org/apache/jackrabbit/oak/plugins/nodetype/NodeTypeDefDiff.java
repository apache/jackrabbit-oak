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
package org.apache.jackrabbit.oak.plugins.nodetype;

import javax.jcr.PropertyType;
import javax.jcr.nodetype.ItemDefinition;
import javax.jcr.nodetype.NodeDefinition;
import javax.jcr.nodetype.NodeTypeDefinition;
import javax.jcr.nodetype.PropertyDefinition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.jackrabbit.JcrConstants;

/**
 * A <code>NodeTypeDefDiff</code> represents the result of the comparison of
 * two node type definitions.
 * <p>
 * The result of the comparison can be categorized as one of the following types:
 * <p>
 * <b><code>NONE</code></b> indicates that there is no modification at all.
 * <p>
 * A <b><code>TRIVIAL</code></b> modification has no impact on the consistency
 * of existing content. The following modifications are considered
 * <code>TRIVIAL</code>:
 * <ul>
 * <li>changing node type <code>orderableChildNodes</code> flag
 * <li>changing node type <code>primaryItemName</code> value
 * <li>adding non-<code>mandatory</code> property/child node
 * <li>changing property/child node <code>protected</code> flag
 * <li>changing property/child node <code>onParentVersion</code> value
 * <li>changing property/child node <code>mandatory</code> flag to <code>false</code>
 * <li>changing property/child node <code>autoCreated</code> flag
 * <li>changing specific property/child node <code>name</code> to <code>*</code>
 * <li>changing child node <code>defaultPrimaryType</code>
 * <li>changing child node <code>sameNameSiblings</code> flag to <code>true</code>
 * <li>weaken child node <code>requiredPrimaryTypes</code> (e.g. by removing)
 * <li>weaken property <code>valueConstraints</code> (e.g. by removing a constraint
 * or by making a specific constraint less restrictive)
 * <li>changing property <code>defaultValues</code>
 * <li>changing specific property <code>requiredType</code> to <code>undefined</code>
 * <li>changing property <code>multiple</code> flag to <code>true</code>
 * </ul>
 * <p>
 * A <b><code>MAJOR</code></b> modification potentially <i>affects</i> the
 * consistency of existing content.
 *
 * All modifications that are not <b><code>TRIVIAL</code></b> are considered
 * <b><code>MAJOR</code></b>.
 *
 * <p>
 * <em>This class duplicates code from org.apache.jackrabbit.spi.commons.nodetype.NodeTypeDefDiff; both should be updated in sync,
 * see <a href="https://issues.apache.org/jira/browse/OAK-2802">OAK-2802</a></em>
 */
public class NodeTypeDefDiff {

    /**
     * no modification
     */
    public static final int NONE = 0;
    /**
     * trivial modification: does not affect consistency of existing content
     */
    public static final int TRIVIAL = 1;
    /**
     * major modification: <i>does</i> affect consistency of existing content
     */
    public static final int MAJOR = 2;

    private final NodeTypeDefinition oldDef;
    private final NodeTypeDefinition newDef;
    private int type;

    private final List<PropDefDiff> propDefDiffs = new ArrayList<PropDefDiff>();
    private final List<ChildNodeDefDiff> childNodeDefDiffs = new ArrayList<ChildNodeDefDiff>();

    /**
     * Constructor
     * @param oldDef old definition
     * @param newDef new definition
     */
    private NodeTypeDefDiff(NodeTypeDefinition oldDef, NodeTypeDefinition newDef) {
        this.oldDef = oldDef;
        this.newDef = newDef;
        init();
    }

    /**
     *
     */
    private void init() {
        if (oldDef.equals(newDef)) {
            // definitions are identical
            type = NONE;
        } else {
            // definitions are not identical, determine type of modification

            // assume TRIVIAL change by default
            type = TRIVIAL;

            // check supertypes
            int tmpType = supertypesDiff();
            if (tmpType > type) {
                type = tmpType;
            }

            // check mixin flag (MAJOR modification)
            tmpType = mixinFlagDiff();
            if (tmpType > type) {
                type = tmpType;
            }

            // check abstract flag (MAJOR modification)
            tmpType = abstractFlagDiff();
            if (tmpType > type) {
                type = tmpType;
            }

            // no need to check orderableChildNodes flag (TRIVIAL modification)
            // no need to check queryable flag (TRIVIAL modification)

            // check property definitions
            tmpType = buildPropDefDiffs();
            if (tmpType > type) {
                type = tmpType;
            }

            // check child node definitions
            tmpType = buildChildNodeDefDiffs();
            if (tmpType > type) {
                type = tmpType;
            }
        }
    }

    /**
     * @param oldDef old definition
     * @param newDef new definition
     * @return the diff
     */
    public static NodeTypeDefDiff create(NodeTypeDefinition oldDef, NodeTypeDefinition newDef) {
        if (oldDef == null || newDef == null) {
            throw new IllegalArgumentException("arguments can not be null");
        }
        if (!oldDef.getName().equals(newDef.getName())) {
            throw new IllegalArgumentException("at least node type names must be matching");
        }
        return new NodeTypeDefDiff(oldDef, newDef);
    }

    /**
     * @return <code>true</code> if modified
     */
    public boolean isModified() {
        return type != NONE;
    }

    /**
     * @return <code>true</code> if trivial
     */
    public boolean isTrivial() {
        return type == TRIVIAL;
    }

    /**
     * @return <code>true</code> if major
     */
    public boolean isMajor() {
        return type == MAJOR;
    }

    /**
     * Returns the type of modification as expressed by the following constants:
     * <ul>
     * <li><b><code>NONE</code></b>: no modification at all
     * <li><b><code>TRIVIAL</code></b>: does not affect consistency of
     * existing content
     * <li><b><code>MAJOR</code></b>: <i>does</i> affect consistency of existing
     * content
     * </ul>
     *
     * @return the type of modification
     */
    public int getType() {
        return type;
    }

    /**
     * @return <code>true</code> if mixin flag diff
     */
    public int mixinFlagDiff() {
        return oldDef.isMixin() != newDef.isMixin() ? MAJOR : NONE;
    }

    /**
     * @return <code>true</code> if abstract flag diff
     */
    public int abstractFlagDiff() {
        return oldDef.isAbstract() && !newDef.isAbstract() ? MAJOR : NONE;
    }

    /**
     * @return <code>true</code> if supertypes diff
     */
    public int supertypesDiff() {
        Set<String> set1 = getDeclaredSuperTypeNames(oldDef);
        Set<String> set2 = getDeclaredSuperTypeNames(newDef);
        return !set1.equals(set2) ? MAJOR : NONE;
    }

    /**
     * Returns the set of declared supertype names without 'nt:base', which is
     * irrelevant for a diff of supertypes.
     *
     * @param def a NodeTypeDefinition.
     * @return the set of declared supertype names.
     */
    private Set<String> getDeclaredSuperTypeNames(NodeTypeDefinition def) {
        Set<String> names = new HashSet<String>(Arrays.asList(def.getDeclaredSupertypeNames()));
        names.remove(JcrConstants.NT_BASE);
        return names;
    }

    /**
     * @return diff type
     */
    private int buildPropDefDiffs() {
        int maxType = NONE;
        Map<PropertyDefinitionId, PropertyDefinition> oldDefs = new HashMap<PropertyDefinitionId, PropertyDefinition>();
        for (PropertyDefinition def : oldDef.getDeclaredPropertyDefinitions()) {
            oldDefs.put(new PropertyDefinitionId(def), def);
        }

        Map<PropertyDefinitionId, PropertyDefinition> newDefs = new HashMap<PropertyDefinitionId, PropertyDefinition>();
        for (PropertyDefinition def : newDef.getDeclaredPropertyDefinitions()) {
            newDefs.put(new PropertyDefinitionId(def), def);
        }

        /**
         * walk through defs1 and process all entries found in
         * both defs1 & defs2 and those found only in defs1
         */
        for (Map.Entry<PropertyDefinitionId, PropertyDefinition> entry : oldDefs.entrySet()) {
            PropertyDefinitionId id = entry.getKey();
            PropertyDefinition def1 = entry.getValue();
            PropertyDefinition def2 = newDefs.get(id);
            PropDefDiff diff = new PropDefDiff(def1, def2);
            if (diff.getType() > maxType) {
                maxType = diff.getType();
            }
            propDefDiffs.add(diff);
            newDefs.remove(id);
        }

        /**
         * defs2 by now only contains entries found in defs2 only;
         * walk through defs2 and process all remaining entries
         */
        for (Map.Entry<PropertyDefinitionId, PropertyDefinition> entry : newDefs.entrySet()) {
            PropertyDefinition def = entry.getValue();
            PropDefDiff diff = new PropDefDiff(null, def);
            if (diff.getType() > maxType) {
                maxType = diff.getType();
            }
            propDefDiffs.add(diff);
        }

        return maxType;
    }

    /**
     * @return diff type
     */
    private int buildChildNodeDefDiffs() {
        int maxType = NONE;
        final Map<NodeDefinitionId, List<NodeDefinition>> oldDefs = collectChildNodeDefs(oldDef.getDeclaredChildNodeDefinitions());
        final Map<NodeDefinitionId, List<NodeDefinition>> newDefs = collectChildNodeDefs(newDef.getDeclaredChildNodeDefinitions());

        for (NodeDefinitionId defId : oldDefs.keySet()) {
            final ChildNodeDefDiffs childNodeDefDiffs = new ChildNodeDefDiffs(oldDefs.get(defId), newDefs.get(defId));
            this.childNodeDefDiffs.addAll(childNodeDefDiffs.getChildNodeDefDiffs());
            newDefs.remove(defId);
        }

        for (NodeDefinitionId defId : newDefs.keySet()) {
            final ChildNodeDefDiffs childNodeDefDiffs = new ChildNodeDefDiffs(null, newDefs.get(defId));
            this.childNodeDefDiffs.addAll(childNodeDefDiffs.getChildNodeDefDiffs());
        }

        for (ChildNodeDefDiff diff : childNodeDefDiffs) {
            if (diff.getType() > maxType) {
                maxType = diff.getType();
            }
        }

        return maxType;
    }

    private Map<NodeDefinitionId, List<NodeDefinition>> collectChildNodeDefs(final NodeDefinition[] cnda1) {
        Map<NodeDefinitionId, List<NodeDefinition>> defs1 = new HashMap<NodeDefinitionId, List<NodeDefinition>>();
        for (NodeDefinition def1 : cnda1) {
            final NodeDefinitionId def1Id = new NodeDefinitionId(def1);
            List<NodeDefinition> list = defs1.get(def1Id);
            if (list == null) {
                list = new ArrayList<NodeDefinition>();
                defs1.put(def1Id, list);
            }
            list.add(def1);
        }
        return defs1;
    }

    @Override
    public String toString() {
        String result = getClass().getName() + "[\n\tnodeTypeName="
                + oldDef.getName();

        result += ",\n\tmixinFlagDiff=" + modificationTypeToString(mixinFlagDiff());
        result += ",\n\tsupertypesDiff=" + modificationTypeToString(supertypesDiff());

        result += ",\n\tpropertyDifferences=[\n";
        result += toString(propDefDiffs);
        result += "\t]";

        result += ",\n\tchildNodeDifferences=[\n";
        result += toString(childNodeDefDiffs);
        result += "\t]\n";
        result += "]\n";

        return result;
    }

    private String toString(List<? extends ChildItemDefDiff> childItemDefDiffs) {
        String result = "";
        for (Iterator<? extends ChildItemDefDiff> iter = childItemDefDiffs.iterator(); iter.hasNext();) {
            ChildItemDefDiff propDefDiff = iter.next();
            result += "\t\t" + propDefDiff;
            if (iter.hasNext()) {
                result += ",";
            }
            result += "\n";
        }
        return result;
    }

    private String modificationTypeToString(int modificationType) {
        String typeString = "unknown";
        switch (modificationType) {
            case NONE:
                typeString = "NONE";
                break;
            case TRIVIAL:
                typeString = "TRIVIAL";
                break;
            case MAJOR:
                typeString = "MAJOR";
                break;
        }
        return typeString;
    }


    //--------------------------------------------------------< inner classes >

    abstract class ChildItemDefDiff {
        protected final ItemDefinition oldDef;
        protected final ItemDefinition newDef;
        protected int type;

        ChildItemDefDiff(ItemDefinition oldDef, ItemDefinition newDef) {
            this.oldDef = oldDef;
            this.newDef = newDef;
            init();
        }

        protected void init() {
            // determine type of modification
            if (isAdded()) {
                if (!newDef.isMandatory()) {
                    // adding a non-mandatory child item is a TRIVIAL change
                    type = TRIVIAL;
                } else {
                    // adding a mandatory child item is a MAJOR change
                    type = MAJOR;
                }
            } else if (isRemoved()) {
                // removing a child item is a MAJOR change
                type = MAJOR;
            } else {
                /**
                 * neither added nor removed => has to be either identical
                 * or modified
                 */
                if (oldDef.equals(newDef)) {
                    // identical
                    type = NONE;
                } else {
                    // modified
                    if (oldDef.isMandatory() != newDef.isMandatory()
                            && newDef.isMandatory()) {
                        // making a child item mandatory is a MAJOR change
                        type = MAJOR;
                    } else {
                        if (!"*".equals(oldDef.getName())
                                && "*".equals(newDef.getName())) {
                            // just making a child item residual is a TRIVIAL change
                            type = TRIVIAL;
                        } else {
                            if (!oldDef.getName().equals(newDef.getName())) {
                                // changing the name of a child item is a MAJOR change
                                type = MAJOR;
                            } else {
                                // all other changes are TRIVIAL
                                type = TRIVIAL;
                            }
                        }
                    }
                }
            }
        }

        public int getType() {
            return type;
        }

        public boolean isAdded() {
            return oldDef == null && newDef != null;
        }

        public boolean isRemoved() {
            return oldDef != null && newDef == null;
        }

        public boolean isModified() {
            return oldDef != null && newDef != null
                    && !oldDef.equals(newDef);
        }

        @Override
        public String toString() {
            String typeString = modificationTypeToString(getType());

            String operationString;
            if (isAdded()) {
                operationString = "ADDED";
            } else if (isModified()) {
                operationString = "MODIFIED";
            } else if (isRemoved()) {
                operationString = "REMOVED";
            } else {
                operationString = "NONE";
            }

            ItemDefinition itemDefinition = (oldDef != null) ? oldDef : newDef;

            return getClass().getName() + "[itemName="
                    + itemDefinition.getName() + ", type=" + typeString
                    + ", operation=" + operationString + "]";
        }

    }

    public class PropDefDiff extends ChildItemDefDiff {

        PropDefDiff(PropertyDefinition oldDef, PropertyDefinition newDef) {
            super(oldDef, newDef);
        }

        public PropertyDefinition getOldDef() {
            return (PropertyDefinition) oldDef;
        }

        public PropertyDefinition getNewDef() {
            return (PropertyDefinition) newDef;
        }

        @Override
        protected void init() {
            super.init();
            /**
             * only need to do comparison if base class implementation
             * detected a non-MAJOR (i.e. TRIVIAL) modification;
             * no need to check for additions or removals as this is already
             * handled in base class implementation.
             */
            if (isModified() && type == TRIVIAL) {
                // check if valueConstraints were made more restrictive
                String[] vca1 = getOldDef().getValueConstraints();
                Set<String> set1 = new HashSet<String>();
                for (String aVca1 : vca1) {
                    set1.add(aVca1);
                }
                String[] vca2 = getNewDef().getValueConstraints();
                Set<String> set2 = new HashSet<String>();
                for (String aVca2 : vca2) {
                    set2.add(aVca2);
                }

                if (!set1.equals(set2)) {
                    // valueConstraints have been modified
                    if (set2.isEmpty()) {
                        // all existing constraints have been cleared
                        // => TRIVIAL change
                        type = TRIVIAL;
                    } else if (set1.isEmpty()) {
                        // constraints have been set on a previously unconstrained property
                        // => MAJOR change
                        type = MAJOR;
                    } else if (set2.containsAll(set1)) {
                        // new set is a superset of old set,
                        // i.e. constraints have been weakened
                        // (since constraints are OR'ed)
                        // => TRIVIAL change
                        type = TRIVIAL;
                    } else {
                        // constraint have been removed/modified (MAJOR change);
                        // since we're unable to semantically compare
                        // value constraints (e.g. regular expressions), all
                        // such modifications are considered a MAJOR change.
                        type = MAJOR;
                    }
                }

                // no need to check defaultValues (TRIVIAL change)
                // no need to check availableQueryOperators (TRIVIAL change)
                // no need to check queryOrderable (TRIVIAL change)

                if (type == TRIVIAL) {
                    int t1 = getOldDef().getRequiredType();
                    int t2 = getNewDef().getRequiredType();
                    if (t1 != t2) {
                        if (t2 == PropertyType.UNDEFINED) {
                            // changed getRequiredType to UNDEFINED (TRIVIAL change)
                            type = TRIVIAL;
                        } else {
                            // changed getRequiredType to specific type (MAJOR change)
                            type = MAJOR;
                        }
                    }
                    boolean b1 = getOldDef().isMultiple();
                    boolean b2 = getNewDef().isMultiple();
                    if (b1 != b2) {
                        if (b2) {
                            // changed multiple flag to true (TRIVIAL change)
                            type = TRIVIAL;
                        } else {
                            // changed multiple flag to false (MAJOR change)
                            type = MAJOR;
                        }
                    }
                }
            }
        }
    }

    public class ChildNodeDefDiff extends ChildItemDefDiff {

        ChildNodeDefDiff(NodeDefinition oldDef, NodeDefinition newDef) {
            super(oldDef, newDef);
        }

        public NodeDefinition getOldDef() {
            return (NodeDefinition) oldDef;
        }

        public NodeDefinition getNewDef() {
            return (NodeDefinition) newDef;
        }

        @Override
        protected void init() {
            super.init();
            /**
             * only need to do comparison if base class implementation
             * detected a non-MAJOR (i.e. TRIVIAL) modification;
             * no need to check for additions or removals as this is already
             * handled in base class implementation.
             */
            if (isModified() && type == TRIVIAL) {

                boolean b1 = getOldDef().allowsSameNameSiblings();
                boolean b2 = getNewDef().allowsSameNameSiblings();
                if (b1 != b2 && !b2) {
                    // changed sameNameSiblings flag to false (MAJOR change)
                    type = MAJOR;
                }

                // no need to check defaultPrimaryType (TRIVIAL change)

                if (type == TRIVIAL) {
                    Set<String> s1 = new HashSet<String>(Arrays.asList(getOldDef().getRequiredPrimaryTypeNames()));
                    Set<String> s2 = new HashSet<String>(Arrays.asList(getNewDef().getRequiredPrimaryTypeNames()));
                    // normalize sets by removing nt:base (adding/removing nt:base is irrelevant for the diff)
                    s1.remove("nt:base");
                    s2.remove("nt:base");
                    if (!s1.equals(s2)) {
                        // requiredPrimaryTypes have been modified
                        if (s1.containsAll(s2)) {
                            // old list is a superset of new list
                            // => removed requiredPrimaryType (TRIVIAL change)
                            type = TRIVIAL;
                        } else {
                            // added/modified requiredPrimaryType (MAJOR change)
                            // todo check whether aggregate of old requiredTypes would include aggregate of new requiredTypes => trivial change
                            type = MAJOR;
                        }
                    }
                }
            }
        }
    }

    /**
     * Identifier used to identify corresponding property definitions
     */
    static class PropertyDefinitionId {

        String declaringNodeType;
        String name;
        boolean definesResidual;

        PropertyDefinitionId(PropertyDefinition def) {
            declaringNodeType = def.getDeclaringNodeType().getName();
            name = def.getName();
            definesResidual = "*".equals(def.getName());
        }

        //---------------------------------------< java.lang.Object overrides >
        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj instanceof PropertyDefinitionId) {
                PropertyDefinitionId other = (PropertyDefinitionId) obj;
                return declaringNodeType.equals(other.declaringNodeType)
                        && name.equals(other.name)
                        && definesResidual == other.definesResidual;
            }
            return false;
        }

        @Override
        public int hashCode() {
            int h = 17;
            h = 37 * h + declaringNodeType.hashCode();
            h = 37 * h + name.hashCode();
            h = 37 * h + (definesResidual ? 11 : 43);
            return h;
        }
    }

    /**
     * Identifier used to identify corresponding node definitions
     */
    static class NodeDefinitionId {

        String declaringNodeType;
        String name;

        NodeDefinitionId(NodeDefinition def) {
            declaringNodeType = def.getDeclaringNodeType().getName();
            name = def.getName();
        }

        //---------------------------------------< java.lang.Object overrides >
        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj instanceof NodeDefinitionId) {
                NodeDefinitionId other = (NodeDefinitionId) obj;
                return declaringNodeType.equals(other.declaringNodeType)
                        && name.equals(other.name);
            }
            return false;
        }

        @Override
        public int hashCode() {
            int h = 17;
            h = 37 * h + declaringNodeType.hashCode();
            h = 37 * h + name.hashCode();
            return h;
        }
    }

    private class ChildNodeDefDiffs {

        private final List<NodeDefinition> defs1;
        private final List<NodeDefinition> defs2;

        private ChildNodeDefDiffs(final List<NodeDefinition> defs1, final List<NodeDefinition> defs2) {
            this.defs1 = defs1 != null ? defs1 : Collections.<NodeDefinition>emptyList();
            this.defs2 = defs2 != null ? defs2 : Collections.<NodeDefinition>emptyList();
        }

        private Collection<ChildNodeDefDiff> getChildNodeDefDiffs() {
            // gather all possible combinations of diffs
            final List<ChildNodeDefDiff> diffs = new ArrayList<ChildNodeDefDiff>();
            for (NodeDefinition def1 : defs1) {
                for (NodeDefinition def2 : defs2) {
                    diffs.add(new ChildNodeDefDiff(def1, def2));
                }
            }
            if (defs2.size() < defs1.size()) {
                for (NodeDefinition def1 : defs1) {
                    diffs.add(new ChildNodeDefDiff(def1, null));
                }
            }
            if (defs1.size() < defs2.size()) {
                for (NodeDefinition def2 : defs2) {
                    diffs.add(new ChildNodeDefDiff(null, def2));
                }
            }
            // sort them according to decreasing compatibility
            Collections.sort(diffs, new Comparator<ChildNodeDefDiff>() {
                @Override
                public int compare(final ChildNodeDefDiff o1, final ChildNodeDefDiff o2) {
                    return o1.getType() - o2.getType();
                }
            });
            // select the most compatible ones
            final int size = defs1.size() > defs2.size() ? defs1.size() : defs2.size();
            AtomicInteger allowedNewNull = new AtomicInteger(defs1.size() - defs2.size());
            AtomicInteger allowedOldNull = new AtomicInteger(defs2.size() - defs1.size());
            final List<ChildNodeDefDiff> results = new ArrayList<ChildNodeDefDiff>();
            for (ChildNodeDefDiff diff : diffs) {
                if (!alreadyMatched(results, diff.getNewDef(), diff.getOldDef(), allowedNewNull, allowedOldNull)) {
                    results.add(diff);
                    if (diff.getNewDef() == null) {
                        allowedNewNull.decrementAndGet();
                    }
                    if (diff.getOldDef() == null) {
                        allowedOldNull.decrementAndGet();
                    }
                }
                if (results.size() == size) {
                    break;
                }
            }
            return results;
        }

        private boolean alreadyMatched(final List<ChildNodeDefDiff> result, final NodeDefinition newDef, final NodeDefinition oldDef, final AtomicInteger allowedNewNull, final AtomicInteger allowedOldNull) {
            boolean containsNewDef = false, containsOldDef = false;
            for (ChildNodeDefDiff d : result) {
                if (d.getNewDef() != null && d.getNewDef().equals(newDef)) {
                    containsNewDef = true;
                    break;
                }
                if (d.getOldDef() != null && d.getOldDef().equals(oldDef)) {
                    containsOldDef = true;
                    break;
                }
            }
            if (oldDef == null) {
                if (allowedOldNull.get() < 1) {
                    containsOldDef = true;
                }
            }
            if (newDef == null) {
                if (allowedNewNull.get() < 1) {
                    containsNewDef = true;
                }
            }

            return containsNewDef || containsOldDef;
        }
    }

}
