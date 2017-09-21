package org.apache.jackrabbit.oak.spi.nodetype;

import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.nodetype.NodeDefinition;
import javax.jcr.nodetype.PropertyDefinition;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;

/**
 * Created by angela on 20/09/17.
 */
public interface EffectiveNodeType {
    boolean includesNodeType(String nodeTypeName);

    boolean includesNodeTypes(String[] nodeTypeNames);

    boolean supportsMixin(String mixin);

    Iterable<NodeDefinition> getNodeDefinitions();

    Iterable<PropertyDefinition> getPropertyDefinitions();

    Iterable<NodeDefinition> getAutoCreateNodeDefinitions();

    Iterable<PropertyDefinition> getAutoCreatePropertyDefinitions();

    Iterable<NodeDefinition> getMandatoryNodeDefinitions();

    Iterable<PropertyDefinition> getMandatoryPropertyDefinitions();

    @Nonnull
    Iterable<NodeDefinition> getNamedNodeDefinitions(
            String oakName);

    @Nonnull
    Iterable<PropertyDefinition> getNamedPropertyDefinitions(
            String oakName);

    @Nonnull
    Iterable<NodeDefinition> getResidualNodeDefinitions();

    @Nonnull
    Iterable<PropertyDefinition> getResidualPropertyDefinitions();

    void checkSetProperty(PropertyState property) throws RepositoryException;

    void checkRemoveProperty(PropertyState property) throws RepositoryException;

    void checkMandatoryItems(Tree tree) throws ConstraintViolationException;

    void checkOrderableChildNodes() throws UnsupportedRepositoryOperationException;

    PropertyDefinition getPropertyDefinition(
            String propertyName, boolean isMultiple,
            int type, boolean exactTypeMatch)
            throws ConstraintViolationException;

    PropertyDefinition getPropertyDefinition(String name, int type, boolean unknownMultiple);

    NodeDefinition getNodeDefinition(
            String childName, EffectiveNodeType childEffective)
            throws ConstraintViolationException;
}
