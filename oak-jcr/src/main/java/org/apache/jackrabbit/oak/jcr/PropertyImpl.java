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
package org.apache.jackrabbit.oak.jcr;

import org.apache.jackrabbit.oak.api.Branch;
import org.apache.jackrabbit.oak.api.ContentTree;
import org.apache.jackrabbit.oak.api.ContentTree.Status;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.jcr.util.LogUtil;
import org.apache.jackrabbit.oak.jcr.util.ValueConverter;
import org.apache.jackrabbit.oak.namepath.Paths;
import org.apache.jackrabbit.value.ValueHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.Binary;
import javax.jcr.Item;
import javax.jcr.ItemNotFoundException;
import javax.jcr.ItemVisitor;
import javax.jcr.Node;
import javax.jcr.PathNotFoundException;
import javax.jcr.Property;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFormatException;
import javax.jcr.nodetype.PropertyDefinition;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.Calendar;

/**
 * {@code PropertyImpl}...
 */
public class PropertyImpl extends ItemImpl implements Property {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(PropertyImpl.class);

    private ContentTree parent;
    private PropertyState propertyState;

    PropertyImpl(SessionContext<SessionImpl> sessionContext, ContentTree parent,
            PropertyState propertyState) {

        super(sessionContext);
        this.parent = parent;
        this.propertyState = propertyState;
    }

    //---------------------------------------------------------------< Item >---
    /**
     * @see javax.jcr.Item#isNode()
     */
    @Override
    public boolean isNode() {
        return false;
    }

    /**
     * @see javax.jcr.Item#getName()
     */
    @Override
    public String getName() throws RepositoryException {
        return name();
    }

    /**
     * @see javax.jcr.Property#getPath() ()
     */
    @Override
    public String getPath() throws RepositoryException {
        return path();
    }

    /**
     * @see javax.jcr.Item#getParent()
     */
    @Override
    public Node getParent() throws RepositoryException {
        return new NodeImpl(sessionContext, getParentContentTree());
    }

    /**
     * @see Item#getAncestor(int)
     */
    @Override
    public Item getAncestor(int depth) throws RepositoryException {
        if (depth == getDepth() - 1) {
            return getParent();
        } else {
            return getParent().getAncestor(depth);
        }
    }

    /**
     * @see javax.jcr.Item#getDepth()
     */
    @Override
    public int getDepth() throws RepositoryException {
        return Paths.getDepth(getPath());
    }

    /**
     * @see javax.jcr.Item#isNew()
     */
    @Override
    public boolean isNew() {
        return getParentContentTree().getPropertyStatus(name()) == Status.NEW;
    }

    /**
     * @see javax.jcr.Item#isModified() ()
     */
    @Override
    public boolean isModified() {
        return getParentContentTree().getPropertyStatus(name()) == Status.MODIFIED;
    }

    /**
     * @see javax.jcr.Item#isNew()
     */
    @Override
    public void remove() throws RepositoryException {
        getParentContentTree().removeProperty(name());
    }

    /**
     * @see javax.jcr.Item#accept(javax.jcr.ItemVisitor)
     */
    @Override
    public void accept(ItemVisitor visitor) throws RepositoryException {
        checkStatus();
        visitor.visit(this);
    }

    //-----------------------------------------------------------< Property >---
    /**
     * @see Property#setValue(Value)
     */
    @Override
    public void setValue(Value value) throws RepositoryException {
        checkStatus();

        int valueType = (value != null) ? value.getType() : PropertyType.UNDEFINED;
        int reqType = getRequiredType(valueType);
        setValue(value, reqType);
    }

    /**
     * @see Property#setValue(Value[])
     */
    @Override
    public void setValue(Value[] values) throws RepositoryException {
        checkStatus();

        // assert equal types for all values entries
        int valueType = PropertyType.UNDEFINED;
        if (values != null) {
            for (Value value : values) {
                if (value == null) {
                    // skip null values as those will be purged later
                    continue;
                }
                if (valueType == PropertyType.UNDEFINED) {
                    valueType = value.getType();
                }
                else if (valueType != value.getType()) {
                    String msg = "Inhomogeneous type of values (" + LogUtil.safeGetJCRPath(this) + ')';
                    log.debug(msg);
                    throw new ValueFormatException(msg);
                }
            }
        }

        int reqType = getRequiredType(valueType);
        setValues(values, reqType);
    }

    /**
     * @see Property#setValue(String)
     */
    @Override
    public void setValue(String value) throws RepositoryException {
        checkStatus();

        int reqType = getRequiredType(PropertyType.STRING);
        if (value == null) {
            setValue(null, reqType);
        } else {
            setValue(getValueFactory().createValue(value), reqType);
        }
    }

    /**
     * @see Property#setValue(String[])
     */
    @Override
    public void setValue(String[] values) throws RepositoryException {
        checkStatus();

        int reqType = getRequiredType(PropertyType.STRING);
        Value[] vs;
        if (values == null) {
            vs = null;
        } else {
            vs = new Value[values.length];
            for (int i = 0; i < values.length; i++) {
                vs[i] = getValueFactory().createValue(values[i]);
            }
        }
        setValues(vs, reqType);
    }

    /**
     * @see Property#setValue(InputStream)
     */
    @SuppressWarnings("deprecation")
    @Override
    public void setValue(InputStream value) throws RepositoryException {
        checkStatus();

        int reqType = getRequiredType(PropertyType.BINARY);
        if (value == null) {
            setValue(null, reqType);
        } else {
            setValue(getValueFactory().createValue(value), reqType);
        }
    }

    /**
     * @see Property#setValue(Binary)
     */
    @Override
    public void setValue(Binary value) throws RepositoryException {
        checkStatus();

        int reqType = getRequiredType(PropertyType.BINARY);
        if (value == null) {
            setValue(null, reqType);
        } else {
            setValue(getValueFactory().createValue(value), reqType);
        }
    }

    /**
     * @see Property#setValue(long)
     */
    @Override
    public void setValue(long value) throws RepositoryException {
        checkStatus();

        int reqType = getRequiredType(PropertyType.LONG);
        setValue(getValueFactory().createValue(value), reqType);
    }

    /**
     * @see Property#setValue(double)
     */
    @Override
    public void setValue(double value) throws RepositoryException {
        checkStatus();

        int reqType = getRequiredType(PropertyType.DOUBLE);
        setValue(getValueFactory().createValue(value), reqType);
    }

    /**
     * @see Property#setValue(BigDecimal)
     */
    @Override
    public void setValue(BigDecimal value) throws RepositoryException {
        checkStatus();

        int reqType = getRequiredType(PropertyType.DECIMAL);
        setValue(getValueFactory().createValue(value), reqType);
    }

    /**
     * @see Property#setValue(Calendar)
     */
    @Override
    public void setValue(Calendar value) throws RepositoryException {
        checkStatus();

        int reqType = getRequiredType(PropertyType.DATE);
        if (value == null) {
            setValue(null, reqType);
        } else {
            setValue(getValueFactory().createValue(value), reqType);
        }
    }

    /**
     * @see Property#setValue(boolean)
     */
    @Override
    public void setValue(boolean value) throws RepositoryException {
        checkStatus();

        int reqType = getRequiredType(PropertyType.BOOLEAN);
        setValue(getValueFactory().createValue(value), reqType);
    }

    /**
     * @see Property#setValue(javax.jcr.Node)
     */
    @Override
    public void setValue(Node value) throws RepositoryException {
        checkStatus();

        int reqType = getRequiredType(PropertyType.REFERENCE);
        if (value == null) {
            setValue(null, reqType);
        } else {
            setValue(getValueFactory().createValue(value), reqType);
        }
    }

    @Override
    public Value getValue() throws RepositoryException {
        checkStatus();
        if (isMultiple()) {
            throw new ValueFormatException(LogUtil.safeGetJCRPath(this) + " is multi-valued.");
        }

        return ValueConverter.toValue(getValueFactory(), getPropertyState().getScalar());
    }

    @Override
    public Value[] getValues() throws RepositoryException {
        checkStatus();
        if (!isMultiple()) {
            throw new ValueFormatException(LogUtil.safeGetJCRPath(this) + " is not multi-valued.");
        }

        return ValueConverter.toValues(getValueFactory(), getPropertyState().getArray());
    }

    /**
     * @see Property#getString()
     */
    @Override
    public String getString() throws RepositoryException {
        return getValue().getString();
    }

    /**
     * @see Property#getStream()
     */
    @SuppressWarnings("deprecation")
    @Override
    public InputStream getStream() throws RepositoryException {
        return getValue().getStream();
    }

    /**
     * @see javax.jcr.Property#getBinary()
     */
    @Override
    public Binary getBinary() throws RepositoryException {
        return getValue().getBinary();
    }

    /**
     * @see Property#getLong()
     */
    @Override
    public long getLong() throws RepositoryException {
        return getValue().getLong();
    }

    /**
     * @see Property#getDouble()
     */
    @Override
    public double getDouble() throws RepositoryException {
        return getValue().getDouble();
    }

    /**
     * @see Property#getDecimal()
     */
    @Override
    public BigDecimal getDecimal() throws RepositoryException {
        return getValue().getDecimal();
    }

    /**
     * @see Property#getDate()
     */
    @Override
    public Calendar getDate() throws RepositoryException {
        return getValue().getDate();
    }

    /**
     * @see Property#getBoolean()
     */
    @Override
    public boolean getBoolean() throws RepositoryException {
        return getValue().getBoolean();
    }

    /**
     * @see javax.jcr.Property#getNode()
     */
    @Override
    public Node getNode() throws RepositoryException {
        Value value = getValue();
        switch (value.getType()) {
            case PropertyType.REFERENCE:
            case PropertyType.WEAKREFERENCE:
                return getSession().getNodeByIdentifier(value.getString());

            case PropertyType.PATH:
            case PropertyType.NAME:
                String path = value.getString();
                try {
                    return (path.charAt(0) == '/') ? getSession().getNode(path) : getParent().getNode(path);
                } catch (PathNotFoundException e) {
                    throw new ItemNotFoundException(path);
                }

            case PropertyType.STRING:
                try {
                    Value refValue = ValueHelper.convert(value, PropertyType.REFERENCE, getValueFactory());
                    return getSession().getNodeByIdentifier(refValue.getString());
                } catch (ItemNotFoundException e) {
                    throw e;
                } catch (RepositoryException e) {
                    // try if STRING value can be interpreted as PATH value
                    Value pathValue = ValueHelper.convert(value, PropertyType.PATH, getValueFactory());
                    path = pathValue.getString();
                    try {
                        return (path.charAt(0) == '/') ? getSession().getNode(path) : getParent().getNode(path);
                    } catch (PathNotFoundException e1) {
                        throw new ItemNotFoundException(pathValue.getString());
                    }
                }

            default:
                throw new ValueFormatException("Property value cannot be converted to a PATH, REFERENCE or WEAKREFERENCE");
        }
    }

    /**
     * @see javax.jcr.Property#getProperty()
     */
    @Override
    public Property getProperty() throws RepositoryException {
        Value value = getValue();
        Value pathValue = ValueHelper.convert(value, PropertyType.PATH, getValueFactory());
        String path = pathValue.getString();
        try {
            return (path.charAt(0) == '/') ? getSession().getProperty(path) : getParent().getProperty(path);
        } catch (PathNotFoundException e) {
            throw new ItemNotFoundException(path);
        }
    }

    /**
     * @see javax.jcr.Property#getLength()
     */
    @Override
    public long getLength() throws RepositoryException {
        return getLength(getValue());
    }

    /**
     * @see javax.jcr.Property#getLengths()
     */
    @Override
    public long[] getLengths() throws RepositoryException {
        Value[] values = getValues();
        long[] lengths = new long[values.length];

        for (int i = 0; i < values.length; i++) {
            lengths[i] = getLength(values[i]);
        }
        return lengths;
    }

    @Override
    public PropertyDefinition getDefinition() throws RepositoryException {
        // TODO
        return null;
    }

    /**
     * @see javax.jcr.Property#getType()
     */
    @Override
    public int getType() throws RepositoryException {
        if (isMultiple()) {
            Value[] values = getValues();
            if (values.length == 0) {
                // retrieve the type from the property definition
                return getRequiredType(PropertyType.UNDEFINED);
            } else {
                return values[0].getType();
            }
        } else {
            return getValue().getType();
        }
    }

    /**
     * @see javax.jcr.Property#isMultiple()
     */
    @Override
    public boolean isMultiple() throws RepositoryException {
        return getPropertyState().isArray();
    }

    //------------------------------------------------------------< private >---

    /**
     *
     * @param defaultType
     * @return the required type for this property.
     */
    private int getRequiredType(int defaultType) throws RepositoryException {
        // check type according to definition of this property
        PropertyDefinition def = getDefinition();
        int reqType = (def == null) ? PropertyType.UNDEFINED : getDefinition().getRequiredType();
        if (reqType == PropertyType.UNDEFINED) {
            if (defaultType == PropertyType.UNDEFINED) {
                reqType = PropertyType.STRING;
            } else {
                reqType = defaultType;
            }
        }
        return reqType;
    }

    /**
     *
     * @param value
     * @param requiredType
     * @throws RepositoryException
     */
    private void setValue(Value value, int requiredType) throws RepositoryException {
        if (requiredType == PropertyType.UNDEFINED) {
            // should never get here since calling methods assert valid type
            throw new IllegalArgumentException("Property type of a value cannot be undefined (" + LogUtil.safeGetJCRPath(this) + ").");
        }

        if (value == null) {
            remove();
        }
        else {
            getParentContentTree().setProperty(name(), ValueConverter.toScalar(value));
        }
    }

    /**
     *
     * @param values
     * @param requiredType
     * @throws RepositoryException
     */
    private void setValues(Value[] values, int requiredType) throws RepositoryException {
        if (requiredType == PropertyType.UNDEFINED) {
            // should never get here since calling methods assert valid type
            throw new IllegalArgumentException("Property type of a value cannot be undefined (" + LogUtil.safeGetJCRPath(this) + ").");
        }

        if (values == null) {
            remove();
        }
        else {
            getParentContentTree().setProperty(name(), ValueConverter.toScalar(values));
        }
    }

    /**
     * Return the length of the specified JCR value object.
     *
     * @param value The value.
     * @return The length of the given value.
     * @throws RepositoryException If an error occurs.
     */
    private static long getLength(Value value) throws RepositoryException {
        if (value.getType() == PropertyType.BINARY) {
            return value.getBinary().getSize();
        }
        else {
            return value.getString().length();
        }
    }

    private Branch getBranch() {
        return sessionContext.getBranch();
    }

    private ContentTree getParentContentTree() {
        resolve();
        return parent;
    }

    private PropertyState getPropertyState() {
        resolve();
        return propertyState;
    }

    private String name() {
        return getPropertyState().getName();
    }

    private String path() {
        return '/' + getParentContentTree().getPath() + '/' + name();
    }

    private synchronized void resolve() {
        parent = getBranch().getContentTree(parent.getPath());
        String path = Paths.concat(parent.getPath(), propertyState.getName());

        if (parent == null) {
            propertyState = null;
        }
        else {
            propertyState = parent.getProperty(Paths.getName(path));
        }
    }

}