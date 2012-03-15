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

import org.apache.jackrabbit.oak.jcr.SessionImpl.Context;
import org.apache.jackrabbit.oak.jcr.json.JsonValue;
import org.apache.jackrabbit.oak.jcr.state.TransientNodeState;
import org.apache.jackrabbit.oak.jcr.util.LogUtil;
import org.apache.jackrabbit.oak.jcr.util.Path;
import org.apache.jackrabbit.oak.jcr.util.ValueConverter;
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
import javax.jcr.lock.LockException;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.nodetype.PropertyDefinition;
import javax.jcr.version.VersionException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.Calendar;

/**
 * <code>PropertyImpl</code>...
 */
public class PropertyImpl extends ItemImpl implements Property {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(PropertyImpl.class);

    private final TransientNodeState parentState;
    private final String name;
    private final JsonValue value;

    static Property create(Context sessionContext, Path path) throws PathNotFoundException,
            ItemNotFoundException {

        TransientNodeState parentState = getNodeState(sessionContext, path.getParent());
        if (parentState == null) {
            throw new PathNotFoundException(path.toJcrPath());
        }

        String name = path.getName();
        JsonValue value = parentState.getPropertyValue(name);
        return new PropertyImpl(sessionContext, parentState, name, value);
    }

    static Property create(Context sessionContext, TransientNodeState parentState, String name, JsonValue value) {
        return new PropertyImpl(sessionContext, parentState, name, value);
    }

    public static boolean exist(Context sessionContext, Path path) {
        TransientNodeState parentState = getNodeState(sessionContext, path.getParent());
        return parentState != null && parentState.hasProperty(path.getName());
    }

    private PropertyImpl(Context sessionContext, TransientNodeState parentState, String name, JsonValue value) {
        super(sessionContext);
        this.parentState = parentState;
        this.name = name;
        this.value = value;
    }

    //---------------------------------------------------------------< Item >---
    /**
     * @see javax.jcr.Item#isNode()
     */
    @Override
    public boolean isNode() {
        return false;
    }

    @Override
    public String getPath() throws RepositoryException {
        return parentState.getPath().concat(name).toJcrPath();
    }

    @Override
    public String getName() throws RepositoryException {
        return name;
    }

    /**
     * @see javax.jcr.Item#accept(javax.jcr.ItemVisitor)
     */
    @Override
    public void accept(ItemVisitor visitor) throws RepositoryException {
        checkStatus();
        visitor.visit(this);
    }

    @Override
    public Item getAncestor(int depth) throws RepositoryException {
        if (depth == getDepth() - 1) {
            return getParent();
        }
        else {
            return getParent().getAncestor(depth);
        }
    }

    @Override
    public Node getParent() throws RepositoryException {
        return NodeImpl.create(sessionContext, parentState);
    }

    @Override
    public int getDepth() throws RepositoryException {
        return parentState.getPath().getDepth() + 1;
    }

    @Override
    public boolean isNew() {
        return parentState.isPropertyNew(name);
    }

    @Override
    public boolean isModified() {
        return parentState.isPropertyModified(name);
    }

    @Override
    public void remove() throws RepositoryException {
        parentState.removeProperty(name);
    }


    //-----------------------------------------------------------< Property >---
    /**
     * @see Property#setValue(Value)
     */
    @Override
    public void setValue(Value value) throws ValueFormatException, VersionException, LockException, ConstraintViolationException, RepositoryException {
        checkStatus();

        int valueType = (value != null) ? value.getType() : PropertyType.UNDEFINED;
        int reqType = getRequiredType(valueType);
        setValue(value, reqType);
    }

    /**
     * @see Property#setValue(Value[])
     */
    @Override
    public void setValue(Value[] values) throws ValueFormatException, VersionException, LockException, ConstraintViolationException, RepositoryException {
        checkStatus();

        // assert equal types for all values entries
        int valueType = PropertyType.UNDEFINED;
        if (values != null) {
            for (int i = 0; i < values.length; i++) {
                if (values[i] == null) {
                    // skip null values as those will be purged later
                    continue;
                }
                if (valueType == PropertyType.UNDEFINED) {
                    valueType = values[i].getType();
                } else if (valueType != values[i].getType()) {
                    String msg = "Inhomogeneous type of values (" + LogUtil.safeGetJCRPath(this) + ")";
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
    public void setValue(String value) throws ValueFormatException, VersionException, LockException, ConstraintViolationException, RepositoryException {
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
    public void setValue(String[] values) throws ValueFormatException, VersionException, LockException, ConstraintViolationException, RepositoryException {
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
    @Override
    public void setValue(InputStream value) throws ValueFormatException, VersionException, LockException, ConstraintViolationException, RepositoryException {
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
    public void setValue(Binary value) throws ValueFormatException, VersionException, LockException, ConstraintViolationException, RepositoryException {
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
    public void setValue(long value) throws ValueFormatException, VersionException, LockException, ConstraintViolationException, RepositoryException {
        checkStatus();

        int reqType = getRequiredType(PropertyType.LONG);
        setValue(getValueFactory().createValue(value), reqType);
    }

    /**
     * @see Property#setValue(double)
     */
    @Override
    public void setValue(double value) throws ValueFormatException, VersionException, LockException, ConstraintViolationException, RepositoryException {
        checkStatus();

        int reqType = getRequiredType(PropertyType.DOUBLE);
        setValue(getValueFactory().createValue(value), reqType);
    }

    /**
     * @see Property#setValue(BigDecimal)
     */
    @Override
    public void setValue(BigDecimal value) throws ValueFormatException, VersionException, LockException, ConstraintViolationException, RepositoryException {
        checkStatus();

        int reqType = getRequiredType(PropertyType.DECIMAL);
        setValue(getValueFactory().createValue(value), reqType);
    }

    /**
     * @see Property#setValue(Calendar)
     */
    @Override
    public void setValue(Calendar value) throws ValueFormatException, VersionException, LockException, ConstraintViolationException, RepositoryException {
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
    public void setValue(boolean value) throws ValueFormatException, VersionException, LockException, ConstraintViolationException, RepositoryException {
        checkStatus();

        int reqType = getRequiredType(PropertyType.BOOLEAN);
        setValue(getValueFactory().createValue(value), reqType);
    }

    /**
     * @see Property#setValue(javax.jcr.Node)
     */
    @Override
    public void setValue(Node value) throws ValueFormatException, VersionException, LockException, ConstraintViolationException, RepositoryException {
        checkStatus();

        int reqType = getRequiredType(PropertyType.REFERENCE);
        if (value == null) {
            setValue(null, reqType);
        } else {
            setValue(getValueFactory().createValue(value), reqType);
        }
    }

    @Override
    public Value getValue() throws ValueFormatException, RepositoryException {
        checkStatus();
        if (isMultiple()) {
            throw new ValueFormatException(LogUtil.safeGetJCRPath(this) + " is multi-valued.");
        }

        return ValueConverter.toValue(getValueFactory(), value.asAtom());
    }

    @Override
    public Value[] getValues() throws ValueFormatException, RepositoryException {
        checkStatus();
        if (!isMultiple()) {
            throw new ValueFormatException(LogUtil.safeGetJCRPath(this) + " is not multi-valued.");
        }

        return ValueConverter.toValue(getValueFactory(), value.asArray());
    }

    /**
     * @see Property#getString()
     */
    @Override
    public String getString() throws ValueFormatException, RepositoryException {
        return getValue().getString();
    }

    /**
     * @see Property#getStream()
     */
    @Override
    public InputStream getStream() throws ValueFormatException, RepositoryException {
        return getValue().getStream();
    }

    /**
     * @see javax.jcr.Property#getBinary()
     */
    @Override
    public Binary getBinary() throws ValueFormatException, RepositoryException {
        return getValue().getBinary();
    }

    /**
     * @see Property#getLong()
     */
    @Override
    public long getLong() throws ValueFormatException, RepositoryException {
        return getValue().getLong();
    }

    /**
     * @see Property#getDouble()
     */
    @Override
    public double getDouble() throws ValueFormatException, RepositoryException {
        return getValue().getDouble();
    }

    /**
     * @see Property#getDecimal()
     */
    @Override
    public BigDecimal getDecimal() throws ValueFormatException, RepositoryException {
        return getValue().getDecimal();
    }

    /**
     * @see Property#getDate()
     */
    @Override
    public Calendar getDate() throws ValueFormatException, RepositoryException {
        return getValue().getDate();
    }

    /**
     * @see Property#getBoolean()
     */
    @Override
    public boolean getBoolean() throws ValueFormatException, RepositoryException {
        return getValue().getBoolean();
    }

    /**
     * @see javax.jcr.Property#getNode()
     */
    @Override
    public Node getNode() throws ItemNotFoundException, ValueFormatException, RepositoryException {
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
    public Property getProperty() throws ItemNotFoundException, ValueFormatException, RepositoryException {
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
    public long getLength() throws ValueFormatException, RepositoryException {
        return getLength(getValue());
    }

    /**
     * @see javax.jcr.Property#getLengths()
     */
    @Override
    public long[] getLengths() throws ValueFormatException, RepositoryException {
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
        return value.isArray();
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
            parentState.setProperty(name, ValueConverter.toJsonValue(value));
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

        if (value == null) {
            remove();
        }
        else {
            parentState.setProperty(name, ValueConverter.toJsonValue(values));
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

}