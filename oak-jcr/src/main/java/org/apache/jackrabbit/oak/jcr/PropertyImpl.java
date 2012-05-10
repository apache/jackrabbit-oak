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

import org.apache.jackrabbit.oak.api.Tree.Status;
import org.apache.jackrabbit.oak.jcr.value.ValueConverter;
import org.apache.jackrabbit.value.ValueHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.Binary;
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
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

/**
 * {@code PropertyImpl}...
 */
public class PropertyImpl extends ItemImpl implements Property {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(PropertyImpl.class);

    private final PropertyDelegate dlg;
    
    PropertyImpl(PropertyDelegate dlg) {
        super(dlg.getSessionDelegate(), dlg);
        this.dlg = dlg;
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
     * @see javax.jcr.Item#getParent()
     */
    @Override
    public Node getParent() throws RepositoryException {
        return new NodeImpl(dlg.getParent());
    }

    /**
     * @see javax.jcr.Item#isNew()
     */
    @Override
    public boolean isNew() {
        return dlg.getStatus() == Status.NEW;
    }

    /**
     * @see javax.jcr.Item#isModified() ()
     */
    @Override
    public boolean isModified() {
        return dlg.getStatus() == Status.MODIFIED;
    }

    /**
     * @see javax.jcr.Item#isNew()
     */
    @Override
    public void remove() throws RepositoryException {
        checkStatus();
        dlg.remove();
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
                    String msg = "Inhomogeneous type of values (" + this + ')';
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
        if (values == null) {
            setValues(null, reqType);
        } else {
            List<Value> vs = new ArrayList<Value>(values.length);
            for (String value : values) {
                if (value != null) {
                    vs.add(getValueFactory().createValue(value));
                }
            }
            setValues(vs.toArray(new Value[vs.size()]), reqType);
        }
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
        if (value == null) {
            setValue(null, reqType);
        } else {
            setValue(getValueFactory().createValue(value), reqType);
        }
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
            throw new ValueFormatException(this + " is multi-valued.");
        }

        return ValueConverter.toValue(dlg.getValue(), sessionDelegate);
    }

    @Override
    public Value[] getValues() throws RepositoryException {
        checkStatus();
        if (!isMultiple()) {
            throw new ValueFormatException(this + " is not multi-valued.");
        }

        return ValueConverter.toValues(dlg.getValues(), sessionDelegate);
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
        return dlg.getDefinition();
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

    @Override
    public boolean isMultiple() throws RepositoryException {
        checkStatus();
        return dlg.isMultivalue();
    }

    //------------------------------------------------------------< private >---

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

    /**
    *
    * @param defaultType
    * @return the required type for this property.
    * @throws javax.jcr.RepositoryException
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
        assert(requiredType != PropertyType.UNDEFINED);

        // TODO check again if definition validation should be respected here.
        if (isMultiple()) {
            throw new ValueFormatException("Attempt to set a single value to multi-valued property.");
        }
        if (value == null) {
            dlg.remove();
        } else {
            Value targetValue = ValueHelper.convert(value, requiredType, sessionDelegate.getValueFactory());
            dlg.setValue(ValueConverter.toCoreValue(targetValue, sessionDelegate));
        }
    }

    /**
   *
   * @param values
   * @param requiredType
   * @throws RepositoryException
   */
    private void setValues(Value[] values, int requiredType) throws RepositoryException {
        assert(requiredType != PropertyType.UNDEFINED);

        // TODO check again if definition validation should be respected here.
        if (!isMultiple()) {
            throw new ValueFormatException("Attempt to set multiple values to single valued property.");
        }
        if (values == null) {
            dlg.remove();
        } else {
            Value[] targetValues = ValueHelper.convert(values, requiredType, sessionDelegate.getValueFactory());
            dlg.setValues(ValueConverter.toCoreValues(targetValues, sessionDelegate));
        }
    }

}