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

import java.io.InputStream;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

import javax.annotation.Nonnull;
import javax.jcr.AccessDeniedException;
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

import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.Tree.Status;
import org.apache.jackrabbit.value.ValueHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * {@code PropertyImpl}...
 */
public class PropertyImpl extends ItemImpl<PropertyDelegate> implements Property {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(PropertyImpl.class);

    PropertyImpl(PropertyDelegate dlg) {
        super(dlg.getSessionDelegate(), dlg);
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
    @Nonnull
    public Node getParent() throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<NodeImpl>() {
            @Override
            public NodeImpl perform() throws RepositoryException {
                NodeDelegate parent = dlg.getParent();
                if (parent == null) {
                    throw new AccessDeniedException();
                } else {
                    return new NodeImpl(dlg.getParent());
                }
            }
        });
    }

    /**
     * @see javax.jcr.Item#isNew()
     */
    @Override
    public boolean isNew() {
        try {
            return sessionDelegate.perform(new SessionOperation<Boolean>() {
                @Override
                public Boolean perform() throws RepositoryException {
                    return dlg.getStatus() == Status.NEW;
                }
            });
        } catch (RepositoryException e) {
            return false;
        }
    }

    /**
     * @see javax.jcr.Item#isModified() ()
     */
    @Override
    public boolean isModified() {
        try {
            return sessionDelegate.perform(new SessionOperation<Boolean>() {
                @Override
                public Boolean perform() throws RepositoryException {
                    return dlg.getStatus() == Status.MODIFIED;
                }
            });
        } catch (RepositoryException e) {
            return false;
        }
    }

    /**
     * @see javax.jcr.Item#isNew()
     */
    @Override
    public void remove() throws RepositoryException {
        checkStatus();
        checkProtected();

        sessionDelegate.perform(new SessionOperation<Void>() {
            @Override
            public Void perform() throws RepositoryException {
                dlg.remove();
                return null;
            }
        });
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
    public void setValue(final Value[] values) throws RepositoryException {
        checkStatus();

        sessionDelegate.perform(new SessionOperation<Void>() {
            @Override
            public Void perform() throws RepositoryException {
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
                        } else if (valueType != value.getType()) {
                            String msg = "Inhomogeneous type of values (" + this + ')';
                            log.debug(msg);
                            throw new ValueFormatException(msg);
                        }
                    }
                }

                int reqType = getRequiredType(valueType);
                setValues(values, reqType);
                return null;
            }
        });
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
    @Nonnull
    public Value getValue() throws RepositoryException {
        checkStatus();

        return sessionDelegate.perform(new SessionOperation<Value>() {
            @Override
            public Value perform() throws RepositoryException {
                if (isMultiple()) {
                    throw new ValueFormatException(this + " is multi-valued.");
                }

                return dlg.getValue();
            }
        });
    }

    @Override
    @Nonnull
    public Value[] getValues() throws RepositoryException {
        checkStatus();

        return sessionDelegate.perform(new SessionOperation<Value[]>() {
            @Override
            public Value[] perform() throws RepositoryException {
                if (!isMultiple()) {
                    throw new ValueFormatException(this + " is not multi-valued.");
                }

                return Iterables.toArray(dlg.getValues(), Value.class);
            }
        });
    }

    /**
     * @see Property#getString()
     */
    @Override
    @Nonnull
    public String getString() throws RepositoryException {
        return getValue().getString();
    }

    /**
     * @see Property#getStream()
     */
    @SuppressWarnings("deprecation")
    @Override
    @Nonnull
    public InputStream getStream() throws RepositoryException {
        return getValue().getStream();
    }

    /**
     * @see javax.jcr.Property#getBinary()
     */
    @Override
    @Nonnull
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
    @Nonnull
    public BigDecimal getDecimal() throws RepositoryException {
        return getValue().getDecimal();
    }

    /**
     * @see Property#getDate()
     */
    @Override
    @Nonnull
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
    @Nonnull
    public Node getNode() throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<Node>() {
            @Override
            public Node perform() throws RepositoryException {
                Value value = getValue();
                switch (value.getType()) {
                    case PropertyType.REFERENCE:
                    case PropertyType.WEAKREFERENCE:
                        return getSession().getNodeByIdentifier(value.getString());

                    case PropertyType.PATH:
                    case PropertyType.NAME:
                        String path = value.getString();
                        if (path.startsWith("[") && path.endsWith("]")) {
                            // identifier path
                            String identifier = path.substring(1, path.length() - 1);
                            return getSession().getNodeByIdentifier(identifier);
                        }
                        else {
                            try {
                                return (path.charAt(0) == '/') ? getSession().getNode(path) : getParent().getNode(path);
                            } catch (PathNotFoundException e) {
                                throw new ItemNotFoundException(path);
                            }
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
        });
    }

    /**
     * @see javax.jcr.Property#getProperty()
     */
    @Override
    @Nonnull
    public Property getProperty() throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<Property>() {
            @Override
            public Property perform() throws RepositoryException {
                Value value = getValue();
                Value pathValue = ValueHelper.convert(value, PropertyType.PATH, getValueFactory());
                String path = pathValue.getString();
                try {
                    return (path.charAt(0) == '/') ? getSession().getProperty(path) : getParent().getProperty(path);
                } catch (PathNotFoundException e) {
                    throw new ItemNotFoundException(path);
                }
            }
        });
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
    @Nonnull
    public long[] getLengths() throws RepositoryException {
        Value[] values = getValues();
        long[] lengths = new long[values.length];

        for (int i = 0; i < values.length; i++) {
            lengths[i] = getLength(values[i]);
        }
        return lengths;
    }

    @Override
    @Nonnull
    public PropertyDefinition getDefinition() throws RepositoryException {
        return dlg.sessionDelegate.getDefinitionProvider().getDefinition(getParent(), this);
    }

    /**
     * @see javax.jcr.Property#getType()
     */
    @Override
    public int getType() throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<Integer>() {
            @Override
            public Integer perform() throws RepositoryException {
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
        });
    }

    @Override
    public boolean isMultiple() throws RepositoryException {
        checkStatus();

        return sessionDelegate.perform(new SessionOperation<Boolean>() {
            @Override
            public Boolean perform() throws RepositoryException {
                return dlg.isMultivalue();
            }
        });
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
        } else {
            return value.getString().length();
        }
    }

    /**
     * @param defaultType
     * @return the required type for this property.
     * @throws javax.jcr.RepositoryException
     */
    private int getRequiredType(int defaultType) throws RepositoryException {
        // check type according to definition of this property
        int reqType = getDefinition().getRequiredType();
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
     * @param value
     * @param requiredType
     * @throws RepositoryException
     */
    private void setValue(Value value, int requiredType) throws RepositoryException {
        checkArgument(requiredType != PropertyType.UNDEFINED);
        checkProtected();

        // TODO check again if definition validation should be respected here.
        if (isMultiple()) {
            throw new ValueFormatException("Attempt to set a single value to multi-valued property.");
        }
        if (value == null) {
            dlg.remove();
        } else {
            Value targetValue = ValueHelper.convert(value, requiredType, sessionDelegate.getValueFactory());
            dlg.setValue(targetValue);
        }
    }

    /**
     * @param values
     * @param requiredType
     * @throws RepositoryException
     */
    private void setValues(Value[] values, int requiredType) throws RepositoryException {
        checkArgument(requiredType != PropertyType.UNDEFINED);
        checkProtected();

        // TODO check again if definition validation should be respected here.
        if (!isMultiple()) {
            throw new ValueFormatException("Attempt to set multiple values to single valued property.");
        }
        if (values == null) {
            dlg.remove();
        } else {
            Value[] targetValues = ValueHelper.convert(values, requiredType, sessionDelegate.getValueFactory());
            Iterable<Value> nonNullValues = Iterables.filter(
                    Arrays.asList(targetValues),
                    Predicates.notNull());

            dlg.setValues(nonNullValues);
        }
    }

}