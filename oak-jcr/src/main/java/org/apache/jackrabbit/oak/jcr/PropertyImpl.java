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

import static com.google.common.base.Preconditions.checkNotNull;
import static javax.jcr.PropertyType.UNDEFINED;

import java.io.InputStream;
import java.math.BigDecimal;
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

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree.Status;
import org.apache.jackrabbit.oak.jcr.delegate.NodeDelegate;
import org.apache.jackrabbit.oak.jcr.delegate.PropertyDelegate;
import org.apache.jackrabbit.oak.plugins.value.ValueFactoryImpl;
import org.apache.jackrabbit.value.ValueHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO document
 */
public class PropertyImpl extends ItemImpl<PropertyDelegate> implements Property {
    private static final Logger log = LoggerFactory.getLogger(PropertyImpl.class);

    private static final Value[] NO_VALUES = new Value[0];

    PropertyImpl(PropertyDelegate dlg, SessionContext sessionContext) {
        super(dlg, sessionContext);
    }

    //---------------------------------------------------------------< Item >---

    @Override
    public boolean isNode() {
        return false;
    }

    @Override
    @Nonnull
    public Node getParent() throws RepositoryException {
        return perform(new ItemReadOperation<NodeImpl<?>>() {
            @Override
            public NodeImpl<?> perform() throws RepositoryException {
                NodeDelegate parent = dlg.getParent();
                if (parent == null) {
                    throw new AccessDeniedException();
                } else {
                    return new NodeImpl<NodeDelegate>(dlg.getParent(), sessionContext);
                }
            }
        });
    }

    @Override
    public boolean isNew() {
        return safePerform(new ItemReadOperation<Boolean>() {
            @Override
            public Boolean perform() {
                return dlg.getStatus() == Status.NEW;
            }
        });
    }

    @Override
    public boolean isModified() {
        return safePerform(new ItemReadOperation<Boolean>() {
            @Override
            public Boolean perform() {
                return dlg.getStatus() == Status.MODIFIED;
            }
        });
    }

    @Override
    public void remove() throws RepositoryException {
        perform(new ItemWriteOperation<Void>() {
            @Override
            public Void perform() throws RepositoryException {
                dlg.remove();
                return null;
            }
        });
    }

    @Override
    public void accept(ItemVisitor visitor) throws RepositoryException {
        checkStatus();
        visitor.visit(this);
    }

    //-----------------------------------------------------------< Property >---

    @Override
    public void setValue(Value value) throws RepositoryException {
        if (value == null) {
            internalRemove();
        } else {
            internalSetValue(value);
        }
    }

    @Override
    public void setValue(final Value[] values) throws RepositoryException {
        if (values == null) {
            internalRemove();
        } else {
            internalSetValues(values, getType(values));
        }
    }

    @Override
    public void setValue(String value) throws RepositoryException {
        if (value == null) {
            internalRemove();
        } else {
            internalSetValue(getValueFactory().createValue(value));
        }
    }

    @Override
    public void setValue(String[] values) throws RepositoryException {
        if (values == null) {
            internalRemove();
        } else {
            Value[] vs = ValueHelper.convert(values, PropertyType.STRING, getValueFactory());
            internalSetValues(vs, UNDEFINED);
        }
    }

    @Override
    public void setValue(InputStream value) throws RepositoryException {
        if (value == null) {
            internalRemove();
        } else {
            internalSetValue(getValueFactory().createValue(value));
        }
    }

    @Override
    public void setValue(Binary value) throws RepositoryException {
        if (value == null) {
            internalRemove();
        } else {
            internalSetValue(getValueFactory().createValue(value));
        }
    }

    @Override
    public void setValue(long value) throws RepositoryException {
        internalSetValue(getValueFactory().createValue(value));
    }

    @Override
    public void setValue(double value) throws RepositoryException {
        internalSetValue(getValueFactory().createValue(value));
    }

    @Override
    public void setValue(BigDecimal value) throws RepositoryException {
        if (value == null) {
            internalRemove();
        } else {
            internalSetValue(getValueFactory().createValue(value));
        }
    }

    @Override
    public void setValue(Calendar value) throws RepositoryException {
        if (value == null) {
            internalRemove();
        } else {
            internalSetValue(getValueFactory().createValue(value));
        }
    }

    @Override
    public void setValue(boolean value) throws RepositoryException {
        internalSetValue(getValueFactory().createValue(value));
    }

    @Override
    public void setValue(Node value) throws RepositoryException {
        if (value == null) {
            internalRemove();
        } else {
            internalSetValue(getValueFactory().createValue(value));
        }
    }

    @Override
    @Nonnull
    public Value getValue() throws RepositoryException {
        return perform(new ItemReadOperation<Value>() {
            @Override
            public Value perform() throws RepositoryException {
                return ValueFactoryImpl.createValue(dlg.getSingleState(), sessionContext);
            }
        });
    }

    @Override
    @Nonnull
    public Value[] getValues() throws RepositoryException {
        return perform(new ItemReadOperation<List<Value>>() {
            @Override
            public List<Value> perform() throws RepositoryException {
                return ValueFactoryImpl.createValues(dlg.getMultiState(), sessionContext);
            }
        }).toArray(NO_VALUES);
    }

    @Override
    @Nonnull
    public String getString() throws RepositoryException {
        return getValue().getString();
    }

    @SuppressWarnings("deprecation")
    @Override
    @Nonnull
    public InputStream getStream() throws RepositoryException {
        return getValue().getStream();
    }

    @Override
    @Nonnull
    public Binary getBinary() throws RepositoryException {
        return getValue().getBinary();
    }

    @Override
    public long getLong() throws RepositoryException {
        return getValue().getLong();
    }

    @Override
    public double getDouble() throws RepositoryException {
        return getValue().getDouble();
    }

    @Override
    @Nonnull
    public BigDecimal getDecimal() throws RepositoryException {
        return getValue().getDecimal();
    }

    @Override
    @Nonnull
    public Calendar getDate() throws RepositoryException {
        return getValue().getDate();
    }

    @Override
    public boolean getBoolean() throws RepositoryException {
        return getValue().getBoolean();
    }

    @Override
    @Nonnull
    public Node getNode() throws RepositoryException {
        return perform(new ItemReadOperation<Node>() {
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

    @Override
    @Nonnull
    public Property getProperty() throws RepositoryException {
        return perform(new ItemReadOperation<Property>() {
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

    @Override
    public long getLength() throws RepositoryException {
        return getLength(getValue());
    }

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
        return perform(new ItemReadOperation<PropertyDefinition>() {
            @Override
            protected PropertyDefinition perform() throws RepositoryException {
                return getPropertyDefinition();
            }
        });
    }

    @Override
    public int getType() throws RepositoryException {
        return perform(new ItemReadOperation<Integer>() {
            @Override
            public Integer perform() throws RepositoryException {
                if (isMultiple()) {
                    Value[] values = getValues();
                    if (values.length == 0) {
                        // retrieve the type from the property definition
                        PropertyDefinition definition = getPropertyDefinition();
                        if (definition.getRequiredType() == PropertyType.UNDEFINED) {
                            return PropertyType.STRING;
                        } else {
                            return definition.getRequiredType();
                        }
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
        return perform(new ItemReadOperation<Boolean>() {
            @Override
            public Boolean perform() throws RepositoryException {
                return dlg.isArray();
            }
        });
    }

    //------------------------------------------------------------< internal >---

    /**
     * Determine the {@link javax.jcr.PropertyType} of the passed values if all are of
     * the same type.
     *
     * @param values array of values of the same type
     * @return  {@link javax.jcr.PropertyType#UNDEFINED} if {@code values} is empty,
     *          {@code values[0].getType()} otherwise.
     * @throws javax.jcr.ValueFormatException  if not all {@code values} are of the same type
     */
    static int getType(Value[] values) throws ValueFormatException {
        int type = UNDEFINED;
        for (Value value : values) {
            if (value != null) {
                if (type == UNDEFINED) {
                    type = value.getType();
                } else if (value.getType() != type) {
                    throw new ValueFormatException(
                            "All values of a multi-valued property must be of the same type");
                }
            }
        }
        return type;
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
        } else {
            return value.getString().length();
        }
    }

    private void internalRemove() throws RepositoryException {
        perform(new ItemWriteOperation<Void>() {
            @Override
            protected Void perform() throws RepositoryException {
                dlg.remove();
                return null;
            }
        });
    }

    private PropertyDefinition getPropertyDefinition() throws RepositoryException {
        return getDefinitionProvider().getDefinition(
                dlg.getParent().getTree(), dlg.getPropertyState(), true);
    }

    private void internalSetValue(@Nonnull final Value value)
            throws RepositoryException {
        checkNotNull(value);
        perform(new ItemWriteOperation<Void>() {
            @Override
            protected Void perform() throws RepositoryException {
                // TODO: Avoid extra JCR method calls (OAK-672)
                PropertyDefinition definition = getPropertyDefinition();
                PropertyState state = createSingleState(dlg.getName(), value, definition);
                dlg.setState(state);
                return null;
            }
        });
    }

    private void internalSetValues(@Nonnull final Value[] values, final int type) throws RepositoryException {
        checkNotNull(values);
        perform(new ItemWriteOperation<Void>() {
            @Override
            protected Void perform() throws RepositoryException {
                // TODO: Avoid extra JCR method calls (OAK-672)
                PropertyDefinition definition = getPropertyDefinition();
                PropertyState state = createMultiState(dlg.getName(), type, values, definition);
                dlg.setState(state);
                return null;
            }
        });
    }

}