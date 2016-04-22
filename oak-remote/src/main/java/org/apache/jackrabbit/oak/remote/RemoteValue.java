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

package org.apache.jackrabbit.oak.remote;

import java.io.InputStream;
import java.math.BigDecimal;

/**
 * Represents a value that can be assigned to a property in the repository.
 * Client of the remote repository provides values as instances of this class.
 */
public class RemoteValue {

    /**
     * A generic interface to represent a supplier of an item.
     * <p/>
     * In the specific, it is used by values whose underlying implementation is
     * an {@code InputStream}. To enable multiple traversals of {@code
     * InputStream}s, the value is wrapped by this interface to effectively have
     * a factory over the underlying {@code InputStream}.
     *
     * @param <T> Type of the item this object is able to create.
     */
    public static interface Supplier<T> {

        T get();

    }

    /**
     * This class helps executing logic that depends on the type of a remote
     * value. Instead of manually branching code depending on the result of
     * {@code isText}, {@code isBoolean} and so on, a handler can be implemented
     * to provide different logic for different types.
     */
    public static class TypeHandler {

        public void isBinary(Supplier<InputStream> value) {
            throw new IllegalStateException("case not handled");
        }

        public void isMultiBinary(Iterable<Supplier<InputStream>> value) {
            throw new IllegalStateException("case not handled");
        }

        public void isBinaryId(String value) {
            throw new IllegalStateException("case not handled");
        }

        public void isMultiBinaryId(Iterable<String> value) {
            throw new IllegalStateException("case not handled");
        }

        public void isBoolean(Boolean value) {
            throw new IllegalStateException("case not handled");
        }

        public void isMultiBoolean(Iterable<Boolean> value) {
            throw new IllegalStateException("case not handled");
        }

        public void isDate(Long value) {
            throw new IllegalStateException("case not handled");
        }

        public void isMultiDate(Iterable<Long> value) {
            throw new IllegalStateException("case not handled");
        }

        public void isDecimal(BigDecimal value) {
            throw new IllegalStateException("case not handled");
        }

        public void isMultiDecimal(Iterable<BigDecimal> value) {
            throw new IllegalStateException("case not handled");
        }

        public void isDouble(Double value) {
            throw new IllegalStateException("case not handled");
        }

        public void isMultiDouble(Iterable<Double> value) {
            throw new IllegalStateException("case not handled");
        }

        public void isLong(Long value) {
            throw new IllegalStateException("case not handled");
        }

        public void isMultiLong(Iterable<Long> value) {
            throw new IllegalStateException("case not handled");
        }

        public void isName(String value) {
            throw new IllegalStateException("case not handled");
        }

        public void isMultiName(Iterable<String> value) {
            throw new IllegalStateException("case not handled");
        }

        public void isPath(String value) {
            throw new IllegalStateException("case not handled");
        }

        public void isMultiPath(Iterable<String> value) {
            throw new IllegalStateException("case not handled");
        }

        public void isReference(String value) {
            throw new IllegalStateException("case not handled");
        }

        public void isMultiReference(Iterable<String> value) {
            throw new IllegalStateException("case not handled");
        }

        public void isText(String value) {
            throw new IllegalStateException("case not handled");
        }

        public void isMultiText(Iterable<String> value) {
            throw new IllegalStateException("case not handled");
        }

        public void isUri(String value) {
            throw new IllegalStateException("case not handled");
        }

        public void isMultiUri(Iterable<String> value) {
            throw new IllegalStateException("case not handled");
        }

        public void isWeakReference(String value) {
            throw new IllegalStateException("case not handled");
        }

        public void isMultiWeakReference(Iterable<String> value) {
            throw new IllegalStateException("case not handled");
        }

        public void isUnknown() {
            throw new IllegalStateException("case not handled");
        }
    }

    /**
     * Create a remote value of type string.
     *
     * @param value The string wrapped by the remote value.
     * @return A remote value of type string wrapping the provided value.
     */
    public static RemoteValue toText(final String value) {
        return new RemoteValue() {

            @Override
            public boolean isText() {
                return true;
            }

            @Override
            public String asText() {
                return value;
            }

        };
    }

    /**
     * Create a remote multi-value of type string.
     *
     * @param value The collection of strings wrapped by the remote value.
     * @return A remote multi-value of type string wrapping the provided
     * collection of strings.
     */
    public static RemoteValue toMultiText(final Iterable<String> value) {
        return new RemoteValue() {

            @Override
            public boolean isMultiText() {
                return true;
            }

            @Override
            public Iterable<String> asMultiText() {
                return value;
            }

        };
    }

    /**
     * Create a remote value of type binary.
     *
     * @param value The factory of input streams wrapped by the remote value.
     * @return A remote value of type binary wrapping the provided factory of
     * input streams.
     */
    public static RemoteValue toBinary(final Supplier<InputStream> value) {
        return new RemoteValue() {

            @Override
            public boolean isBinary() {
                return true;
            }

            @Override
            public Supplier<InputStream> asBinary() {
                return value;
            }

        };
    }

    /**
     * Create a remote multi-value of type binary.
     *
     * @param value The collection of factories of input streams wrapped by the
     *              remote value.
     * @return A remote multi-value of type binary wrapping the provided
     * collection of input streams.
     */
    public static RemoteValue toMultiBinary(final Iterable<Supplier<InputStream>> value) {
        return new RemoteValue() {

            @Override
            public boolean isMultiBinary() {
                return true;
            }

            @Override
            public Iterable<Supplier<InputStream>> asMultiBinary() {
                return value;
            }

        };
    }

    /**
     * Create a remote value of type binary ID.
     *
     * @param value The binary ID wrapped by the remote value.
     * @return A remote value wrapping the provided binary ID.
     */
    public static RemoteValue toBinaryId(final String value) {
        return new RemoteValue() {

            @Override
            public boolean isBinaryId() {
                return true;
            }

            @Override
            public String asBinaryId() {
                return value;
            }

        };
    }

    /**
     * Create a remote multi-value of type binary ID.
     *
     * @param value The collection of binary IDs wrapped by the remote value.
     * @return A remote multi-value wrapping the provided collection of binary
     * IDs.
     */
    public static RemoteValue toMultiBinaryId(final Iterable<String> value) {
        return new RemoteValue() {

            @Override
            public boolean isMultiBinaryId() {
                return true;
            }

            @Override
            public Iterable<String> asMultiBinaryId() {
                return value;
            }

        };
    }

    /**
     * Create a remote value of type long.
     *
     * @param value The long to wrap in a remote value.
     * @return A remote value of type long wrapping the provided long value.
     */
    public static RemoteValue toLong(final long value) {
        return new RemoteValue() {

            @Override
            public boolean isLong() {
                return true;
            }


            @Override
            public Long asLong() {
                return value;
            }

        };
    }

    /**
     * Create a remote multi-value of type long.
     *
     * @param value The collection of long values to wrap in a remote value.
     * @return A remote multi-value of type long wrapping the provided
     * collection of long values.
     */
    public static RemoteValue toMultiLong(final Iterable<Long> value) {
        return new RemoteValue() {

            @Override
            public boolean isMultiLong() {
                return true;
            }

            @Override
            public Iterable<Long> asMultiLong() {
                return value;
            }

        };
    }

    /**
     * Create a remote value of type double.
     *
     * @param value The double value to wrap into a remote value.
     * @return A remote value wrapping the provided remote value.
     */
    public static RemoteValue toDouble(final double value) {
        return new RemoteValue() {

            @Override
            public boolean isDouble() {
                return true;
            }

            @Override
            public Double asDouble() {
                return value;
            }

        };
    }

    /**
     * Create a remote multi-value of type double.
     *
     * @param value The collection of double values to wrap into a remote
     *              value.
     * @return A remote multi-value of type double wrapping the provided
     * collection of double values.
     */
    public static RemoteValue toMultiDouble(final Iterable<Double> value) {
        return new RemoteValue() {

            @Override
            public boolean isMultiDouble() {
                return true;
            }

            @Override
            public Iterable<Double> asMultiDouble() {
                return value;
            }

        };
    }

    /**
     * Create a remote value of type date.
     *
     * @param value The date to wrap into a remote value. The date is expressed
     *              in milliseconds since January 1, 1970, 00:00:00 GMT.
     * @return A remote value of type date wrapping the provided date.
     */
    public static RemoteValue toDate(final long value) {
        return new RemoteValue() {

            @Override
            public boolean isDate() {
                return true;
            }

            @Override
            public Long asDate() {
                return value;
            }

        };
    }

    /**
     * Create a remote multi-value of type date.
     *
     * @param value The collection of dates to wrap into a remote value. Every
     *              date is expressed in milliseconds since January 1, 1970,
     *              00:00:00 GMT.
     * @return A remote multi-value of type date wrapping the provided
     * collection of dates.
     */
    public static RemoteValue toMultiDate(final Iterable<Long> value) {
        return new RemoteValue() {

            @Override
            public boolean isMultiDate() {
                return true;
            }

            @Override
            public Iterable<Long> asMultiDate() {
                return value;
            }

        };
    }

    /**
     * Create a remote value of type boolean.
     *
     * @param value The boolean value to wrap into a remote value.
     * @return A remote value wrapping the provided boolean value.
     */
    public static RemoteValue toBoolean(final boolean value) {
        return new RemoteValue() {

            @Override
            public boolean isBoolean() {
                return true;
            }

            @Override
            public boolean asBoolean() {
                return value;
            }

        };
    }

    /**
     * Create a remote multi-value of type boolean.
     *
     * @param value The collection of boolean values to wrap into a remote
     *              value.
     * @return A remote value wrapping the provided collection of boolean
     * values.
     */
    public static RemoteValue toMultiBoolean(final Iterable<Boolean> value) {
        return new RemoteValue() {

            @Override
            public boolean isMultiBoolean() {
                return true;
            }

            @Override
            public Iterable<Boolean> asMultiBoolean() {
                return value;
            }

        };
    }

    /**
     * Create a remote value of type name.
     *
     * @param value The name to wrap into a remote value. A name is represented
     *              as a string.
     * @return A remote value of type name wrapping the provided string.
     */
    public static RemoteValue toName(final String value) {
        return new RemoteValue() {

            @Override
            public boolean isName() {
                return true;
            }

            @Override
            public String asName() {
                return value;
            }

        };
    }

    /**
     * Create a remote multi-value of type name.
     *
     * @param value The collection of names to wrap into a remote value. Every
     *              name is represented by a string.
     * @return A remote multi-value of type name wrapping the provided
     * collection of strings.
     */
    public static RemoteValue toMultiName(final Iterable<String> value) {
        return new RemoteValue() {

            @Override
            public boolean isMultiName() {
                return true;
            }

            @Override
            public Iterable<String> asMultiName() {
                return value;
            }

        };
    }

    /**
     * Create a remote value of type path.
     *
     * @param value The path to wrap into the remote value. A path is
     *              represented by a string.
     * @return A remote value of type path wrapping the provided string.
     */
    public static RemoteValue toPath(final String value) {
        return new RemoteValue() {

            @Override
            public boolean isPath() {
                return true;
            }

            @Override
            public String asPath() {
                return value;
            }

        };
    }

    /**
     * Create a remote multi-value of type path.
     *
     * @param value The collection of paths to wrap into a remote value. Every
     *              path is represented by a string.
     * @return A remote multi-value of type path wrapping the provided strings.
     */
    public static RemoteValue toMultiPath(final Iterable<String> value) {
        return new RemoteValue() {

            @Override
            public boolean isMultiPath() {
                return true;
            }

            @Override
            public Iterable<String> asMultiPath() {
                return value;
            }

        };
    }

    /**
     * Create a remote value of type reference.
     *
     * @param value The reference to wrap in a remote value. The reference is
     *              represented by a string.
     * @return A remote value of type reference wrapping the provided string.
     */
    public static RemoteValue toReference(final String value) {
        return new RemoteValue() {

            @Override
            public boolean isReference() {
                return true;
            }

            @Override
            public String asReference() {
                return value;
            }

        };
    }

    /**
     * Create a remote multi-value of type reference.
     *
     * @param value The collection of references to wrap in a remote value.
     *              Every reference is represented by a string.
     * @return A remote multi-value of type reference wrapping the provided
     * collection of strings.
     */
    public static RemoteValue toMultiReference(final Iterable<String> value) {
        return new RemoteValue() {

            @Override
            public boolean isMultiReference() {
                return true;
            }

            @Override
            public Iterable<String> asMultiReference() {
                return value;
            }

        };
    }

    /**
     * Create a remote value of type weak reference.
     *
     * @param value The weak reference to wrap into a remote value. The weak
     *              reference is represented by a string value.
     * @return A remote value of type weak reference wrapping the provided
     * string value.
     */
    public static RemoteValue toWeakReference(final String value) {
        return new RemoteValue() {

            @Override
            public boolean isWeakReference() {
                return true;
            }

            @Override
            public String asWeakReference() {
                return value;
            }

        };
    }

    /**
     * Create a remote multi-value of type weak reference.
     *
     * @param value The collection of weak references to wrap into a remote
     *              value. Every weak reference is represented by a string.
     * @return A remote multi-value of type weak reference wrapping the provided
     * collection of strings.
     */
    public static RemoteValue toMultiWeakReference(final Iterable<String> value) {
        return new RemoteValue() {

            @Override
            public boolean isMultiWeakReference() {
                return true;
            }

            @Override
            public Iterable<String> asMultiWeakReference() {
                return value;
            }

        };
    }

    /**
     * Create a remote value of type URI.
     *
     * @param value The string representation of the URI to wrap into a remote
     *              value.
     * @return A remote value of type URI wrapping the provided string.
     */
    public static RemoteValue toUri(final String value) {
        return new RemoteValue() {

            @Override
            public boolean isUri() {
                return true;
            }

            @Override
            public String asUri() {
                return value;
            }

        };
    }

    /**
     * Create a remote multi-value of type URI.
     *
     * @param value The collection of URIs to wrap into the remote value. Every
     *              URI is represented by a string.
     * @return A remote multi-value of type URI wrapping the provided collection
     * of strings.
     */
    public static RemoteValue toMultiUri(final Iterable<String> value) {
        return new RemoteValue() {

            @Override
            public boolean isMultiUri() {
                return true;
            }

            @Override
            public Iterable<String> asMultiUri() {
                return value;
            }

        };
    }

    /**
     * Create a remote value of type decimal.
     *
     * @param value The decimal to wrap into a remote value.
     * @return A remote value of type decimal wrapping the provided decimal
     * value.
     */
    public static RemoteValue toDecimal(final BigDecimal value) {
        return new RemoteValue() {

            @Override
            public boolean isDecimal() {
                return true;
            }

            @Override
            public BigDecimal asDecimal() {
                return value;
            }

        };
    }

    /**
     * Create a remote multi-value of type decimal.
     *
     * @param value The collection of decimals to wrap into a remote value.
     * @return A remote multi-value of type decimal wrapping the provided
     * collection of decimals.
     */
    public static RemoteValue toMultiDecimal(final Iterable<BigDecimal> value) {
        return new RemoteValue() {

            @Override
            public boolean isMultiDecimal() {
                return true;
            }

            @Override
            public Iterable<BigDecimal> asMultiDecimal() {
                return value;
            }

        };
    }

    private RemoteValue() {

    }

    /**
     * Check if this remote value is of type string.
     *
     * @return {@code true} if this remote value is of type string, {@code
     * false} otherwise.
     */
    public boolean isText() {
        return false;
    }

    /**
     * Read the value of this remote string value.
     *
     * @return The string wrapped by this remote value if this remote value is
     * of type string, {@code null} otherwise.
     */
    public String asText() {
        return null;
    }

    /**
     * Check if this remote value is a multi-value of type string.
     *
     * @return {@code true} if this remote value is a multi-value of type
     * string, {@code false} otherwise.
     */
    public boolean isMultiText() {
        return false;
    }

    /**
     * Read the value of this remote string multi-value.
     *
     * @return The collection of strings wrapped by this remote value if this
     * remote value is a multi-value of type string, {@code null} otherwise.
     */
    public Iterable<String> asMultiText() {
        return null;
    }

    /**
     * Check if this remote value is of type binary.
     *
     * @return {@code true} if this remote value is of type binary, {@code
     * false} otherwise.
     */
    public boolean isBinary() {
        return false;
    }

    /**
     * Read the value of this remote boolean value.
     *
     * @return The value of this remote value if this remote value is of type
     * binary, {@code null} otherwise.
     */
    public Supplier<InputStream> asBinary() {
        return null;
    }

    /**
     * Check if this remote value is a multi-value of type binary.
     *
     * @return {@code true} if this remote value is a multi-value of type
     * binary, {@code false} otherwise.
     */
    public boolean isMultiBinary() {
        return false;
    }

    /**
     * Read the value of this remote binary multi-value.
     *
     * @return The value of this remote value if this remote value is a
     * multi-value of type binary, {@code null} otherwise.
     */
    public Iterable<Supplier<InputStream>> asMultiBinary() {
        return null;
    }

    /**
     * Check if this remote value is of type binary ID.
     *
     * @return {@code true} if this remote value is of type binary ID, {@code
     * false} otherwise.
     */
    public boolean isBinaryId() {
        return false;
    }

    /**
     * Read the value of this remote binary ID multi-value.
     *
     * @return The value of this remote value if this remote value is of type
     * binary ID, {@code null} otherwise.
     */
    public String asBinaryId() {
        return null;
    }

    /**
     * Check if this remote value is a multi-value of type binary ID.
     *
     * @return {@code true} if this remote value is a multi-value of type binary
     * ID, {@code false} otherwise.
     */
    public boolean isMultiBinaryId() {
        return false;
    }

    /**
     * Return the value of this remote binary ID multi-value.
     *
     * @return The value of this remote value if this remote value is a
     * multi-value of type binary ID, {@code null} otherwise.
     */
    public Iterable<String> asMultiBinaryId() {
        return null;
    }

    /**
     * Check if this remote value is of type long.
     *
     * @return {@code true} if this remote value is of type long, {@code false}
     * otherwise.
     */
    public boolean isLong() {
        return false;
    }

    /**
     * Read the value of this remote long multi-value.
     *
     * @return The value of this remote value if this remote value is of type
     * long, {@code null} otherwise.
     */
    public Long asLong() {
        return null;
    }

    /**
     * Check if this remote value is a multi-value of type long.
     *
     * @return {@code true} if this value is a multi-value of type long, {@code
     * false} otherwise.
     */
    public boolean isMultiLong() {
        return false;
    }

    /**
     * Read the value of this remote multi-value of type long.
     *
     * @return The value of this remote value if this remote value is a
     * multi-value of type long, {@code null} otherwise.
     */
    public Iterable<Long> asMultiLong() {
        return null;
    }

    /**
     * Check if this remote value is of type double.
     *
     * @return {@code true} if this remote value is of type long, {@code false}
     * otherwise.
     */
    public boolean isDouble() {
        return false;
    }

    /**
     * Read the value of this remote value of type double.
     *
     * @return The value of this remote value if this remote value is of type
     * double, {@code null} otherwise.
     */
    public Double asDouble() {
        return null;
    }

    /**
     * Check if this remote value is a multi-value of type double.
     *
     * @return {@code true} if this remote value is a multi-value of type
     * double, {@code false} otherwise.
     */
    public boolean isMultiDouble() {
        return false;
    }

    /**
     * Read the value of this remote multi-value of type double.
     *
     * @return The value of this remote value if this remote value is a
     * multi-value of type double, {@code null} otherwise.
     */
    public Iterable<Double> asMultiDouble() {
        return null;
    }

    /**
     * Check if this remote value is of type date.
     *
     * @return {@code true} if this remote value is of type date, {@code false}
     * otherwise.
     */
    public boolean isDate() {
        return false;
    }

    /**
     * Read the value of this remote multi-value of type date.
     *
     * @return The value of this remote value if this remote value is of type
     * date, {@code null} otherwise.
     */
    public Long asDate() {
        return null;
    }

    /**
     * Check if this remote value is a multi-value of type date.
     *
     * @return {@code true} if this remote value is a multi-value of type date,
     * {@code false} otherwise.
     */
    public boolean isMultiDate() {
        return false;
    }

    /**
     * Read the value of this remote multi-value of type date.
     *
     * @return The value of this remote value if this remote value is a
     * multi-value of type date, {@code null} otherwise.
     */
    public Iterable<Long> asMultiDate() {
        return null;
    }

    /**
     * Check if this remote value is of type boolean.
     *
     * @return {@code true} if this remote value is fo type boolean, {@code
     * false} otherwise.
     */
    public boolean isBoolean() {
        return false;
    }

    /**
     * Read the value of this remote value of type boolean.
     *
     * @return The value of this remote value if this remote value is of type
     * boolean, false otherwise.
     */
    public boolean asBoolean() {
        return false;
    }

    /**
     * Check if this remote value is a multi-value of type boolean.
     *
     * @return {@code true} if this remote value is a multi-value of type
     * boolean, {@code false} otherwise.
     */
    public boolean isMultiBoolean() {
        return false;
    }

    /**
     * Read the value of this remote multi-value of type boolean.
     *
     * @return The value of this remote value if this remote value is a
     * multi-value of type boolean, {@code null} otherwise.
     */
    public Iterable<Boolean> asMultiBoolean() {
        return null;
    }

    /**
     * Check if this remote value is of type name.
     *
     * @return {@code true} if this remote value is of type name, {@code false}
     * otherwise.
     */
    public boolean isName() {
        return false;
    }

    /**
     * Read the value of this remote value of type name.
     *
     * @return The value of this remote value if this remote value is of type
     * name, {@code null} otherwise.
     */
    public String asName() {
        return null;
    }

    /**
     * Check if this remote value is a multi-value of type name.
     *
     * @return {@code true} if this remote value is a multi-value of type name,
     * {@code false} otherwise.
     */
    public boolean isMultiName() {
        return false;
    }

    /**
     * Read the value of this remote multi-value of type name.
     *
     * @return The value of this remote value if this remote value is a
     * multi-value of type name, {@code null} otherwise.
     */
    public Iterable<String> asMultiName() {
        return null;
    }

    /**
     * Check if this value is of type path.
     *
     * @return {@code true} if this remote value is of type path, {@code false}
     * otherwise.
     */
    public boolean isPath() {
        return false;
    }

    /**
     * Read the value of this remote value of type path.
     *
     * @return The value of this remote value if this remote value is of type
     * path, {@code null} otherwise.
     */
    public String asPath() {
        return null;
    }

    /**
     * Check if this remote value is a multi-value of type path.
     *
     * @return {@code true} if this remote value is a multi-value of type path,
     * {@code false} otherwise.
     */
    public boolean isMultiPath() {
        return false;
    }

    /**
     * Read the value of this remote multi-value of type path.
     *
     * @return The value of this remote value if this remote value is a
     * multi-value of type path, {@code null} otherwise.
     */
    public Iterable<String> asMultiPath() {
        return null;
    }

    /**
     * Check if this remote value is of type reference.
     *
     * @return {@code true} if this remote value is of type reference, {@code
     * false} otherwise.
     */
    public boolean isReference() {
        return false;
    }

    /**
     * Read the value of this remote value of type reference.
     *
     * @return The value of this remote value if this remote value is of type
     * reference, {@code null} otherwise.
     */
    public String asReference() {
        return null;
    }

    /**
     * Check if this remote value is a multi-value of type reference.
     *
     * @return {@code true} if this remote value is a multi-value of type
     * reference, {@code false} otherwise.
     */
    public boolean isMultiReference() {
        return false;
    }

    /**
     * Read the value of this remote multi-value of type reference.
     *
     * @return The value of this remote value if this remote value is a
     * multi-value of type reference, {@code null} otherwise.
     */
    public Iterable<String> asMultiReference() {
        return null;
    }

    /**
     * Check if this remote value is of type weak reference.
     *
     * @return {@code true} if this remote value is fo type weak reference,
     * {@code false} otherwise.
     */
    public boolean isWeakReference() {
        return false;
    }

    /**
     * Read the value of this remote value of type weak reference.
     *
     * @return The value of this remote value if this remote value is of type
     * weak reference, {@code null} otherwise.
     */
    public String asWeakReference() {
        return null;
    }

    /**
     * Check if this remote value is a multi-value of type weak reference.
     *
     * @return {@code true} if this remote value is a multi-value of type weak
     * reference, {@code false} otherwise.
     */
    public boolean isMultiWeakReference() {
        return false;
    }

    /**
     * Read the value of this remote multi-value of type weak reference.
     *
     * @return The value of this remote value if this remote value is a
     * multi-value of type weak reference, {@code null} otherwise.
     */
    public Iterable<String> asMultiWeakReference() {
        return null;
    }

    /**
     * Check if this remote value is of type decimal.
     *
     * @return {@code true} if this remote value is of type decimal, {@code
     * false} otherwise.
     */
    public boolean isDecimal() {
        return false;
    }

    /**
     * Read the value of this remote value of type decimal.
     *
     * @return The value of this remote value if this remote value is of type
     * decimal, {@code null} otherwise.
     */
    public BigDecimal asDecimal() {
        return null;
    }

    /**
     * Check if this remote value is a multi-value of type decimal.
     *
     * @return {@code true} if this remote value is a multi-value of type
     * decimal, {@code false} otherwise.
     */
    public boolean isMultiDecimal() {
        return false;
    }

    /**
     * Read the value of this remote multi-value of type decimal.
     *
     * @return The value of this remote value if this remote value is a
     * multi-value of type decimal, {@code null} otherwise.
     */
    public Iterable<BigDecimal> asMultiDecimal() {
        return null;
    }

    /**
     * Check if this remote value is of type URI.
     *
     * @return {@code true} if this remote value is of type URI, {@code false}
     * otherwise.
     */
    public boolean isUri() {
        return false;
    }

    /**
     * Read the value of this remote value of type URI.
     *
     * @return The value of this remote value if this remote value is of type
     * URI, {@code null} otherwise.
     */
    public String asUri() {
        return null;
    }

    /**
     * Check if this remote value is a multi-value of type URI.
     *
     * @return {@code true} if this remote value is a multi-value of type URI,
     * {@code false} otherwise.
     */
    public boolean isMultiUri() {
        return false;
    }

    /**
     * Read the value of this remote multi-value of type URI.
     *
     * @return The value of this remote value if this remote value is a
     * multi-value of type URI, {@code null} otherwise.
     */
    public Iterable<String> asMultiUri() {
        return null;
    }

    /**
     * Calls a method of the provided handler according to the type of this
     * remote value.
     *
     * @param handler Handler containing logic to be executing according to the
     *                type of this remote value.
     */
    public void whenType(TypeHandler handler) {
        if (isBinary()) {
            handler.isBinary(asBinary());
            return;
        }

        if (isMultiBinary()) {
            handler.isMultiBinary(asMultiBinary());
            return;
        }

        if (isBinaryId()) {
            handler.isBinaryId(asBinaryId());
            return;
        }

        if (isMultiBinaryId()) {
            handler.isMultiBinaryId(asMultiBinaryId());
            return;
        }

        if (isBoolean()) {
            handler.isBoolean(asBoolean());
            return;
        }

        if (isMultiBoolean()) {
            handler.isMultiBoolean(asMultiBoolean());
            return;
        }

        if (isDate()) {
            handler.isDate(asDate());
            return;
        }

        if (isMultiDate()) {
            handler.isMultiDate(asMultiDate());
            return;
        }

        if (isDecimal()) {
            handler.isDecimal(asDecimal());
            return;
        }

        if (isMultiDecimal()) {
            handler.isMultiDecimal(asMultiDecimal());
            return;
        }

        if (isDouble()) {
            handler.isDouble(asDouble());
            return;
        }

        if (isMultiDouble()) {
            handler.isMultiDouble(asMultiDouble());
            return;
        }

        if (isLong()) {
            handler.isLong(asLong());
            return;
        }

        if (isMultiLong()) {
            handler.isMultiLong(asMultiLong());
            return;
        }

        if (isName()) {
            handler.isName(asName());
            return;
        }

        if (isMultiName()) {
            handler.isMultiName(asMultiName());
            return;
        }

        if (isPath()) {
            handler.isPath(asPath());
            return;
        }

        if (isMultiPath()) {
            handler.isMultiPath(asMultiPath());
            return;
        }

        if (isReference()) {
            handler.isReference(asReference());
            return;
        }

        if (isMultiReference()) {
            handler.isMultiReference(asMultiReference());
            return;
        }

        if (isText()) {
            handler.isText(asText());
            return;
        }

        if (isMultiText()) {
            handler.isMultiText(asMultiText());
            return;
        }

        if (isUri()) {
            handler.isUri(asUri());
            return;
        }

        if (isMultiUri()) {
            handler.isMultiUri(asMultiUri());
            return;
        }

        if (isWeakReference()) {
            handler.isWeakReference(asWeakReference());
            return;
        }

        if (isMultiWeakReference()) {
            handler.isMultiWeakReference(asMultiWeakReference());
            return;
        }

        handler.isUnknown();
    }

}
