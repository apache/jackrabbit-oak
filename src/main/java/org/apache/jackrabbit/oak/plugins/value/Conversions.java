/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.plugins.value;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;

import com.google.common.io.ByteStreams;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.plugins.memory.StringBasedBlob;

/**
 * Utility class defining the conversion that take place between {@link org.apache.jackrabbit.oak.api.PropertyState}s
 * of different types.
 * <p>
 * Example:
 * <pre>
 *    double three = convert("3.0").toDouble();
 * </pre>
 */
public final class Conversions {
    private Conversions() {}

    /**
     * A converter converts a value to its representation as a specific target type. Not all target
     * types might be supported for a given value in which case implementations throw an exception.
     * The default implementations of the various conversion methods all operate on the string
     * representation of the underlying value (i.e. call {@code Converter.toString()}.
     */
    public abstract static class Converter {

        /**
         * Convert to string
         * @return  string representation of the converted value
         */
        public abstract String toString();

        /**
         * Convert to binary. This default implementation returns an new instance
         * of {@link StringBasedBlob}.
         * @return  binary representation of the converted value
         */
        public Blob toBinary() {
            return new StringBasedBlob(toString());
        }

        /**
         * Convert to long. This default implementation is based on {@code Long.parseLong(String)}.
         * @return  long representation of the converted value
         * @throws NumberFormatException
         */
        public long toLong() {
            return Long.parseLong(toString());
        }

        /**
         * Convert to double. This default implementation is based on {@code Double.parseDouble(String)}.
         * @return  double representation of the converted value
         * @throws NumberFormatException
         */
        public double toDouble() {
            return Double.parseDouble(toString());
        }

        /**
         * Convert to boolean. This default implementation is based on {@code Boolean.parseBoolean(String)}.
         * @return  boolean representation of the converted value
         */
        public boolean toBoolean() {
            return Boolean.parseBoolean(toString());
        }

        /**
         * Convert to decimal. This default implementation is based on {@code new BigDecimal(String)}.
         * @return  decimal representation of the converted value
         * @throws NumberFormatException
         */
        public BigDecimal toDecimal() {
            return new BigDecimal(toString());
        }
    }

    /**
     * Create a converter for a string.
     * @param value  The string to convert
     * @return  A converter for {@code value}
     * @throws NumberFormatException
     */
    public static Converter convert(final String value) {
        return new Converter() {
            @Override
            public String toString() {
                return value;
            }
        };
    }

    /**
     * Create a converter for a binary.
     * For the conversion to {@code String} the binary in interpreted as UTF-8 encoded string.
     * @param value  The binary to convert
     * @return  A converter for {@code value}
     * @throws IllegalArgumentException  if the binary is inaccessible
     */
    public static Converter convert(final Blob value) {
        return new Converter() {
            @Override
            public String toString() {
                try {
                    InputStream in = value.getNewStream();
                    try {
                        return new String(ByteStreams.toByteArray(in), "UTF-8");
                    }
                    finally {
                        in.close();
                    }
                }
                catch (IOException e) {
                    throw new IllegalArgumentException(e);
                }
            }

            @Override
            public Blob toBinary() {
                return value;
            }
        };
    }

    /**
     * Create a converter for a long. {@code String.valueOf(long)} is used for the conversion to {@code String}.
     * The conversions to {@code double} and {@code long} return the {@code value} itself.
     * The conversion to decimal uses {@code new BigDecimal(long)}.
     * @param value  The long to convert
     * @return  A converter for {@code value}
     */
    public static Converter convert(final long value) {
        return new Converter() {
            @Override
            public String toString() {
                return String.valueOf(value);
            }

            @Override
            public long toLong() {
                return value;
            }

            @Override
            public double toDouble() {
                return value;
            }

            @Override
            public BigDecimal toDecimal() {
                return new BigDecimal(value);
            }
        };
    }

    /**
     * Create a converter for a double. {@code String.valueOf(double)} is used for the conversion to {@code String}.
     * The conversions to {@code double} and {@code long} return the {@code value} itself where in the former case
     * the value is casted to {@code long}.
     * The conversion to decimal uses {@code new BigDecimal(double)}.
     * @param value  The double to convert
     * @return  A converter for {@code value}
     */
    public static Converter convert(final double value) {
        return new Converter() {
            @Override
            public String toString() {
                return String.valueOf(value);
            }

            @Override
            public long toLong() {
                return (long) value;
            }

            @Override
            public double toDouble() {
                return value;
            }

            @Override
            public BigDecimal toDecimal() {
                return new BigDecimal(value);
            }
        };
    }

    /**
     * Create a converter for a boolean. {@code Boolean.toString(boolean)} is used for the conversion to {@code String}.
     * @param value  The boolean to convert
     * @return  A converter for {@code value}
     */
    public static Converter convert(final boolean value) {
        return new Converter() {
            @Override
            public String toString() {
                return Boolean.toString(value);
            }

            @Override
            public boolean toBoolean() {
                return value;
            }
        };
    }

    /**
     * Create a converter for a decimal. {@code BigDecimal.toString()} is used for the conversion to {@code String}.
     * {@code BigDecimal.longValue()} and {@code BigDecimal.doubleValue()} is used for the conversions to
     * {@code long} and {@code double}, respectively.
     * @param value  The decimal to convert
     * @return  A converter for {@code value}
     */
    public static Converter convert(final BigDecimal value) {
        return new Converter() {
            @Override
            public String toString() {
                return value.toString();
            }

            @Override
            public long toLong() {
                return value.longValue();
            }

            @Override
            public double toDouble() {
                return value.doubleValue();
            }

            @Override
            public BigDecimal toDecimal() {
                return value;
            }
        };
    }

}
