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
import java.util.Calendar;
import java.util.TimeZone;

import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.StringBasedBlob;
import org.apache.jackrabbit.util.ISO8601;

/**
 * Utility class defining the conversion that take place between {@link org.apache.jackrabbit.oak.api.PropertyState}s
 * of different types. All conversions defined in this class are compatible with the conversions specified
 * in JSR-283 $3.6.4. However, some conversion in this class might not be defined in JSR-283.
 * <p>
 * Example:
 * <pre>
 *    double three = convert("3.0").toDouble();
 * </pre>
 */
public final class Conversions {

    private static final TimeZone UTC = TimeZone.getTimeZone("GMT+00:00");

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
         * Convert to date. This default implementation is based on {@code ISO8601.parse(String)}.
         * @return  date representation of the converted value
         * @throws IllegalArgumentException  if the string cannot be parsed into a date
         */
        public Calendar toCalendar() {
            Calendar date = ISO8601.parse(toString());
            if (date == null) {
                throw new IllegalArgumentException("Not a date string: " + toString());
            }
            return date;
        }

        /**
         * Convert to date. This default implementation delegates to {@link #toCalendar()}
         * and returns the {@code ISO8601.format(Calendar)} value of the calendar.
         * @return  date representation of the converted value
         * @throws IllegalArgumentException  if the string cannot be parsed into a date
         */
        public String toDate() {
            return ISO8601.format(toCalendar());
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
                        return new String(ByteStreams.toByteArray(in), Charsets.UTF_8);
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
     * The conversion to decimal uses {@code new BigDecimal.valueOf(long)}.
     * The conversion to date interprets the value as number of milliseconds since {@code 1970-01-01T00:00:00.000Z}.
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
            public Calendar toCalendar() {
                Calendar date = Calendar.getInstance(UTC);
                date.setTimeInMillis(value);
                return date;
            }

            @Override
            public BigDecimal toDecimal() {
                return BigDecimal.valueOf(value);
            }
        };
    }

    /**
     * Create a converter for a double. {@code String.valueOf(double)} is used for the conversion to {@code String}.
     * The conversions to {@code double} and {@code long} return the {@code value} itself where in the former case
     * the value is casted to {@code long}.
     * The conversion to decimal uses {@code BigDecimal.valueOf(double)}.
     * The conversion to date interprets {@code toLong()} as number of milliseconds since
     * {@code 1970-01-01T00:00:00.000Z}.
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
            public Calendar toCalendar() {
                Calendar date = Calendar.getInstance(TimeZone.getTimeZone("GMT+00:00"));
                date.setTimeInMillis(toLong());
                return date;
            }

            @Override
            public BigDecimal toDecimal() {
                return BigDecimal.valueOf(value);
            }
        };
    }

    /**
     * Create a converter for a date. {@code ISO8601.format(Calendar)} is used for the conversion to {@code String}.
     * The conversions to {@code double}, {@code long} and {@code BigDecimal} return the number of milliseconds
     * since  {@code 1970-01-01T00:00:00.000Z}.
     * @param value  The date to convert
     * @return  A converter for {@code value}
     */
    public static Converter convert(final String value, Type<?> type) {
        if (type == Type.DECIMAL) {
            return convert(convert(value).toDecimal());
        } else if (type == Type.DOUBLE) {
            return convert(convert(value).toDouble());
        } else if (type == Type.LONG) {
            return convert(convert(value).toLong());
        } else if (type != Type.DATE) {
            return convert(value);
        } else {
            return new Converter() {
                @Override
                public String toString() {
                    return value;
                }
                @Override
                public Calendar toCalendar() {
                    return ISO8601.parse(toString());
                }
                @Override
                public long toLong() {
                    return toCalendar().getTimeInMillis();
                }
                @Override
                public double toDouble() {
                    return toLong();
                }
                @Override
                public BigDecimal toDecimal() {
                    return new BigDecimal(toLong());
                }
            };
        }
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
     * The conversion to date interprets {@code toLong()} as number of milliseconds since
     * {@code 1970-01-01T00:00:00.000Z}.
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
            public Calendar toCalendar() {
                Calendar date = Calendar.getInstance(TimeZone.getTimeZone("GMT+00:00"));
                date.setTimeInMillis(toLong());
                return date;
            }

            @Override
            public BigDecimal toDecimal() {
                return value;
            }
        };
    }

}
