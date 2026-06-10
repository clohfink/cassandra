/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.cassandra.schema;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.cql3.statements.PropertyDefinitions;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.utils.ByteBufferUtil;

import static java.lang.String.format;

/**
 * Centralized registry of Netflix-specific table options.
 *
 * <p>Each option is settable through ordinary CQL {@code CREATE TABLE ... WITH} and
 * {@code ALTER TABLE ... WITH} clauses and is surfaced by {@code DESCRIBE} as a first-class table
 * option (e.g. {@code netflix_tier = 2}) rather than as an opaque binary blob. Physically, every
 * option is stored inside the standard {@link org.apache.cassandra.schema.TableParams#extensions}
 * map (key = the {@value #PREFIX}-prefixed option name, value = the UTF-8 encoded canonical value)
 * so no change to the on-disk or system schema layout is required.
 *
 * <p>All known options, their names, validation and rendering live here in {@link Option}; only
 * options declared in that enum are accepted, and each value is validated against its option.
 *
 * <p><b>Mixed-version clusters.</b> An option name is validated against {@link Option} only by the
 * coordinator handling the {@code CREATE}/{@code ALTER} statement, which rejects an unrecognized
 * {@value #PREFIX} name. Once accepted, the value lives in the generic {@code extensions} map and
 * needs no schema-layout change, so it propagates to and is persisted by every node regardless of
 * version. A node whose {@link Option} enum does not include a given option neither validates nor
 * interprets it &mdash; it simply preserves it in {@code extensions} and ignores it. This keeps a
 * mixed-version cluster safe: a newer node may set an option that older nodes store and round-trip
 * but do not act on, rather than failing schema propagation.
 */
public final class NetflixTableOptions
{
    /** Every Netflix custom table option name starts with this prefix. */
    public static final String PREFIX = "netflix_";

    /**
     * The set of recognized Netflix table options. To add a new option, add a constant here with
     * its value validation; that is the only change needed for it to be accepted, validated, stored
     * and rendered by DESCRIBE.
     */
    public enum Option
    {
        /**
         * Marks a table as immutable: no deletes or overwrites are permitted. Boolean valued.
         *
         * <p>Informational only for now &mdash; nothing yet enforces the semantics.
         */
        IMMUTABLE("immutable")
        {
            @Override
            public String canonicalize(String value)
            {
                return Boolean.toString(PropertyDefinitions.parseBoolean(optionName(), value));
            }

            @Override
            public boolean quoted()
            {
                return false;
            }
        },

        /**
         * The table's tier, an integer from {@value #MIN_TIER} to {@value #MAX_TIER} (inclusive).
         *
         * <p>Informational only for now.
         */
        TIER("tier")
        {
            @Override
            public String canonicalize(String value)
            {
                int tier = PropertyDefinitions.toInt(optionName(), value, null);
                if (tier < MIN_TIER || tier > MAX_TIER)
                    throw new SyntaxException(format("Invalid value %s for table option '%s'; must be an integer between %d and %d",
                                                     value, optionName(), MIN_TIER, MAX_TIER));
                return Integer.toString(tier);
            }

            @Override
            public boolean quoted()
            {
                return false;
            }
        };

        private static final int MIN_TIER = 0;
        private static final int MAX_TIER = 4;

        private final String optionName;

        Option(String suffix)
        {
            this.optionName = PREFIX + suffix;
        }

        /** The full CQL option name, including the {@value #PREFIX} prefix (e.g. {@code netflix_tier}). */
        public String optionName()
        {
            return optionName;
        }

        /**
         * Validates {@code value} for this option and returns the canonical string form to store.
         *
         * @throws SyntaxException if the value is not valid for this option
         */
        public abstract String canonicalize(String value);

        /**
         * Whether DESCRIBE renders this option's value single-quoted (a string) or bare (a boolean or
         * number). Defaults to quoted; boolean/numeric options override.
         */
        public boolean quoted()
        {
            return true;
        }
    }

    private static final ImmutableMap<String, Option> BY_NAME;

    static
    {
        ImmutableMap.Builder<String, Option> builder = ImmutableMap.builder();
        for (Option option : Option.values())
            builder.put(option.optionName(), option);
        BY_NAME = builder.build();
    }

    private NetflixTableOptions()
    {
    }

    /** Returns the {@link Option} for the given full option name, or {@code null} if it is not a known Netflix option. */
    public static Option fromName(String name)
    {
        return name == null ? null : BY_NAME.get(name);
    }

    /** Returns {@code true} if {@code name} is a recognized Netflix table option. */
    public static boolean isNetflixOption(String name)
    {
        return fromName(name) != null;
    }

    /**
     * Validates the given Netflix option name/value pair and returns the canonical value to store.
     *
     * @throws SyntaxException if the option name is unknown or the value is invalid for the option
     */
    public static String canonicalize(String name, String value)
    {
        Option option = fromName(name);
        if (option == null)
            throw new SyntaxException(format("Unknown Netflix table option '%s'", name));
        return option.canonicalize(value);
    }

    /** Encodes a Netflix option value for physical storage in the {@code extensions} map. */
    public static ByteBuffer encode(String value)
    {
        return ByteBufferUtil.bytes(value);
    }

    /** Decodes a Netflix option value previously stored via {@link #encode(String)}. */
    public static String decode(ByteBuffer value)
    {
        try
        {
            return ByteBufferUtil.string(value);
        }
        catch (CharacterCodingException e)
        {
            throw new RuntimeException("Invalid (non UTF-8) value stored for Netflix table option", e);
        }
    }
}
