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
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.netflix.cassandra.schema.NetflixTableOptions.Option;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit tests for the centralized {@link NetflixTableOptions} registry: option lookup, per-option
 * value validation, canonicalization, and encode/decode. These do not require a running server.
 */
public class NetflixTableOptionsValidationTest
{
    @Test
    public void testEveryOptionNameIsPrefixed()
    {
        for (Option option : Option.values())
            assertTrue(option.optionName(), option.optionName().startsWith(NetflixTableOptions.PREFIX));
    }

    @Test
    public void testLookupByName()
    {
        assertSame(Option.IMMUTABLE, NetflixTableOptions.fromName("netflix_immutable"));
        assertSame(Option.TIER, NetflixTableOptions.fromName("netflix_tier"));
        assertSame(Option.RELAXED_TRUNCATE, NetflixTableOptions.fromName("netflix_relaxed_truncate"));

        assertNull(NetflixTableOptions.fromName("netflix_does_not_exist"));
        assertNull(NetflixTableOptions.fromName("comment"));
        assertNull(NetflixTableOptions.fromName(null));

        assertTrue(NetflixTableOptions.isNetflixOption("netflix_tier"));
        assertTrue(NetflixTableOptions.isNetflixOption("netflix_relaxed_truncate"));
        assertFalse(NetflixTableOptions.isNetflixOption("netflix_made_up"));
        assertFalse(NetflixTableOptions.isNetflixOption("gc_grace_seconds"));
        assertFalse(NetflixTableOptions.isNetflixOption(null));
    }

    @Test
    public void testUnknownOptionRejectedByCanonicalize()
    {
        assertThrows("Unknown Netflix table option", () -> NetflixTableOptions.canonicalize("netflix_nope", "x"));
    }

    @Test
    public void testImmutableCanonicalization()
    {
        // Accepted spellings normalize to canonical 'true'/'false'.
        for (String t : new String[]{ "true", "yes", "1", "TRUE", "Yes" })
            assertEquals("true", Option.IMMUTABLE.canonicalize(t));
        for (String f : new String[]{ "false", "no", "0", "FALSE", "No" })
            assertEquals("false", Option.IMMUTABLE.canonicalize(f));

        assertFalse(Option.IMMUTABLE.quoted());
        assertThrows("Invalid boolean value", () -> Option.IMMUTABLE.canonicalize("maybe"));
    }

    @Test
    public void testTierCanonicalization()
    {
        for (int i = 0; i <= 4; i++)
            assertEquals(Integer.toString(i), Option.TIER.canonicalize(Integer.toString(i)));

        assertFalse(Option.TIER.quoted());

        // Out of range.
        assertThrows("must be an integer between 0 and 4", () -> Option.TIER.canonicalize("5"));
        assertThrows("must be an integer between 0 and 4", () -> Option.TIER.canonicalize("-1"));
        // Not an integer.
        assertThrows("Invalid integer value", () -> Option.TIER.canonicalize("gold"));
    }

    @Test
    public void testRelaxedTruncateCanonicalization()
    {
        // Accepted boolean spellings normalize to canonical 'true'/'false'.
        for (String t : new String[]{ "true", "yes", "1", "TRUE", "Yes" })
            assertEquals("true", Option.RELAXED_TRUNCATE.canonicalize(t));
        for (String f : new String[]{ "false", "no", "0", "FALSE", "No" })
            assertEquals("false", Option.RELAXED_TRUNCATE.canonicalize(f));

        assertFalse(Option.RELAXED_TRUNCATE.quoted());
        assertThrows("Invalid boolean value", () -> Option.RELAXED_TRUNCATE.canonicalize("maybe"));
    }

    @Test
    public void testIsRelaxedTruncateHelper()
    {
        Map<String, ByteBuffer> extensions = new HashMap<>();

        // Absent option means strict (false).
        assertFalse(NetflixTableOptions.isRelaxedTruncate(extensions));
        assertNull(NetflixTableOptions.get(extensions, Option.RELAXED_TRUNCATE));

        extensions.put("netflix_relaxed_truncate", NetflixTableOptions.encode("true"));
        assertTrue(NetflixTableOptions.isRelaxedTruncate(extensions));
        assertEquals("true", NetflixTableOptions.get(extensions, Option.RELAXED_TRUNCATE));

        extensions.put("netflix_relaxed_truncate", NetflixTableOptions.encode("false"));
        assertFalse(NetflixTableOptions.isRelaxedTruncate(extensions));
    }

    @Test
    public void testEncodeDecodeRoundTrip()
    {
        assertEquals(ByteBufferUtil.bytes("true"), NetflixTableOptions.encode("true"));
        assertEquals("3", NetflixTableOptions.decode(NetflixTableOptions.encode("3")));
    }

    private static void assertThrows(String expectedMessageFragment, Runnable runnable)
    {
        try
        {
            runnable.run();
            fail("Expected a SyntaxException containing: " + expectedMessageFragment);
        }
        catch (SyntaxException e)
        {
            assertTrue("Unexpected message: " + e.getMessage(),
                       e.getMessage().toLowerCase().contains(expectedMessageFragment.toLowerCase()));
        }
    }
}
