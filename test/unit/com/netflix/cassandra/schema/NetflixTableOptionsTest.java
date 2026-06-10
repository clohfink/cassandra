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

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableParams;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * End-to-end tests for Netflix custom table options through CQL CREATE/ALTER and DESCRIBE: they are
 * validated against {@link NetflixTableOptions.Option}, physically stored within the
 * {@code extensions} map, and surfaced by DESCRIBE as first-class table options rather than as
 * opaque binary extensions.
 */
public class NetflixTableOptionsTest extends CQLTester
{
    @Test
    public void testCreateStoresNetflixOptionsInExtensions()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int) " +
                    "WITH netflix_immutable = true AND netflix_tier = 2");

        TableMetadata metadata = currentTableMetadata();

        // Physically stored inside the extensions map, keyed by the option name, value UTF-8 encoded.
        assertEquals(ByteBufferUtil.bytes("true"), metadata.params.extensions.get("netflix_immutable"));
        assertEquals(ByteBufferUtil.bytes("2"), metadata.params.extensions.get("netflix_tier"));
    }

    @Test
    public void testDescribeRendersNetflixOptionsAsFirstClassOptions()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int) " +
                    "WITH netflix_immutable = true AND netflix_tier = 2");

        String cql = currentTableMetadata().toCqlString(false, false);

        // Surfaced as first-class options, rendered bare like other boolean/numeric options...
        assertTrue(cql, cql.contains("AND netflix_immutable = true"));
        assertTrue(cql, cql.contains("AND netflix_tier = 2"));

        // ...and pulled out of the opaque binary extensions blob (which is otherwise empty here).
        assertTrue(cql, cql.contains("AND extensions = {}"));
        assertFalse(cql, cql.contains("0x"));
    }

    @Test
    public void testAlterAddsAndUpdatesNetflixOptionsPreservingOthers()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int) WITH netflix_tier = 1");

        // Add a new option and overwrite the existing one in the same ALTER.
        alterTable("ALTER TABLE %s WITH netflix_tier = 3 AND netflix_immutable = true");

        TableMetadata metadata = currentTableMetadata();
        assertEquals(ByteBufferUtil.bytes("3"), metadata.params.extensions.get("netflix_tier"));
        assertEquals(ByteBufferUtil.bytes("true"), metadata.params.extensions.get("netflix_immutable"));

        // Altering an unrelated standard option must not drop existing Netflix options.
        alterTable("ALTER TABLE %s WITH comment = 'hi'");
        metadata = currentTableMetadata();
        assertEquals(ByteBufferUtil.bytes("3"), metadata.params.extensions.get("netflix_tier"));
        assertEquals(ByteBufferUtil.bytes("true"), metadata.params.extensions.get("netflix_immutable"));
    }

    @Test
    public void testAlterUnsetRemovesNetflixOption()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int) " +
                    "WITH netflix_immutable = true AND netflix_tier = 2");

        // An empty string is the sentinel for "unset": the option is removed from extensions entirely.
        alterTable("ALTER TABLE %s WITH netflix_tier = ''");

        TableMetadata metadata = currentTableMetadata();
        assertFalse(metadata.params.extensions.containsKey("netflix_tier"));
        // Unsetting one option leaves the others untouched.
        assertEquals(ByteBufferUtil.bytes("true"), metadata.params.extensions.get("netflix_immutable"));
    }

    @Test
    public void testDescribeOmitsUnsetNetflixOption()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int) " +
                    "WITH netflix_immutable = true AND netflix_tier = 2");

        alterTable("ALTER TABLE %s WITH netflix_tier = ''");

        // The unset option disappears from DESCRIBE; surviving options are still rendered.
        String cql = currentTableMetadata().toCqlString(false, false);
        assertFalse(cql, cql.contains("netflix_tier"));
        assertTrue(cql, cql.contains("AND netflix_immutable = true"));
    }

    @Test
    public void testUnsetAndSetInSameAlter()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int) WITH netflix_tier = 2");

        // Remove one option and add another in a single ALTER.
        alterTable("ALTER TABLE %s WITH netflix_tier = '' AND netflix_immutable = true");

        TableMetadata metadata = currentTableMetadata();
        assertFalse(metadata.params.extensions.containsKey("netflix_tier"));
        assertEquals(ByteBufferUtil.bytes("true"), metadata.params.extensions.get("netflix_immutable"));
    }

    @Test
    public void testUnsetMissingNetflixOptionIsNoOp()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int) WITH netflix_tier = 1");

        // Unsetting an option that was never set must not fail and must not disturb existing options.
        alterTable("ALTER TABLE %s WITH netflix_immutable = ''");

        TableMetadata metadata = currentTableMetadata();
        assertFalse(metadata.params.extensions.containsKey("netflix_immutable"));
        assertEquals(ByteBufferUtil.bytes("1"), metadata.params.extensions.get("netflix_tier"));
    }

    @Test
    public void testCreateWithUnsetSentinelDoesNotSetOption()
    {
        // On CREATE there is nothing to remove, so the empty-string sentinel is simply a no-op.
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int) WITH netflix_tier = ''");

        assertFalse(currentTableMetadata().params.extensions.containsKey("netflix_tier"));
    }

    @Test
    public void testNetflixOptionCanBeSetAgainAfterUnset()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int) WITH netflix_tier = 2");

        alterTable("ALTER TABLE %s WITH netflix_tier = ''");
        assertFalse(currentTableMetadata().params.extensions.containsKey("netflix_tier"));

        // An option can be set again after having been unset.
        alterTable("ALTER TABLE %s WITH netflix_tier = 3");
        assertEquals(ByteBufferUtil.bytes("3"), currentTableMetadata().params.extensions.get("netflix_tier"));
    }

    @Test
    public void testDescribeOutputRoundTrips()
    {
        String table = createTable("CREATE TABLE %s (k int PRIMARY KEY, v int) " +
                                    "WITH netflix_immutable = false AND netflix_tier = 4");

        String createStatement = currentTableMetadata().toCqlString(false, false);

        // The generated DESCRIBE output must be valid CQL that reproduces the options.
        String copy = table + "_copy";
        schemaChange(createStatement.replace(KEYSPACE + '.' + table, KEYSPACE + '.' + copy));

        TableMetadata recreated = Schema.instance.getTableMetadata(KEYSPACE, copy);
        assertEquals(ByteBufferUtil.bytes("false"), recreated.params.extensions.get("netflix_immutable"));
        assertEquals(ByteBufferUtil.bytes("4"), recreated.params.extensions.get("netflix_tier"));
    }

    @Test
    public void testNonCanonicalValuesAreNormalized()
    {
        // 'yes' is an accepted boolean spelling; it is stored/rendered canonically as 'true'.
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int) WITH netflix_immutable = 'yes'");

        assertEquals(ByteBufferUtil.bytes("true"), currentTableMetadata().params.extensions.get("netflix_immutable"));
        assertTrue(currentTableMetadata().toCqlString(false, false).contains("AND netflix_immutable = true"));
    }

    @Test
    public void testUnknownNetflixOptionIsRejected()
    {
        assertRejected("CREATE TABLE " + KEYSPACE + ".netflix_unknown_opt (k int PRIMARY KEY, v int) " +
                       "WITH netflix_does_not_exist = 'x'",
                       "unknown property");
    }

    @Test
    public void testTierOutOfRangeIsRejected()
    {
        assertRejected("CREATE TABLE " + KEYSPACE + ".netflix_bad_tier (k int PRIMARY KEY, v int) " +
                       "WITH netflix_tier = 5",
                       "must be an integer between 0 and 4");
    }

    @Test
    public void testNonIntegerTierIsRejected()
    {
        assertRejected("CREATE TABLE " + KEYSPACE + ".netflix_nan_tier (k int PRIMARY KEY, v int) " +
                       "WITH netflix_tier = 'gold'",
                       "invalid integer value");
    }

    @Test
    public void testNonBooleanImmutableIsRejected()
    {
        assertRejected("CREATE TABLE " + KEYSPACE + ".netflix_bad_immutable (k int PRIMARY KEY, v int) " +
                       "WITH netflix_immutable = 'maybe'",
                       "invalid boolean value");
    }

    @Test
    public void testUnknownNetflixOptionFromNewerNodeIsToleratedAndPreserved()
    {
        // Mixed-version cluster: a newer node set an option this node's enum does not know, and it
        // arrived here in the extensions map. This node must neither fail on it nor lose it.
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int) WITH netflix_tier = 2");

        TableMetadata current = currentTableMetadata();
        Map<String, ByteBuffer> extensions = new HashMap<>(current.params.extensions);
        extensions.put("netflix_future_option", ByteBufferUtil.bytes("from-newer-node"));
        TableParams params = current.params.unbuild().extensions(extensions).build();
        TableMetadata propagated = current.unbuild().params(params).build();

        // Rendering (DESCRIBE) must not throw on the unrecognized option.
        String cql = propagated.toCqlString(false, false);

        // The option this node knows is still surfaced as a first-class option...
        assertTrue(cql, cql.contains("AND netflix_tier = 2"));
        // ...while the unknown one is preserved, carried opaquely in the extensions blob and ignored.
        assertTrue(cql, cql.contains("netflix_future_option"));
        assertEquals(ByteBufferUtil.bytes("from-newer-node"),
                     propagated.params.extensions.get("netflix_future_option"));
    }

    private static void assertRejected(String cql, String expectedMessageFragment)
    {
        try
        {
            schemaChange(cql);
            fail("Expected statement to be rejected: " + cql);
        }
        catch (RuntimeException e)
        {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            assertTrue("Unexpected message: " + cause.getMessage(),
                       cause.getMessage().toLowerCase().contains(expectedMessageFragment));
        }
    }
}
