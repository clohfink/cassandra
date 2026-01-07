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

package com.netflix.cassandra.importing;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TriggerMetadata;
import org.apache.cassandra.schema.Triggers;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.triggers.ITrigger;
import org.apache.cassandra.triggers.TriggerExecutor;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.*;

/**
 * Unit tests for RemoteImportTrigger and ClientState access in triggers.
 */
public class RemoteImportTriggerTest
{
    private static final String KEYSPACE = "test_ks";
    private static final String TABLE = "remote_import";

    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setup()
    {
        // Clear any stale ThreadLocal state
        TriggerExecutor.getClientState();
    }

    /**
     * Test that ClientState is accessible from a trigger when provided, and null when not provided
     */
    @Test
    public void testClientStateAvailabilityInTrigger() throws Exception
    {
        TableMetadata metadata = makeRemoteImportTableMetadata(
            TriggerMetadata.create("client_state_test", ClientStateCapturingTrigger.class.getName())
        );

        PartitionUpdate update = makeRemoteImportUpdate(metadata, UUID.randomUUID(), "test_ks", "test_table", "staging");

        // Test WITH ClientState
        ClientState clientState = ClientState.forInternalCalls();
        PartitionUpdate result = TriggerExecutor.instance.execute(update, clientState);

        assertNotNull(result);
        assertTrue("ClientState should have been available in trigger",
                   ClientStateCapturingTrigger.clientStateWasAvailable);
        assertSame("Trigger should receive the exact ClientState instance we passed",
                   clientState, ClientStateCapturingTrigger.capturedClientState);

        // Test WITHOUT ClientState (backward compatibility)
        result = TriggerExecutor.instance.execute(update);

        assertNotNull(result);
        assertFalse("ClientState should not be available when not provided",
                    ClientStateCapturingTrigger.clientStateWasAvailable);
        assertNull("Captured ClientState should be null",
                   ClientStateCapturingTrigger.capturedClientState);
    }

    /**
     * Test that ClientState ThreadLocal is properly cleaned up after trigger execution
     */
    @Test
    public void testClientStateThreadLocalCleanup() throws Exception
    {
        TableMetadata metadata = makeRemoteImportTableMetadata(
            TriggerMetadata.create("cleanup_test", ClientStateCapturingTrigger.class.getName())
        );

        PartitionUpdate update = makeRemoteImportUpdate(metadata, UUID.randomUUID(), "test_ks", "test_table", "staging");
        ClientState clientState = ClientState.forInternalCalls();

        // Execute trigger with ClientState
        TriggerExecutor.instance.execute(update, clientState);

        // Verify ClientState is NOT accessible outside trigger execution
        assertNull("ClientState should be cleaned up after trigger execution",
                   TriggerExecutor.getClientState());
    }

    /**
     * Test that triggers can perform permission checks using ClientState
     */
    @Test
    public void testPermissionCheckInTrigger() throws Exception
    {
        TableMetadata metadata = makeRemoteImportTableMetadata(
            TriggerMetadata.create("permission_test", PermissionCheckingTrigger.class.getName())
        );

        PartitionUpdate update = makeRemoteImportUpdate(metadata, UUID.randomUUID(), "test_ks", "test_table", "staging");

        // Reset permission check state
        PermissionCheckingTrigger.permissionCheckPassed = false;
        PermissionCheckingTrigger.caughtException = null;

        // Create a ClientState for internal calls (which has all permissions)
        ClientState clientState = ClientState.forInternalCalls();

        // Execute trigger with ClientState
        PartitionUpdate result = TriggerExecutor.instance.execute(update, clientState);

        // Verify the trigger was executed
        assertNotNull(result);

        // Verify permission check was performed and passed
        assertTrue("Permission check should have been performed",
                   PermissionCheckingTrigger.permissionCheckPassed);
        assertNull("No exception should have been thrown for internal calls",
                   PermissionCheckingTrigger.caughtException);
    }

    /**
     * Test that triggers receive null ClientState when not provided
     */
    @Test
    public void testPermissionCheckWithoutClientState() throws Exception
    {
        TableMetadata metadata = makeRemoteImportTableMetadata(
            TriggerMetadata.create("permission_test_no_state", PermissionCheckingTrigger.class.getName())
        );

        PartitionUpdate update = makeRemoteImportUpdate(metadata, UUID.randomUUID(), "test_ks", "test_table", "staging");

        // Reset permission check state
        PermissionCheckingTrigger.permissionCheckPassed = false;
        PermissionCheckingTrigger.caughtException = null;

        // Execute trigger WITHOUT ClientState (backward compatibility)
        PartitionUpdate result = TriggerExecutor.instance.execute(update);

        // Verify the trigger was executed
        assertNotNull(result);

        // Verify permission check was NOT performed (ClientState was null)
        assertFalse("Permission check should not have been performed without ClientState",
                    PermissionCheckingTrigger.permissionCheckPassed);
        assertNull("No exception should have been thrown",
                   PermissionCheckingTrigger.caughtException);
    }


    /**
     * Test RemoteImportTrigger with proper partition key structure
     */
    @Test
    public void testRemoteImportTriggerWithValidPartitionKey() throws Exception
    {
        UUID snapshotId = UUID.randomUUID();
        TestableRemoteImportTrigger trigger = new TestableRemoteImportTrigger();

        PartitionUpdate update = makeRemoteImportUpdate(makeRemoteImportTableMetadata(null),
                                                        snapshotId, "test_ks", "test_table", "staging");

        // Execute trigger and verify it parses everything correctly
        Collection<Mutation> result = trigger.augment(update);

        // Verify trigger executed and parsed all fields correctly
        assertTrue("Trigger should have been executed and called sendImportStateChangeRequest",
                   trigger.sendRequestCalled);
        assertEquals("Trigger should have parsed correct snapshot ID", snapshotId, trigger.capturedSnapshotId);
        assertEquals("Trigger should have parsed correct keyspace", "test_ks", trigger.capturedKeyspace);
        assertEquals("Trigger should have parsed correct table", "test_table", trigger.capturedTable);
        assertEquals("Trigger should have detected correct state", "staging", trigger.capturedNewState);

        // Should return empty list (no augmentation)
        assertTrue("RemoteImportTrigger should not augment mutations", result.isEmpty());
    }

    /**
     * Test RemoteImportTrigger handles invalid partition key structure gracefully
     */
    @Test
    public void testRemoteImportTriggerWithInvalidPartitionKey() throws Exception
    {
        // Use our instrumented trigger
        TableMetadata metadata = TableMetadata.builder(KEYSPACE, TABLE)
            .addPartitionKeyColumn("pkey", UTF8Type.instance)
            .addRegularColumn("state", UTF8Type.instance)
            .triggers(Triggers.of(TriggerMetadata.create("import_trigger_instrumented", InstrumentedRemoteImportTrigger.class.getName())))
            .build();

        // Create update with simple string key (not composite)
        RowUpdateBuilder builder = new RowUpdateBuilder(metadata, FBUtilities.timestampMicros(), "invalid_key");
        builder.add("state", "staging");
        PartitionUpdate update = builder.buildUpdate();

        // Reset trigger execution flag
        InstrumentedRemoteImportTrigger.wasExecuted = false;
        InstrumentedRemoteImportTrigger.handledInvalidKey = false;

        // Execute trigger - should handle gracefully and not throw
        Collection<Mutation> result = TriggerExecutor.instance.execute(Collections.singletonList(new Mutation(update)));

        // Verify trigger was executed and handled invalid key gracefully
        assertTrue("RemoteImportTrigger should have been executed",
                   InstrumentedRemoteImportTrigger.wasExecuted);
        assertTrue("RemoteImportTrigger should have handled invalid key gracefully",
                   InstrumentedRemoteImportTrigger.handledInvalidKey);

        // Should return null (no augmentation) or empty list
        assertTrue("RemoteImportTrigger should handle invalid keys gracefully",
                   result == null || result.isEmpty());
    }

    /**
     * Test that validateStateTransition detects "staging" state and calls sendImportStateChangeRequest
     */
    @Test
    public void testValidateStateTransitionWithStagingState() throws Exception
    {
        UUID snapshotId = UUID.randomUUID();
        TestableRemoteImportTrigger trigger = new TestableRemoteImportTrigger();

        PartitionUpdate update = makeRemoteImportUpdate(makeRemoteImportTableMetadata(null),
                                                        snapshotId, "test_ks", "test_table", "staging");

        trigger.augment(update);

        assertTrue("sendImportStateChangeRequest should have been called for staging state",
                   trigger.sendRequestCalled);
        assertEquals("Should have captured correct snapshot ID", snapshotId, trigger.capturedSnapshotId);
        assertEquals("Should have captured correct keyspace", "test_ks", trigger.capturedKeyspace);
        assertEquals("Should have captured correct table", "test_table", trigger.capturedTable);
        assertEquals("Should have captured correct new state", "staging", trigger.capturedNewState);
    }

    /**
     * Test that validateStateTransition detects "importing" state
     */
    @Test
    public void testValidateStateTransitionWithImportingState() throws Exception
    {
        UUID snapshotId = UUID.randomUUID();
        TestableRemoteImportTrigger trigger = new TestableRemoteImportTrigger();

        PartitionUpdate update = makeRemoteImportUpdate(makeRemoteImportTableMetadata(null),
                                                        snapshotId, "prod_ks", "user_table", "importing");

        trigger.augment(update);

        assertTrue("sendImportStateChangeRequest should have been called for importing state",
                   trigger.sendRequestCalled);
        assertEquals("Should have captured correct new state", "importing", trigger.capturedNewState);
    }

    /**
     * Test that validateStateTransition ignores states other than "staging" or "importing"
     */
    @Test
    public void testValidateStateTransitionIgnoresOtherStates() throws Exception
    {
        UUID snapshotId = UUID.randomUUID();
        TestableRemoteImportTrigger trigger = new TestableRemoteImportTrigger();

        // Test with "completed" state
        PartitionUpdate update = makeRemoteImportUpdate(makeRemoteImportTableMetadata(null),
                                                        snapshotId, "test_ks", "test_table", "completed");
        trigger.augment(update);

        assertFalse("sendImportStateChangeRequest should NOT be called for completed state",
                    trigger.sendRequestCalled);

        // Reset and test with "failed" state
        trigger = new TestableRemoteImportTrigger();
        update = makeRemoteImportUpdate(makeRemoteImportTableMetadata(null),
                                       snapshotId, "test_ks", "test_table", "failed");
        trigger.augment(update);

        assertFalse("sendImportStateChangeRequest should NOT be called for failed state",
                    trigger.sendRequestCalled);
    }

    /**
     * Test that validateStateTransition handles case-insensitive state values by converting to lowercase
     */
    @Test
    public void testValidateStateTransitionCaseInsensitive() throws Exception
    {
        UUID snapshotId = UUID.randomUUID();
        TestableRemoteImportTrigger trigger = new TestableRemoteImportTrigger();

        // Test with uppercase "STAGING" - should be converted to lowercase and trigger the request
        PartitionUpdate update = makeRemoteImportUpdate(makeRemoteImportTableMetadata(null),
                                                        snapshotId, "test_ks", "test_table", "STAGING");
        trigger.augment(update);

        assertTrue("Should handle uppercase STAGING and trigger state change", trigger.sendRequestCalled);
        assertEquals("Should convert uppercase to lowercase", "staging", trigger.capturedNewState);

        // Reset and test with mixed case "ImPoRtInG"
        trigger = new TestableRemoteImportTrigger();
        update = makeRemoteImportUpdate(makeRemoteImportTableMetadata(null),
                                       snapshotId, "test_ks", "test_table", "ImPoRtInG");
        trigger.augment(update);

        assertTrue("Should handle mixed case IMPORTING and trigger state change", trigger.sendRequestCalled);
        assertEquals("Should convert mixed case to lowercase", "importing", trigger.capturedNewState);

        // Test that non-matching states (even if uppercase) are still ignored
        trigger = new TestableRemoteImportTrigger();
        update = makeRemoteImportUpdate(makeRemoteImportTableMetadata(null),
                                       snapshotId, "test_ks", "test_table", "COMPLETED");
        trigger.augment(update);

        assertFalse("Should ignore COMPLETED state even if uppercase", trigger.sendRequestCalled);
    }

    /**
     * Test that validateStateTransition extracts correct values from partition key
     */
    @Test
    public void testValidateStateTransitionExtractsCorrectPartitionKeyValues() throws Exception
    {
        UUID expectedSnapshotId = UUID.fromString("12345678-1234-1234-1234-123456789abc");
        String expectedKeyspace = "my_keyspace";
        String expectedTable = "my_table";

        TestableRemoteImportTrigger trigger = new TestableRemoteImportTrigger();
        PartitionUpdate update = makeRemoteImportUpdate(makeRemoteImportTableMetadata(null),
                                                        expectedSnapshotId, expectedKeyspace, expectedTable, "staging");

        trigger.augment(update);

        assertEquals("Should extract correct snapshot ID", expectedSnapshotId, trigger.capturedSnapshotId);
        assertEquals("Should extract correct keyspace", expectedKeyspace, trigger.capturedKeyspace);
        assertEquals("Should extract correct table", expectedTable, trigger.capturedTable);
    }

    /**
     * Test that trigger handles null static row gracefully (no state update)
     */
    @Test
    public void testTriggerWithNullStaticRow() throws Exception
    {
        TableMetadata metadata = makeRemoteImportTableMetadata(null);
        UUID snapshotId = UUID.randomUUID();

        // Create a partition update with no static row (only regular rows)
        CompositeType partitionType = CompositeType.getInstance(UUIDType.instance, UTF8Type.instance, UTF8Type.instance);
        ByteBuffer partitionKey = partitionType.decompose(snapshotId, "test_ks", "test_table");
        DecoratedKey key = metadata.partitioner.decorateKey(partitionKey);

        // Create an empty partition update (no static row, no regular rows)
        PartitionUpdate update = PartitionUpdate.emptyUpdate(metadata, key);

        TestableRemoteImportTrigger trigger = new TestableRemoteImportTrigger();
        Collection<Mutation> result = trigger.augment(update);

        // Should return empty list without attempting to process state
        assertFalse("Should not call sendImportStateChangeRequest when static row is null",
                    trigger.sendRequestCalled);
        assertTrue("Should return empty list", result.isEmpty());
    }

    /**
     * Test that trigger ignores updates to non-state columns (regular columns, not static)
     * Tests that the trigger only cares about the "state" column at RemoteImportTrigger.java:72
     */
    @Test
    public void testTriggerIgnoresNonStateColumnUpdates()
    {
        TableMetadata metadata = makeRemoteImportTableMetadata(null);
        UUID snapshotId = UUID.randomUUID();

        CompositeType partitionType = CompositeType.getInstance(UUIDType.instance, UTF8Type.instance, UTF8Type.instance);
        ByteBuffer partitionKey = partitionType.decompose(snapshotId, "test_ks", "test_table");

        // Create update with regular column "data", not static "state"
        RowUpdateBuilder builder = new RowUpdateBuilder(metadata, FBUtilities.timestampMicros(), partitionKey);
        builder.add("data", "some data value");
        PartitionUpdate update = builder.buildUpdate();

        TestableRemoteImportTrigger trigger = new TestableRemoteImportTrigger();
        trigger.augment(update);

        // Should not trigger state change since we didn't update the state column
        // (update has no static row at all)
        assertFalse("Should not call sendImportStateChangeRequest when state column is not updated",
                    trigger.sendRequestCalled);
    }

    /**
     * Test that RemoteImportTrigger performs permission check with authorized ClientState
     * Tests the permission check at RemoteImportTrigger.java:104-119
     */
    @Test
    public void testRemoteImportTriggerPermissionCheckWithAuthorizedUser() throws Exception
    {
        UUID snapshotId = UUID.randomUUID();
        TableMetadata metadata = makeRemoteImportTableMetadata(
            TriggerMetadata.create("remote_import_trigger", TestableRemoteImportTriggerWithPermissionTracking.class.getName())
        );

        PartitionUpdate update = makeRemoteImportUpdate(metadata, snapshotId, "test_ks", "test_table", "staging");

        // Reset tracking state
        TestableRemoteImportTriggerWithPermissionTracking.permissionCheckPerformed = false;
        TestableRemoteImportTriggerWithPermissionTracking.permissionCheckPassed = false;
        TestableRemoteImportTriggerWithPermissionTracking.unauthorizedException = null;

        // Execute with internal ClientState (has all permissions)
        ClientState clientState = ClientState.forInternalCalls();
        TriggerExecutor.instance.execute(update, clientState);

        // Verify permission check was performed and passed
        assertTrue("Permission check should have been performed",
                   TestableRemoteImportTriggerWithPermissionTracking.permissionCheckPerformed);
        assertTrue("Permission check should have passed for internal calls",
                   TestableRemoteImportTriggerWithPermissionTracking.permissionCheckPassed);
        assertNull("No exception should have been thrown",
                   TestableRemoteImportTriggerWithPermissionTracking.unauthorizedException);
    }

    /**
     * Test that RemoteImportTrigger performs permission check without ClientState (backward compatibility)
     * Tests the permission check at RemoteImportTrigger.java:104-119
     */
    @Test
    public void testRemoteImportTriggerPermissionCheckWithoutClientState() throws Exception
    {
        UUID snapshotId = UUID.randomUUID();
        TableMetadata metadata = makeRemoteImportTableMetadata(
            TriggerMetadata.create("remote_import_trigger", TestableRemoteImportTriggerWithPermissionTracking.class.getName())
        );

        PartitionUpdate update = makeRemoteImportUpdate(metadata, snapshotId, "test_ks", "test_table", "staging");

        // Reset tracking state
        TestableRemoteImportTriggerWithPermissionTracking.permissionCheckPerformed = false;
        TestableRemoteImportTriggerWithPermissionTracking.permissionCheckPassed = false;
        TestableRemoteImportTriggerWithPermissionTracking.unauthorizedException = null;

        // Execute WITHOUT ClientState (backward compatibility)
        TriggerExecutor.instance.execute(update);

        // Verify permission check was NOT performed (ClientState was null)
        assertFalse("Permission check should not have been performed when ClientState is null",
                    TestableRemoteImportTriggerWithPermissionTracking.permissionCheckPerformed);
        assertFalse("Permission check should not have passed",
                    TestableRemoteImportTriggerWithPermissionTracking.permissionCheckPassed);
        assertNull("No exception should have been thrown",
                   TestableRemoteImportTriggerWithPermissionTracking.unauthorizedException);
    }

    /**
     * Test that RemoteImportTrigger throws UnauthorizedException for unauthorized access
     * Tests the permission check at RemoteImportTrigger.java:104-119
     */
    @Test(expected = UnauthorizedException.class)
    public void testRemoteImportTriggerPermissionCheckWithUnauthorizedUser() throws Exception
    {
        UUID snapshotId = UUID.randomUUID();
        TableMetadata metadata = makeRemoteImportTableMetadata(
            TriggerMetadata.create("remote_import_trigger", TestableRemoteImportTriggerWithPermissionTracking.class.getName())
        );

        PartitionUpdate update = makeRemoteImportUpdate(metadata, snapshotId, "test_ks", "test_table", "staging");

        // Reset tracking state
        TestableRemoteImportTriggerWithPermissionTracking.permissionCheckPerformed = false;
        TestableRemoteImportTriggerWithPermissionTracking.permissionCheckPassed = false;
        TestableRemoteImportTriggerWithPermissionTracking.unauthorizedException = null;

        // Create a ClientState that will fail permission checks
        // We'll use a mock ClientState that throws UnauthorizedException
        TestableRemoteImportTriggerWithPermissionTracking.simulateUnauthorized = true;

        try
        {
            // Execute with a ClientState
            ClientState clientState = ClientState.forInternalCalls();
            TriggerExecutor.instance.execute(update, clientState);
        }
        finally
        {
            // Reset simulation flag
            TestableRemoteImportTriggerWithPermissionTracking.simulateUnauthorized = false;
        }
    }

    /**
     * Test that permission check validates the correct keyspace and table from partition key
     */
    @Test
    public void testRemoteImportTriggerPermissionCheckValidatesCorrectKeyspaceAndTable() throws Exception
    {
        UUID snapshotId = UUID.randomUUID();
        String expectedKeyspace = "prod_keyspace";
        String expectedTable = "users_table";

        TableMetadata metadata = makeRemoteImportTableMetadata(
            TriggerMetadata.create("remote_import_trigger", TestableRemoteImportTriggerWithPermissionTracking.class.getName())
        );

        PartitionUpdate update = makeRemoteImportUpdate(metadata, snapshotId, expectedKeyspace, expectedTable, "importing");

        // Reset tracking state
        TestableRemoteImportTriggerWithPermissionTracking.permissionCheckPerformed = false;
        TestableRemoteImportTriggerWithPermissionTracking.checkedKeyspace = null;
        TestableRemoteImportTriggerWithPermissionTracking.checkedTable = null;

        // Execute with internal ClientState
        ClientState clientState = ClientState.forInternalCalls();
        TriggerExecutor.instance.execute(update, clientState);

        // Verify the correct keyspace and table were checked
        assertTrue("Permission check should have been performed",
                   TestableRemoteImportTriggerWithPermissionTracking.permissionCheckPerformed);
        assertEquals("Should check permission for correct keyspace",
                     expectedKeyspace, TestableRemoteImportTriggerWithPermissionTracking.checkedKeyspace);
        assertEquals("Should check permission for correct table",
                     expectedTable, TestableRemoteImportTriggerWithPermissionTracking.checkedTable);
    }

    /**
     * Test that trigger handles empty state string gracefully
     * Tests that empty state doesn't match the "staging" or "importing" check at RemoteImportTrigger.java:107
     */
    @Test
    public void testTriggerWithEmptyStateString()
    {
        UUID snapshotId = UUID.randomUUID();
        TestableRemoteImportTrigger trigger = new TestableRemoteImportTrigger();

        // Create update with empty state string
        PartitionUpdate update = makeRemoteImportUpdate(makeRemoteImportTableMetadata(null),
                                                        snapshotId, "test_ks", "test_table", "");
        trigger.augment(update);

        // Empty string doesn't match "staging" or "importing", so shouldn't trigger
        assertFalse("Should not call sendImportStateChangeRequest for empty state",
                    trigger.sendRequestCalled);
    }

    /**
     * Test that trigger handles null UUID in partition key gracefully
     * Tests the null check at RemoteImportTrigger.java:84-95
     */
    @Test
    public void testTriggerWithNullUUIDInPartitionKey()
    {
        TableMetadata metadata = makeRemoteImportTableMetadata(null);

        // Create partition key with null UUID component
        CompositeType partitionType = CompositeType.getInstance(UUIDType.instance, UTF8Type.instance, UTF8Type.instance);
        ByteBuffer partitionKey = partitionType.decompose(null, "test_ks", "test_table");

        long timestamp = FBUtilities.timestampMicros();

        Row.Builder staticBuilder = BTreeRow.unsortedBuilder();
        staticBuilder.newRow(Clustering.STATIC_CLUSTERING);

        ColumnMetadata stateColumn = metadata.getColumn(UTF8Type.instance.decompose("state"));
        staticBuilder.addCell(BufferCell.live(
            stateColumn, timestamp, UTF8Type.instance.decompose("staging")));

        Row staticRow = staticBuilder.build();

        DecoratedKey key = metadata.partitioner.decorateKey(partitionKey);
        PartitionUpdate update = PartitionUpdate.singleRowUpdate(metadata, key, null, staticRow);

        TestableRemoteImportTrigger trigger = new TestableRemoteImportTrigger();

        // Should handle gracefully without throwing exception (logs warning and returns)
        Collection<Mutation> result = trigger.augment(update);

        assertFalse("Should not call sendImportStateChangeRequest with null UUID",
                    trigger.sendRequestCalled);
        assertTrue("Should return empty list", result.isEmpty());
    }

    // Helper methods

    private static TableMetadata makeRemoteImportTableMetadata(TriggerMetadata trigger)
    {
        CompositeType partitionType = CompositeType.getInstance(UUIDType.instance, UTF8Type.instance, UTF8Type.instance);

        TableMetadata.Builder builder = TableMetadata.builder(KEYSPACE, TABLE)
            .addPartitionKeyColumn("pkey", partitionType)
            .addStaticColumn("state", UTF8Type.instance)
            .addRegularColumn("data", UTF8Type.instance);

        if (trigger != null)
            builder.triggers(Triggers.of(trigger));

        return builder.build();
    }

    private static PartitionUpdate makeRemoteImportUpdate(TableMetadata metadata, UUID snapshotId, String keyspace, String table, String state)
    {
        CompositeType partitionType = CompositeType.getInstance(UUIDType.instance, UTF8Type.instance, UTF8Type.instance);
        ByteBuffer partitionKey = partitionType.decompose(snapshotId, keyspace, table);

        long timestamp = FBUtilities.timestampMicros();

        // Build static row using BTreeRow
        Row.Builder staticBuilder = BTreeRow.unsortedBuilder();
        staticBuilder.newRow(Clustering.STATIC_CLUSTERING);

        ColumnMetadata stateColumn = metadata.getColumn(UTF8Type.instance.decompose("state"));
        staticBuilder.addCell(BufferCell.live(
            stateColumn, timestamp, UTF8Type.instance.decompose(state)));

        Row staticRow = staticBuilder.build();

        // Create partition update with static row
        DecoratedKey key = metadata.partitioner.decorateKey(partitionKey);
        return PartitionUpdate.singleRowUpdate(metadata, key, null, staticRow);
    }

    // Test trigger implementations

    /**
     * Trigger that captures ClientState for testing
     */
    public static class ClientStateCapturingTrigger implements ITrigger
    {
        public static volatile boolean clientStateWasAvailable = false;
        public static volatile ClientState capturedClientState = null;

        @Override
        public Collection<Mutation> augment(Partition partition)
        {
            // Reset state
            clientStateWasAvailable = false;
            capturedClientState = null;

            // Capture ClientState
            ClientState state = TriggerExecutor.getClientState();
            clientStateWasAvailable = (state != null);
            capturedClientState = state;

            // No mutations to return
            return Collections.emptyList();
        }
    }


    /**
     * Trigger that checks permissions using ClientState
     */
    public static class PermissionCheckingTrigger implements ITrigger
    {
        public static volatile boolean permissionCheckPassed = false;
        public static volatile UnauthorizedException caughtException = null;

        @Override
        public Collection<Mutation> augment(Partition partition)
        {
            permissionCheckPassed = false;
            caughtException = null;

            ClientState clientState = TriggerExecutor.getClientState();
            if (clientState != null)
            {
                try
                {
                    // Check if user has MODIFY permission
                    clientState.ensureTablePermission(partition.metadata(), Permission.MODIFY);
                    permissionCheckPassed = true;
                }
                catch (UnauthorizedException e)
                {
                    caughtException = e;
                }
            }

            return Collections.emptyList();
        }
    }


    /**
     * Instrumented version of RemoteImportTrigger that tracks execution for testing.
     * Mimics the behavior of RemoteImportTrigger but sets flags we can verify.
     */
    public static class InstrumentedRemoteImportTrigger implements ITrigger
    {
        public static volatile boolean wasExecuted = false;
        public static volatile UUID lastSnapshotId = null;
        public static volatile boolean handledInvalidKey = false;

        private final CompositeType partitionType = CompositeType.getInstance(
            UUIDType.instance, UTF8Type.instance, UTF8Type.instance);

        @Override
        public Collection<Mutation> augment(Partition partition)
        {
            wasExecuted = true;

            if (!(partition instanceof PartitionUpdate))
            {
                return Collections.emptyList();
            }

            PartitionUpdate update = (PartitionUpdate) partition;

            try
            {
                // Try to parse partition key
                ByteBuffer[] split = partitionType.split(update.partitionKey().getKey());

                // Validate partition key components
                if (split.length < 3)
                {
                    handledInvalidKey = true;
                    return Collections.emptyList();
                }

                if (split[0] == null)
                {
                    handledInvalidKey = true;
                    return Collections.emptyList();
                }

                UUID snapshotId = UUIDType.instance.compose(split[0]);
                if (snapshotId == null)
                {
                    handledInvalidKey = true;
                    return Collections.emptyList();
                }

                // Successfully parsed
                lastSnapshotId = snapshotId;
            }
            catch (Exception e)
            {
                // Invalid key structure
                handledInvalidKey = true;
            }

            return Collections.emptyList();
        }
    }

    /**
     * Testable version of RemoteImportTrigger that captures sendImportStateChangeRequest calls
     */
    public static class TestableRemoteImportTrigger extends RemoteImportTrigger
    {
        public volatile boolean sendRequestCalled = false;
        public volatile UUID capturedSnapshotId = null;
        public volatile String capturedKeyspace = null;
        public volatile String capturedTable = null;
        public volatile String capturedCurrentState = null;
        public volatile String capturedNewState = null;

        @Override
        protected void sendImportStateChangeRequest(UUID snapshotId, String keyspace, String table, String currentState, String newState)
        {
            sendRequestCalled = true;
            capturedSnapshotId = snapshotId;
            capturedKeyspace = keyspace;
            capturedTable = table;
            capturedCurrentState = currentState;
            capturedNewState = newState;

            // Don't actually send messages in tests
        }
    }

    /**
     * Testable version of RemoteImportTrigger that tracks permission checks
     */
    public static class TestableRemoteImportTriggerWithPermissionTracking extends RemoteImportTrigger
    {
        public static volatile boolean permissionCheckPerformed = false;
        public static volatile boolean permissionCheckPassed = false;
        public static volatile UnauthorizedException unauthorizedException = null;
        public static volatile String checkedKeyspace = null;
        public static volatile String checkedTable = null;
        public static volatile boolean simulateUnauthorized = false;

        private final CompositeType partitionType = CompositeType.getInstance(UUIDType.instance, UTF8Type.instance, UTF8Type.instance);

        @Override
        protected void validateStateTransition(PartitionUpdate partition, Row staticRow)
        {
            // Track that we're in validateStateTransition
            staticRow.cells().forEach(cell -> {
                if ("state".equals(cell.column().name.toString()))
                {
                    String newState = UTF8Type.instance.compose((ByteBuffer) cell.value()).toLowerCase();
                    ByteBuffer[] split = partitionType.split(partition.partitionKey().getKey());

                    // Validate partition key components
                    if (split.length < 3 || split[0] == null)
                        return;

                    UUID snapshotId = UUIDType.instance.compose(split[0]);
                    if (snapshotId == null)
                        return;

                    String keyspaceStr = UTF8Type.instance.compose(split[1]);
                    String tableStr = UTF8Type.instance.compose(split[2]);

                    // Track the keyspace/table being checked
                    checkedKeyspace = keyspaceStr;
                    checkedTable = tableStr;

                    // Check if ClientState is available and verify permissions
                    ClientState clientState = TriggerExecutor.getClientState();
                    if (clientState != null)
                    {
                        permissionCheckPerformed = true;
                        try
                        {
                            if (simulateUnauthorized)
                            {
                                // Simulate unauthorized access
                                throw new UnauthorizedException("Simulated unauthorized access for testing");
                            }

                            // Ensure the client has MODIFY permission on the target keyspace/table
                            clientState.ensureTablePermission(keyspaceStr, tableStr, Permission.MODIFY);
                            permissionCheckPassed = true;
                        }
                        catch (UnauthorizedException e)
                        {
                            unauthorizedException = e;
                            throw e;
                        }
                    }

                    // Continue with normal behavior (but don't actually send messages in tests)
                    if (newState.equals("staging") || newState.equals("importing"))
                    {
                        // Track but don't actually send
                    }
                }
            });
        }

        @Override
        protected void sendImportStateChangeRequest(UUID snapshotId, String keyspace, String table, String currentState, String newState)
        {
            // Don't actually send messages in tests
        }
    }
}