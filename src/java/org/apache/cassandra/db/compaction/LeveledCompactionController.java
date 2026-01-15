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

package org.apache.cassandra.db.compaction;

import java.util.Set;
import java.util.function.LongPredicate;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.io.sstable.format.SSTableReader;

/**
 * CompactionController for LeveledCompactionStrategy that supports aggressive SSTable expiration.
 * 
 * When ignoreOverlaps is enabled, this controller will:
 * 1. Skip overlap checking when determining if an SSTable is fully expired
 * 2. Aggressively purge tombstones without checking for shadowed data in other SSTables
 * 
 * This is useful for workloads with TTL data where data resurrection is not a concern
 * (e.g., insert-only workloads without updates or deletes).
 */
public class LeveledCompactionController extends CompactionController
{
    private final boolean ignoreOverlaps;

    public LeveledCompactionController(ColumnFamilyStore cfs, Set<SSTableReader> compacting, int gcBefore, boolean ignoreOverlaps)
    {
        super(cfs, compacting, gcBefore, null,
              cfs.getCompactionStrategyManager().getCompactionParams().tombstoneOption(), ignoreOverlaps);
        this.ignoreOverlaps = ignoreOverlaps;
    }

    @Override
    protected boolean ignoreOverlaps()
    {
        return ignoreOverlaps;
    }

    @Override
    public LongPredicate getPurgeEvaluator(DecoratedKey key)
    {
        if (NEVER_PURGE_TOMBSTONES || !compactingRepaired() || cfs.getNeverPurgeTombstones())
        {
            return time -> false;
        }
        if (ignoreOverlaps)
        {
            return time -> true;
        }
        return super.getPurgeEvaluator(key);
    }
}
