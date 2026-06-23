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
package org.apache.cassandra.service;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.apache.cassandra.db.TruncateResponse;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.config.DatabaseDescriptor.getTruncateRpcTimeout;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * Response handler for Netflix "relaxed" TRUNCATE (opted into per-table via the
 * {@code netflix_relaxed_truncate} table option; see
 * {@link com.netflix.cassandra.schema.NetflixTableOptions.Option#RELAXED_TRUNCATE}).
 *
 * Unlike {@link TruncateResponseHandler}, this handler does not fail-fast on
 * a single replica failure or treat unresponsive replicas as a hard error.
 * Instead it tracks per-endpoint outcomes and, after {@link #get()} returns,
 * exposes the set of endpoints that did not successfully ack the TRUNCATE via
 * {@link #missingResponses()}. The caller (the coordinator) decides whether
 * to throw a retriable error to the client based on whether anything is
 * missing.
 *
 * The truncate request itself has already been performed on every endpoint
 * that did ack — the relaxed semantics are entirely about how the coordinator
 * surfaces partial completion to its client, not about replica-side behavior.
 */
public class RelaxedTruncateResponseHandler extends TruncateResponseHandler
{
    private final Set<InetAddressAndPort> targets;
    /** Targets that have replied with success. */
    private final Set<InetAddressAndPort> acked = ConcurrentHashMap.newKeySet();
    /** Targets that have replied with failure. Used only for early-completion accounting. */
    private final Set<InetAddressAndPort> failed = ConcurrentHashMap.newKeySet();
    private final long start;

    public RelaxedTruncateResponseHandler(Set<InetAddressAndPort> targets)
    {
        // The caller (StorageProxy#truncateBlockingRelaxed) only constructs this handler after
        // verifying that the live-ring set is non-empty; the live set is a subset of `targets`,
        // so `targets` is necessarily non-empty here. The parent constructor's 1 <= responseCount
        // assert documents the same invariant from its side. Parent's responseCount is otherwise
        // unused — we override get()/onResponse/onFailure.
        super(targets.size());
        this.targets = ImmutableSet.copyOf(targets);
        this.start = nanoTime();
    }

    @Override
    public void onResponse(Message<TruncateResponse> message)
    {
        // Defensive: only count responses from endpoints we actually targeted. If `live` (whose
        // contents we sent to) ever drifts from `targets` (e.g. bootstrapping peers), an
        // untargeted ack could otherwise inflate the accounting and signal early-completion
        // before all real targets have responded.
        InetAddressAndPort from = message.from();
        if (!targets.contains(from))
            return;
        acked.add(from);
        if (allAccountedFor())
            condition.signalAll();
    }

    @Override
    public void onFailure(InetAddressAndPort from, RequestFailureReason failureReason)
    {
        // Don't fail-fast. Record the failure so we can early-exit once every endpoint has
        // reported, but leave the "missing" decision to missingResponses() (which keys off acked).
        if (!targets.contains(from))
            return;
        logger.debug("Relaxed TRUNCATE: replica {} reported failure ({}); will surface as missing", from, failureReason);
        failed.add(from);
        if (allAccountedFor())
            condition.signalAll();
    }

    @Override
    public boolean invokeOnFailure()
    {
        return true;
    }

    private boolean allAccountedFor()
    {
        // Every targeted endpoint has either acked or reported failure. Both sets are subsets
        // of targets (we filter in onResponse/onFailure); the request-callback contract is
        // also that exactly one of the two is invoked per request, so the sum should not
        // exceed targets.size() in practice. Using >= keeps the check robust if both ever do
        // fire for the same endpoint.
        return acked.size() + failed.size() >= targets.size();
    }

    /**
     * Block until every targeted endpoint has reported a definitive outcome
     * (ack or failure), or the truncate RPC timeout elapses. Unlike the
     * strict-mode parent, this method does not throw on partial completion;
     * the caller inspects {@link #missingResponses()} after return. An ordinary
     * timeout (some replicas didn't respond) returns normally rather than throwing.
     */
    @Override
    public void get() throws TimeoutException
    {
        long timeoutNanos = getTruncateRpcTimeout(NANOSECONDS) - (nanoTime() - start);
        if (timeoutNanos <= 0)
            return; // already past deadline; let caller inspect missingResponses()
        try
        {
            condition.await(timeoutNanos, NANOSECONDS);
        }
        catch (InterruptedException e)
        {
            throw new UncheckedInterruptedException(e);
        }
        // Either everyone responded, or we timed out. Either way the caller
        // makes the success/retry decision based on missingResponses().
    }

    /**
     * The set of targeted endpoints that did not successfully ack the
     * TRUNCATE. This is the union of explicit failures reported via
     * {@link #onFailure} and endpoints that never responded within the
     * truncate RPC timeout (covers both gossip-unreachable peers and
     * live-but-unresponsive peers — a single accounting).
     */
    public Set<InetAddressAndPort> missingResponses()
    {
        // A target is "missing" iff it never acked. Explicit failures (tracked
        // in `failed`) are by construction also absent from `acked`, so the
        // single "targets - acked" computation already covers them.
        return ImmutableSet.copyOf(Sets.difference(targets, acked));
    }
}
