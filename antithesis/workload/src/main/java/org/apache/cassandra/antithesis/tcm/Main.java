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
package org.apache.cassandra.antithesis.tcm;

import java.util.Map;

import org.apache.cassandra.antithesis.tcm.Harness.Log;

/**
 * Entry point for every test command. Each command in {@code antithesis/test/v1/tcm/} is a thin
 * shell wrapper that invokes one subcommand here.
 *
 * <p>Exit codes matter to Antithesis: a non-zero exit marks the test command as failed. Property
 * violations are reported through SDK assertions rather than exit codes, so a command that runs to
 * completion exits 0 even if an assertion failed. A non-zero exit here means the *workload* broke
 * (bad configuration, unusable state file) -- not that the system under test did.
 */
public final class Main
{
    public static void main(String[] args)
    {
        if (args.length == 0)
        {
            System.err.println("usage: <subcommand> [args]");
            System.err.println("  wait-ready            block until every node answers CQL and JMX");
            System.err.println("  setup-cms [rf] [ms]   grow the CMS to rf members (default 3)");
            System.err.println("  create-schema         create the probe keyspace and tables");
            System.err.println("  schema-churn [n]      commit n tagged DDL transformations");
            System.err.println("  membership-churn      one random membership operation");
            System.err.println("  concurrent-movements  launch 2+ overlapping range movements");
            System.err.println("  cms-churn             one random CMS operation");
            System.err.println("  prepared-check        prepared-statement staleness check");
            System.err.println("  coordinator-behind    coordinator-behind reachability probe");
            System.err.println("  check-invariants      all continuous invariant checks");
            System.err.println("  check-recovery        quiet-period convergence and drain checks");
            System.err.println("  report                print accumulated run metadata");
            System.exit(2);
        }

        String command = args[0];
        int exit = 0;
        try (Harness harness = new Harness())
        {
            Actions actions = new Actions(harness);
            Checks checks = new Checks(harness);

            switch (command)
            {
                case "wait-ready":
                    if (!actions.waitForCluster(argLong(args, 1, 600_000)))
                        exit = 1;
                    break;

                case "setup-cms":
                    // Grow the CMS beyond its single initial member before anything else runs. Not
                    // optional: a one-member CMS is a single point of failure for all metadata
                    // commits, and it makes the CMS quorum properties trivially true.
                    if (!actions.ensureCmsReplicationFactor((int) argLong(args, 1, 3),
                                                            argLong(args, 2, 600_000)))
                        exit = 1;
                    break;

                case "create-schema":
                    actions.createProbeSchema();
                    break;

                case "schema-churn":
                    actions.schemaChurn((int) argLong(args, 1, 3));
                    break;

                case "membership-churn":
                    actions.membershipChurn();
                    break;

                case "concurrent-movements":
                    actions.launchConcurrentMovements();
                    break;

                case "replace-node":
                    actions.replaceNode();
                    break;

                case "cms-churn":
                    actions.cmsChurn();
                    break;

                case "prepared-check":
                    actions.preparedStatementStaleness();
                    break;

                case "coordinator-behind":
                    actions.coordinatorBehindProbe();
                    break;

                case "check-invariants":
                    checkInvariants(checks);
                    break;

                case "check-recovery":
                    // Order matters. Workload-induced impediments are cleared and stopped nodes
                    // restarted first: a sequence owned by a node the workload killed and never
                    // restarted is *correctly* stuck (CEP-21 forbids failure detection from
                    // cancelling sequences), so asserting it drains without restarting first would
                    // assert the opposite of the design.
                    actions.clearWorkloadFaults();
                    checks.convergence(argLong(args, 1, 600_000));
                    checks.sequencesDrained(argLong(args, 2, 600_000));
                    checks.cmsAcceptsCommit(argLong(args, 3, 300_000));
                    report(harness);
                    break;

                case "report":
                    report(harness);
                    break;

                default:
                    System.err.println("unknown subcommand: " + command);
                    exit = 2;
            }
        }
        catch (Throwable t)
        {
            // A workload crash is a harness problem, not a property violation. Fail loudly so it
            // shows up as a failed test command rather than as silence.
            System.out.println("[workload][error] " + command + " failed: " + t);
            t.printStackTrace(System.out);
            exit = 1;
        }
        System.exit(exit);
    }

    /**
     * The continuous invariant sweep, run from the {@code anytime_} command.
     *
     * <p>Ordering is deliberate: the CMS snapshot is taken once and shared, so every check that
     * depends on it sees the same instant rather than each re-reading and comparing across slightly
     * different moments. {@code allNodesCompared} runs last because it reports on how much evidence
     * everything before it managed to gather.
     */
    private static void checkInvariants(Checks checks)
    {
        Map<String, Map<String, String>> snapshot = checks.cmsSnapshot();

        checks.epochMonotonic(snapshot);
        checks.singleMetadataIdentifier(snapshot);
        checks.cmsMembershipNonEmpty(snapshot);
        checks.initializationUniform(snapshot);
        checks.reconfigurationObserved(snapshot);

        int sequencesInFlight = checks.concurrentSequences();

        checks.logPrefixAgreement(snapshot);
        checks.schemaAgreementAtSameEpoch();
        checks.placements(snapshot, sequencesInFlight);
        checks.peersMatchDirectory();
        checks.commitLedgerExactlyOnce();
        checks.availableDuringChurn();

        checks.allNodesCompared();
    }

    /**
     * Run metadata that is not an assertion but decides how to read the assertions.
     *
     * <p>Per evaluation refinements R5 and R10: a run whose ring checks all happened on a settled
     * ring tested nothing Antithesis was needed for, and a log that grows without snapshots would
     * surface as a convergence failure misdiagnosed as a catch-up bug.
     */
    private static void report(Harness harness)
    {
        Log.info("---- run metadata ----");
        harness.state.counters.forEach((k, v) -> Log.info(String.format("  %-40s %d", k, v)));

        for (Harness.Node n : harness.nodes)
        {
            Map<String, String> cms = n.describeCMS();
            if (cms == null)
            {
                Log.info(String.format("  %-20s unreachable", n.host));
                continue;
            }
            Log.info(String.format("  %-20s epoch=%s pending=%s cms_id=%s members=%s",
                                   n.host,
                                   cms.get("EPOCH"),
                                   cms.get("LOCAL_PENDING"),
                                   cms.get("CMS_ID"),
                                   cms.get("MEMBERS")));
        }
        Log.info("  ledger size: " + harness.state.tagLedger.size());
    }

    private static long argLong(String[] args, int index, long fallback)
    {
        if (args.length <= index)
            return fallback;
        try
        {
            return Long.parseLong(args[index]);
        }
        catch (NumberFormatException e)
        {
            return fallback;
        }
    }
}
