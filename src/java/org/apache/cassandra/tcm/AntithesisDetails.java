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
package org.apache.cassandra.tcm;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Builds the {@code details} payload carried by Antithesis SDK assertions in the TCM package.
 *
 * <p>Assertions are placed on TCM's cold paths only -- log entry enactment, range-movement
 * admission, progress barriers, and the CMS commit decision. None of these run per-request, so
 * building a small JSON object at each is not on any hot path.
 *
 * <p>This class deliberately does <em>not</em> wrap the assertion calls themselves. The Antithesis
 * assertion cataloger requires each {@code message} to be a string literal or compile-time constant
 * at the call site -- each distinct message becomes its own test property -- so wrapping
 * {@code Assert.always(...)} behind a helper that took the message as a parameter would break
 * cataloging. Only the payload construction is shared.
 *
 * <p>Every assertion in the TCM package is a no-op outside the Antithesis environment (the SDK falls
 * back to doing nothing unless {@code ANTITHESIS_SDK_LOCAL_OUTPUT} is set) and no SDK assertion ever
 * terminates the process. The corresponding properties are catalogued in
 * {@code antithesis/scratchbook/property-catalog.md}.
 */
public final class AntithesisDetails
{
    /**
     * Gate for the (relatively expensive) cluster-metadata serialization round-trip check in
     * {@code LocalLog}. Serializing the full {@link ClusterMetadata} on every committed epoch is
     * cheap enough for a small simulated cluster but is pure overhead in production, so it is only
     * performed when the Antithesis node image sets
     * {@code -Dcassandra.antithesis.serialization_check=true} (see
     * {@code antithesis/docker/entrypoint-cassandra.sh}). Unset outside Antithesis, so production is
     * unaffected.
     */
    public static final boolean SERIALIZATION_CHECK =
        Boolean.getBoolean("cassandra.antithesis.serialization_check");

    private AntithesisDetails()
    {
    }

    /**
     * Builds an {@link ObjectNode} from alternating key/value pairs. Values are rendered with
     * {@code String.valueOf} unless they are numeric or boolean, which keeps the payload usable for
     * filtering in the triage report without needing serializers for TCM's own types.
     *
     * @param keysAndValues alternating {@code String} key and arbitrary value; must be even length
     */
    public static ObjectNode of(Object... keysAndValues)
    {
        ObjectNode node = new ObjectNode(JsonNodeFactory.instance);
        if (keysAndValues == null)
            return node;

        // Tolerate a malformed call rather than throwing: an exception raised while building a
        // diagnostic payload would turn an observation into an outage, and these call sites sit on
        // the metadata publication path.
        int count = keysAndValues.length - (keysAndValues.length % 2);
        for (int i = 0; i < count; i += 2)
        {
            String key = String.valueOf(keysAndValues[i]);
            Object value = keysAndValues[i + 1];
            if (value == null)
                node.putNull(key);
            else if (value instanceof Integer)
                node.put(key, (Integer) value);
            else if (value instanceof Long)
                node.put(key, (Long) value);
            else if (value instanceof Boolean)
                node.put(key, (Boolean) value);
            else
                node.put(key, String.valueOf(value));
        }
        return node;
    }
}
