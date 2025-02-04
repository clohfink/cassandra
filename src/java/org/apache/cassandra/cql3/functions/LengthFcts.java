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

package org.apache.cassandra.cql3.functions;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Lengths of stored data without returning the data.
 * <p>
 * Backport of CASSANDRA-20102
 */
public class LengthFcts
{
    public static Collection<Function> all()
    {
        Collection<Function> functions = new ArrayList<>();

        // As all types ultimately end up as bytebuffers they should all be compatible with
        // octetlength
        Set<AbstractType<?>> types = new HashSet<>();
        for (CQL3Type type : CQL3Type.Native.values())
        {
            AbstractType<?> udfType = type.getType().udfType();
            if (!types.add(udfType))
                continue;
            functions.add(makeOctetLengthFunction(type.getType().udfType()));
        }

        // Special handling for string length which is number of utf-8 codepoints
        functions.add(length);
        return functions;
    }

    public static Function makeOctetLengthFunction(AbstractType<?> fromType)
    {
        return new NativeScalarFunction("octet_length", Int32Type.instance, fromType)
        {
            public ByteBuffer execute(ProtocolVersion protocolVersion, List<ByteBuffer> parameters)
            {
                if (argTypes.size() != 1)
                    throw new InvalidRequestException(String.format("octet_length() only accepts one argument (got %d)", argTypes.size()));

                if (parameters.get(0) == null)
                    return null;

                return ByteBufferUtil.bytes(parameters.get(0).remaining());
            }
        };
    }

    public static final Function length = new NativeScalarFunction("length", Int32Type.instance, UTF8Type.instance)
    {
        public ByteBuffer execute(ProtocolVersion protocolVersion, List<ByteBuffer> parameters)
        {
            if (argTypes.size() != 1)
                throw new InvalidRequestException(String.format("length() only accepts one argument (got %d)", argTypes.size()));

            if (parameters.get(0) == null)
                return null;

            String parsed = UTF8Type.instance.getString(parameters.get(0));
            return ByteBufferUtil.bytes(parsed.length());
        }
    };
}
