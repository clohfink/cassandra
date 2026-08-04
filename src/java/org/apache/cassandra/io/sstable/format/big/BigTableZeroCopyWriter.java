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
package org.apache.cassandra.io.sstable.format.big;

import java.io.EOFException;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.EnumMap;
import java.util.Map;
import java.util.zip.CRC32;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.io.util.File;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.FileOutputStreamPlus;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.cassandra.net.AsyncStreamingInputPlus;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadataRef;

import static java.lang.String.format;
import static org.apache.cassandra.utils.FBUtilities.prettyPrintMemory;

public class BigTableZeroCopyWriter extends SSTable implements SSTableMultiWriter
{
    private static final Logger logger = LoggerFactory.getLogger(BigTableZeroCopyWriter.class);

    private final TableMetadataRef metadata;
    private volatile SSTableReader finalReader;
    private final Map<Component.Type, SequentialWriter> componentWriters;

    /**
     * CRC32 of every byte written to Data.db, kept only when the sender did not send a Digest.crc32 of its own.
     * <p>
     * A partial zero-copy stream cannot produce one: its Data.db is byte ranges of a larger file that reach the
     * socket by {@code sendfile} without ever entering the sending process, so a digest there would cost a second
     * full read of the data. Computing it here costs nothing -- the bytes are already in hand on their way to the
     * file -- and produces exactly the same value, because it is a checksum of the same bytes. Null when the
     * manifest named DIGEST, in which case the sender's file is authoritative and is written verbatim like any
     * other component.
     */
    private final CRC32 dataDigest;

    private static final SequentialWriterOption WRITER_OPTION =
        SequentialWriterOption.newBuilder()
                              .trickleFsync(false)
                              .bufferSize(2 << 20)
                              .bufferType(BufferType.OFF_HEAP)
                              .build();

    private static final ImmutableSet<Component> SUPPORTED_COMPONENTS =
        ImmutableSet.of(Component.DATA,
                        Component.PRIMARY_INDEX,
                        Component.SUMMARY,
                        Component.STATS,
                        Component.COMPRESSION_INFO,
                        Component.FILTER,
                        Component.DIGEST,
                        Component.CRC);

    public BigTableZeroCopyWriter(Descriptor descriptor,
                                  TableMetadataRef metadata,
                                  LifecycleNewTracker lifecycleNewTracker,
                                  final Collection<Component> components)
    {
        super(descriptor, ImmutableSet.copyOf(components), metadata, DatabaseDescriptor.getDiskOptimizationStrategy());

        lifecycleNewTracker.trackNew(this);
        this.metadata = metadata;
        this.componentWriters = new EnumMap<>(Component.Type.class);

        if (!SUPPORTED_COMPONENTS.containsAll(components))
            throw new AssertionError(format("Unsupported streaming component detected %s",
                                            Sets.difference(ImmutableSet.copyOf(components), SUPPORTED_COMPONENTS)));

        for (Component c : components)
            componentWriters.put(c.type, makeWriter(descriptor, c));

        this.dataDigest = components.contains(Component.DIGEST) ? null : new CRC32();
    }

    private static SequentialWriter makeWriter(Descriptor descriptor, Component component)
    {
        return new SequentialWriter(new File(descriptor.filenameFor(component)), WRITER_OPTION, false);
    }

    private void write(DataInputPlus in, long size, SequentialWriter out, CRC32 digest) throws FSWriteError
    {
        final int BUFFER_SIZE = 1 << 20;
        long bytesRead = 0;
        byte[] buff = new byte[BUFFER_SIZE];
        try
        {
            while (bytesRead < size)
            {
                int toRead = (int) Math.min(size - bytesRead, BUFFER_SIZE);
                in.readFully(buff, 0, toRead);
                int count = Math.min(toRead, BUFFER_SIZE);
                out.write(buff, 0, count);
                if (digest != null)
                    digest.update(buff, 0, count);
                bytesRead += count;
            }
            out.sync(); // finish will also call sync(). Leaving here to get stuff flushed as early as possible
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, out.getPath());
        }
    }

    @Override
    public boolean append(UnfilteredRowIterator partition)
    {
        throw new UnsupportedOperationException("Operation not supported by BigTableBlockWriter");
    }

    @Override
    public Collection<SSTableReader> finish(long repairedAt, long maxDataAge, boolean openResult)
    {
        return finish(openResult);
    }

    @Override
    public Collection<SSTableReader> finish(boolean openResult)
    {
        setOpenResult(openResult);

        for (SequentialWriter writer : componentWriters.values())
            writer.finish();

        return finished();
    }

    @Override
    public Collection<SSTableReader> finished()
    {
        if (finalReader == null)
        {
            // The sender could not produce a Digest.crc32 for what it sent -- a partial zero-copy stream never has
            // the bytes in process -- so this is where the component comes from. It is a CRC of exactly the file
            // just written, which is what Verifier compares against, and writing it here rather than leaving it
            // out is what keeps `nodetool verify` on a received sstable a whole-file CRC instead of an extended
            // row-by-row verification.
            if (dataDigest != null && components.contains(Component.DATA))
                writeDigest();

            // Wire format omits TOC.txt; write it locally so snapshot createLinks
            // hardlinks it and the on-disk layout matches flush/compaction output.
            components.add(Component.TOC);
            appendTOC(descriptor, components);

            finalReader = SSTableReader.open(descriptor, components, metadata);
        }

        return ImmutableList.of(finalReader);
    }

    @Override
    public SSTableMultiWriter setOpenResult(boolean openResult)
    {
        return null;
    }

    @Override
    public long getFilePointer()
    {
        return 0;
    }

    @Override
    public long getOnDiskBytesWritten()
    {
        return 0;
    }

    @Override
    public TableId getTableId()
    {
        return metadata.id;
    }

    @Override
    public Throwable commit(Throwable accumulate)
    {
        for (SequentialWriter writer : componentWriters.values())
            accumulate = writer.commit(accumulate);
        return accumulate;
    }

    @Override
    public Throwable abort(Throwable accumulate)
    {
        for (SequentialWriter writer : componentWriters.values())
            accumulate = writer.abort(accumulate);
        return accumulate;
    }

    @Override
    public void prepareToCommit()
    {
        for (SequentialWriter writer : componentWriters.values())
            writer.prepareToCommit();
    }

    @Override
    public void close()
    {
        for (SequentialWriter writer : componentWriters.values())
            writer.close();
    }

    /**
     * Digest.crc32 the way {@code ChecksumWriter.writeFullChecksum} writes it: the plain decimal ASCII of the
     * CRC32, no newline and no prefix, fsynced. Failure to write it is logged rather than thrown: the sstable is
     * complete and correct without the component, and the only consequence is a slower verification.
     */
    private void writeDigest()
    {
        File file = new File(descriptor.filenameFor(Component.DIGEST));
        try (FileOutputStreamPlus out = new FileOutputStreamPlus(file))
        {
            out.write(String.valueOf(dataDigest.getValue()).getBytes(StandardCharsets.UTF_8));
            out.flush();
            out.sync();
            components.add(Component.DIGEST);
        }
        catch (IOException e)
        {
            logger.warn("Failed writing {} for a received sstable; it will verify by extended verification instead",
                        file, e);
            file.deleteIfExists();
        }
    }

    public void writeComponent(Component.Type type, DataInputPlus in, long size) throws ClosedChannelException
    {
        logger.info("Writing component {} to {} length {}", type, componentWriters.get(type).getPath(), prettyPrintMemory(size));

        // Only Data.db is digested, and only when the sender sent no digest of its own.
        CRC32 digest = type == Component.Type.DATA ? dataDigest : null;

        if (in instanceof AsyncStreamingInputPlus)
            write((AsyncStreamingInputPlus) in, size, componentWriters.get(type), digest);
        else
            write(in, size, componentWriters.get(type), digest);
    }

    private void write(AsyncStreamingInputPlus in, long size, SequentialWriter writer, CRC32 digest) throws ClosedChannelException
    {
        logger.info("Block Writing component to {} length {}", writer.getPath(), prettyPrintMemory(size));

        try
        {
            in.consume(buffer -> {
                // duplicate(): update() consumes the buffer it is given, and the channel still needs it.
                if (digest != null)
                    digest.update(buffer.duplicate());
                return writer.writeDirectlyToChannel(buffer);
            }, size);
            writer.sync();
        }
        catch (EOFException e)
        {
            in.close();
        }
        catch (ClosedChannelException e)
        {
            // FSWriteError triggers disk failure policy, but if we get a connection issue we do not want to do that
            // so rethrow so the error handling logic higher up is able to deal with this
            // see CASSANDRA-17116
            throw e;
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, writer.getPath());
        }
    }
}
