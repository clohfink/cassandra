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
package org.apache.cassandra.tools;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.KeyIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.ValidationMetadata;
import org.apache.cassandra.schema.Functions;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.schema.Types;
import org.apache.cassandra.schema.Views;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import com.google.common.util.concurrent.RateLimiter;

/**
 * Export SSTables to JSON format.
 */
public class SSTableExport
{

    private static final String KEY_OPTION = "k";
    private static final String DEBUG_OUTPUT_OPTION = "d";
    private static final String EXCLUDE_KEY_OPTION = "x";
    private static final String ENUMERATE_KEYS_OPTION = "e";

    private static final Options options = new Options();
    private static CommandLine cmd;

    static
    {
        Config.setClientMode(true);

        Option optKey = new Option(KEY_OPTION, true, "Row key");
        // Number of times -k <key> can be passed on the command line.
        optKey.setArgs(500);
        options.addOption(optKey);

        Option excludeKey = new Option(EXCLUDE_KEY_OPTION, true, "Excluded row key");
        // Number of times -x <key> can be passed on the command line.
        excludeKey.setArgs(500);
        options.addOption(excludeKey);

        Option optEnumerate = new Option(ENUMERATE_KEYS_OPTION, false, "enumerate keys only");
        options.addOption(optEnumerate);

        Option debugOutput = new Option(DEBUG_OUTPUT_OPTION, false, "CQL row per line internal representation");
        options.addOption(debugOutput);
    }

    public static CFMetaData metadataFromSSTable(Descriptor desc)
    {
        try
        {
            EnumSet<MetadataType> types = EnumSet.of(MetadataType.VALIDATION, MetadataType.STATS, MetadataType.HEADER);
            Map<MetadataType, MetadataComponent> sstableMetadata = desc.getMetadataSerializer().deserialize(desc,
                    types);
            ValidationMetadata validationMetadata = (ValidationMetadata) sstableMetadata.get(MetadataType.VALIDATION);
            SerializationHeader.Component header = (SerializationHeader.Component) sstableMetadata
                    .get(MetadataType.HEADER);

            IPartitioner partitioner = FBUtilities.newPartitioner(validationMetadata.partitioner);
            CFMetaData.Builder builder = CFMetaData.Builder.create("keyspace", "table").withPartitioner(partitioner);
            header.getStaticColumns().entrySet().stream()
                    .forEach(entry -> builder.addStaticColumn(UTF8Type.instance.getString(entry.getKey()),
                            entry.getValue()));
            header.getRegularColumns().entrySet().stream()
                    .forEach(entry -> builder.addRegularColumn(UTF8Type.instance.getString(entry.getKey()),
                            entry.getValue()));
            builder.addPartitionKey("PartitionKey", header.getKetType());
            for (int i = 0; i < header.getClusteringTypes().size(); i++)
            {
                builder.addClusteringColumn("clustering" + (i > 0 ? i : ""), header.getClusteringTypes().get(i));
            }
            return builder.build();
        }
        catch (IOException e)
        {
            System.err.println("Unable to build CFMetaData" + e.getMessage());
            e.printStackTrace();
            System.exit(1);
            return null;
        }
    }

    private static <T> Stream<T> iterToStream(Iterator<T> iter)
    {
        Spliterator<T> splititer = Spliterators.spliteratorUnknownSize(iter, Spliterator.IMMUTABLE);
        return StreamSupport.stream(splititer, false);
    }

    /**
     * Given arguments specifying an SSTable, and optionally an output file, export the contents of the SSTable to JSON.
     *
     * @param args
     *            command lines arguments
     * @throws ConfigurationException
     *             on configuration failure (wrong params given)
     */
    public static void main(String[] args) throws ConfigurationException
    {
        String usage = String.format("Usage: %s <sstable> [-k key [-k key [...]] -x key [-x key [...]]]%n",
                SSTableExport.class.getName());

        CommandLineParser parser = new PosixParser();
        try
        {
            cmd = parser.parse(options, args);
        }
        catch (ParseException e1)
        {
            System.err.println(e1.getMessage());
            System.err.println(usage);
            System.exit(1);
        }

        if (cmd.getArgs().length != 1)
        {
            System.err.println("You must supply exactly one sstable");
            System.err.println(usage);
            System.exit(1);
        }

        String[] keys = cmd.getOptionValues(KEY_OPTION);
        HashSet<String> excludes = new HashSet<String>(Arrays.asList(
                cmd.getOptionValues(EXCLUDE_KEY_OPTION) == null
                        ? new String[0]
                        : cmd.getOptionValues(EXCLUDE_KEY_OPTION)));
        String ssTableFileName = new File(cmd.getArgs()[0]).getAbsolutePath();

        if (Descriptor.isLegacyFile(new File(ssTableFileName)))
        {
            System.err.println("Unsupported legacy sstable");
            System.exit(1);
        }
        if (!new File(ssTableFileName).exists())
        {
            System.err.println("Cannot find file " + ssTableFileName);
            System.exit(1);
        }
        Descriptor desc = Descriptor.fromFilename(ssTableFileName);
        CFMetaData metadata = metadataFromSSTable(desc);
        try
        {
            if (cmd.hasOption(ENUMERATE_KEYS_OPTION))
            {
                JsonTransformer.keysToJson(null, iterToStream(new KeyIterator(desc, metadata)), metadata, System.out);
            }
            else
            {
                SSTableReader sstable = SSTableReader.openNoValidation(desc, metadata);
                IPartitioner partitioner = sstable.getPartitioner();
                final ISSTableScanner currentScanner;
                if ((keys != null) && (keys.length > 0))
                {
                    Stream<Token> tokens = Arrays.stream(keys)
                            .filter(key -> !excludes.contains(key))
                            .map(metadata.getKeyValidator()::fromString)
                            .map(partitioner::decorateKey)
                            .sorted()
                            .map(DecoratedKey::getToken);
                    List<Range<Token>> ts = tokens.map(t -> new Range<Token>(t.decreaseSlightly(), t)).collect(Collectors.toList());
                    currentScanner = sstable.getScanner(ts, RateLimiter.create(Double.MAX_VALUE));
                }
                else
                {
                    currentScanner = sstable.getScanner();
                }
                Stream<UnfilteredRowIterator> partitions = iterToStream(currentScanner).filter(i ->
                {
                    return excludes == null ||
                            excludes.isEmpty() ||
                            !excludes.contains(metadata.getKeyValidator().getString(i.partitionKey().getKey()));
                });
                if (cmd.hasOption(DEBUG_OUTPUT_OPTION))
                {
                    AtomicLong position = new AtomicLong();
                    partitions.forEach(partition -> 
                    {
                        position.set(currentScanner.getCurrentPosition());
                        partition.forEachRemaining(row -> 
                        {
                            System.out.println(
                                    "[" + metadata.getKeyValidator().getString(partition.partitionKey().getKey()) + "]@"
                                            + position.get() + " " + row.toString(metadata, false, true));
                            position.set(currentScanner.getCurrentPosition());
                        });
                    });
                }
                else
                {
                    JsonTransformer.toJson(currentScanner, partitions, metadata, System.out);
                }
            }
        }
        catch (IOException e)
        {
            // throwing exception outside main with broken pipe causes windows cmd to hang
            e.printStackTrace(System.err);
        }

        System.exit(0);
    }
}