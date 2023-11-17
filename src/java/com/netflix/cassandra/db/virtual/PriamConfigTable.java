package com.netflix.cassandra.db.virtual;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.db.virtual.AbstractVirtualTable;
import org.apache.cassandra.db.virtual.SimpleDataSet;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.schema.TableMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class PriamConfigTable extends AbstractVirtualTable
{
    private static final Logger logger = LoggerFactory.getLogger(PriamConfigTable.class);
    private static final ObjectMapper jsonMapper = new ObjectMapper(new JsonFactory());

    private static final String NAME = "name";
    private static final String VALUE = "value";
    private static final String CONFIG_URL = "http://localhost:8080/Priam/REST/v1/config/structured/all";

    public static final String TABLE_NAME = "priam_settings";

    PriamConfigTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, TABLE_NAME)
                .comment("current settings")
                .kind(TableMetadata.Kind.VIRTUAL)
                .partitioner(new LocalPartitioner(UTF8Type.instance))
                .addPartitionKeyColumn(NAME, UTF8Type.instance)
                .addRegularColumn(VALUE, UTF8Type.instance)
                .build());
    }

    private static String sendGET(String url) throws Exception
    {
        HttpURLConnection con = (HttpURLConnection) new URL(url).openConnection();
        con.setReadTimeout(10000);
        con.setRequestMethod("GET");
        if (con.getResponseCode() != HttpURLConnection.HTTP_OK)
            throw new RuntimeException(String.format("Failed fetching %s", url));
        try (BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream())))
        {
            return in.lines().collect(Collectors.joining("\n"));
        }
    }

    @Override
    public AbstractVirtualTable.DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());
        try
        {
            String config = sendGET(CONFIG_URL);
            ((Set<Map.Entry<String,?>>) jsonMapper.readValue(config, Map.class).entrySet())
                    .forEach(entry -> result.row(entry.getKey()).column(VALUE, entry.getValue().toString()));
        }
        catch (Exception e)
        {
            logger.error("Failure to fetch priam structured configs, if running locally you can proxy to a priam instance" +
                    "\n ie : 'socat tcp-l:8080,fork,reuseaddr tcp:$CASSANDRA_HOST_IP:8080'", e);
            result.row("ERROR").column(VALUE, e.getMessage());
        }
        return result;
    }
}

