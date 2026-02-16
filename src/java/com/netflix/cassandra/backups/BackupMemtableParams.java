package com.netflix.cassandra.backups;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.MemtableParams;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.FBUtilities;
import software.amazon.awssdk.regions.Region;

/**
 * Implementation of MemtableParams that provides S3-based memtable functionality.
 * This class handles the configuration and creation of memtables that read from backups
 * It supports configuration through environment variables and parameter strings.
 */
public class BackupMemtableParams extends MemtableParams implements Memtable.Factory
{
    private static final Logger logger = LoggerFactory.getLogger(BackupMemtableParams.class);

    public static final String NETFLIX_REGION = BackupUtils.NETFLIX_REGION;
    public static final String NETFLIX_APP = BackupUtils.NETFLIX_APP;
    public static final String NETFLIX_ENVIRONMENT = BackupUtils.NETFLIX_ENVIRONMENT;
    public static final String BACKUP_MEMTABLE_ACCESS_SETTINGS = "BACKUP_MEMTABLE_ACCESS_SETTINGS";

    public static final String BUCKET = "bucket";
    public static final String PREFIX = "prefix";
    public static final String TOKEN = "token";
    public static final String TIMESTAMP = "timestamp";
    public static final String KEYSPACE = "keyspace";
    public static final String TABLE = "table";

    /** Set of allowed configuration parameters */
    private static final Set<String> ALLOWED_PARAMS = Sets.newHashSet(
        BUCKET,
        PREFIX,
        TOKEN,
        TIMESTAMP,
        KEYSPACE,
        TABLE,
        // overridable env variables for tests
        NETFLIX_APP,
        NETFLIX_REGION,
        NETFLIX_ENVIRONMENT,
        // optional S3 settings, available through env variables
        BACKUP_MEMTABLE_ACCESS_SETTINGS
    );

    /** The Cassandra keyspace name */
    private final String keyspace;

    /** The Cassandra table name */
    private final String table;

    /** Timestamp for the memtable data */
    private final long timestamp;

    private final ObjectStoreAccess s3;

    /** Environment variables used for configuration */
    private final Map<String, String> envVars;

    /** Configuration for S3 API retries and timeouts. */
    private final ObjectStoreConfiguration objectStoreConfiguration;

    /** Backup context containing bucket, prefix, and token */
    private final BackupContext backupContext;

    /**
     * Creates a new BackupMemtableParams instance using the system environment variables.
     *
     * @param configurationKey The configuration key string in the format "backup:param1=value1,param2=value2"
     */
    public BackupMemtableParams(String configurationKey)
    {
        this(configurationKey, System.getenv());
    }

    /**
     * Creates a new BackupMemtableParams instance with specified environment variables.
     *
     * @param configurationKey The configuration key string in the format "backup:param1=value1,param2=value2"
     * @param envVars Map of environment variables to use for configuration
     */
    public BackupMemtableParams(String configurationKey, Map<String, String> envVars)
    {
        super(null, configurationKey);
        Map<String, String> mergedEnvVars = new HashMap<>(envVars);
        Map<String, String> params = parseParams(configurationKey);
        mergedEnvVars.putAll(params);
        this.envVars = mergedEnvVars;

        logger.debug("Parsed BackupMemtableParams: {}", params);

        // Build BackupContext from config params with env vars as fallback
        this.backupContext            = buildBackupContext(params, envVars);
        this.keyspace                 = params.get(KEYSPACE);   // may be null
        this.table                    = params.get(TABLE);      // may be null
        this.objectStoreConfiguration = deriveS3AsyncConfiguration(this.envVars.get(BACKUP_MEMTABLE_ACCESS_SETTINGS));
        this.s3                       = ObjectStoreAccess.get(Region.of(this.envVars.get(NETFLIX_REGION)), objectStoreConfiguration);
        this.timestamp                = deriveTimestamp(params);
    }

    /**
     * Builds a BackupContext from configuration parameters with environment variable fallbacks.
     */
    static BackupContext buildBackupContext(Map<String, String> configParams, Map<String, String> envVars)
    {
        String env = configParams.containsKey(NETFLIX_ENVIRONMENT) ? configParams.get(NETFLIX_ENVIRONMENT) : envVars.getOrDefault(NETFLIX_ENVIRONMENT, "test");
        String region = configParams.containsKey(NETFLIX_REGION) ? configParams.get(NETFLIX_REGION) : envVars.get(NETFLIX_REGION);
        String app = configParams.containsKey(NETFLIX_APP) ? configParams.get(NETFLIX_APP) : envVars.get(NETFLIX_APP);
        String token = configParams.containsKey(TOKEN) ? configParams.get(TOKEN) : BackupUtils.getToken();

        if (region == null || app == null)
        {
            logger.error("Missing required environment variables: NETFLIX_REGION={}, NETFLIX_APP={}", region, app);
            throw new IllegalArgumentException("Missing NETFLIX_REGION or NETFLIX_APP");
        }

        // Pass bucket/prefix overrides if explicitly configured
        String bucketOverride = configParams.get(BUCKET);
        String prefixOverride = configParams.get(PREFIX);

        return new BackupContext(env, region, app, token, bucketOverride, prefixOverride);
    }

    /**
     * Creates a new BackupMemtable instance with the specified parameters.
     *
     * @param clLowerBound The commit log lower bound position
     * @param metadataRef Reference to the table metadata
     * @param owner The owner of the memtable
     * @return A new BackupMemtable instance
     */
    @Override
    public Memtable create(AtomicReference<CommitLogPosition> clLowerBound,
                           TableMetadataRef metadataRef,
                           Memtable.Owner owner)
    {
        return new BackupMemtable(metadataRef, this);
    }

    /**
     * Parses the configuration string into a map of parameters.
     *
     * @param config The configuration string in the format "backup:param1=value1,param2=value2"
     * @return Map of parameter names to values
     * @throws IllegalArgumentException if the configuration string is invalid
     */
    Map<String,String> parseParams(String config)
    {
        String[] parts = config.split(":", 2);
        if (!"backupmemtable".equals(parts[0]) && !"backup".equals(parts[0]))
            throw new IllegalArgumentException("Config must start with 'backup:' or 'backupmemtable:'");

        Map<String,String> m = new HashMap<>();
        if (parts.length == 2 && !parts[1].trim().isEmpty())
        {
            for (String kv : parts[1].split(","))
            {
                String[] pair = kv.trim().split("=", 2);
                if (pair.length == 2 && ! pair[0].isEmpty())
                {
                    String param = pair[0].trim();
                    if (!ALLOWED_PARAMS.contains(param))
                    {
                        throw new IllegalArgumentException(
                            String.format("Unknown parameter '%s'. Allowed parameters are: %s",
                                        param, String.join(", ", ALLOWED_PARAMS)));
                    }
                    m.put(param, pair[1].trim());
                }
            }
        }
        return m;
    }

    public Memtable.Factory factory()
    {
        return this;
    }

    @Override
    public void validate()
    {
        if (!backupContext.isValid())
            throw new ConfigurationException(String.format(
                "Invalid backup memtable configuration: env='%s', region='%s', app='%s', token='%s'. " +
                "All fields must be non-blank, region must be a valid AWS region, and environment must be 'test' or 'prod'.",
                backupContext.env(), backupContext.region(), backupContext.app(), backupContext.token()));

        // Verify the backup prefix path exists in the bucket (cluster check)
        List<String> prefixKeys;
        try
        {
            prefixKeys = s3.getObjectKeys(backupContext.bucket(), backupContext.prefix()).get();
        }
        catch (Exception e)
        {
            throw new ConfigurationException(String.format(
                "Failed to access backup data at bucket '%s' prefix '%s': %s",
                backupContext.bucket(), backupContext.prefix(), e.getMessage()), e);
        }

        if (prefixKeys.isEmpty())
            throw new ConfigurationException(String.format(
                "No backup data found at bucket '%s' prefix '%s'. " +
                "Verify the cluster exists and has backups.",
                backupContext.bucket(), backupContext.prefix()));

        // Verify backup manifests exist for this node's token
        List<String> metaKeys;
        try
        {
            metaKeys = s3.getObjectKeys(backupContext.bucket(), backupContext.metafilePrefix()).get();
        }
        catch (Exception e)
        {
            throw new ConfigurationException(String.format(
                "Failed to list backup manifests for token '%s': %s",
                backupContext.token(), e.getMessage()), e);
        }

        if (metaKeys.isEmpty())
            throw new ConfigurationException(String.format(
                "No backup manifests found for token '%s' at '%s'. " +
                "Verify backups exist for this token.",
                backupContext.token(), backupContext.metafilePrefix()));
    }

    /**
     * Derives the timestamp from parameters or uses the current time.
     *
     * @param configParams Map of configuration parameters
     * @return The timestamp in milliseconds
     * @throws IllegalArgumentException if the timestamp parameter is invalid
     */
    static long deriveTimestamp(Map<String,String> configParams)
    {
        if (configParams.containsKey(TIMESTAMP))
        {
            try
            {
                return Long.parseLong(configParams.get(TIMESTAMP));
            }
            catch (NumberFormatException e)
            {
                throw new IllegalArgumentException(
                "Invalid timestamp", e
                );
            }
        }
        return FBUtilities.now().toEpochMilli();
    }

    /**
     * Gets the effective keyspace name, using the configured value or falling back to the metadata.
     *
     * @param meta Reference to the table metadata (used as fallback if keyspace not configured)
     * @return The keyspace name
     */
    String getEffectiveKeyspace(TableMetadataRef meta)
    {
        if (keyspace != null)
            return keyspace;
        if (meta == null)
            throw new IllegalStateException("Keyspace must be specified when not using table metadata");
        return meta.get().keyspace;
    }

    /**
     * Gets the effective table name, using the configured value or falling back to the metadata.
     *
     * @param meta Reference to the table metadata (used as fallback if table not configured)
     * @return The table name
     */
    String getEffectiveTable(TableMetadataRef meta)
    {
        if (table != null)
            return table;
        if (meta == null)
            throw new IllegalStateException("Table must be specified when not using table metadata");
        return meta.get().name;
    }

    ObjectStoreConfiguration deriveS3AsyncConfiguration(String s3Settings)
    {
        if (s3Settings != null && !s3Settings.isEmpty())
        {
            try
            {
                return ObjectStoreConfiguration.fromKeyValueString(s3Settings);
            }
            catch (Exception e)
            {
                logger.error("Failed to parse BACKUP_MEMTABLE_ACCESS_SETTINGS: {}", s3Settings, e);
                throw new IllegalArgumentException("Invalid BACKUP_MEMTABLE_ACCESS_SETTINGS", e);
            }
        }
        return ObjectStoreConfiguration.defaultRetryConfig;
    }

    /**
     * Loads the backup manifest from a file.
     *
     * @param metaFile The file containing the backup manifest
     * @return The loaded BackupManifest
     * @throws RuntimeException if the manifest cannot be loaded
     */
    BackupManifest loadManifest(File metaFile)
    {
        try (InputStream in = metaFile.newInputStream())
        {
            return BackupUtils.getManifest(in);
        }
        catch (Exception e)
        {
            logger.error("Failed to load backup manifest from {}", metaFile, e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Finds the table data in the backup manifest.
     *
     * @param m The backup manifest
     * @param ks The keyspace name
     * @param tbl The table name (may include UUID suffix which will be stripped)
     * @return The matching BackupManifest.Data object
     * @throws RuntimeException if no matching table is found in the manifest
     */
    BackupManifest.Data findTableData(BackupManifest m,
                                              String ks,
                                              String tbl)
    {
        // Strip UUID suffix from table name if present (format: tablename-uuid)
        String tblBase = tbl.split("-")[0];
        for (BackupManifest.Data d : m.getData())
        {
            // Also strip UUID from manifest's columnfamily name for comparison
            String cfBase = d.getColumnfamilyName().split("-")[0];
            if (d.getKeyspaceName().equals(ks) && cfBase.equals(tblBase))
                return d;
        }
        throw new RuntimeException("No matching table '" + ks + "." + tblBase + "' in manifest");
    }

    public ObjectStoreAccess getS3()
    {
        return s3;
    }

    public String getToken()
    {
        return backupContext.token();
    }

    public long getTimestamp()
    {
        return timestamp;
    }

    public String getTable()
    {
        return table;
    }

    public String getKeyspace()
    {
        return keyspace;
    }

    public String getBucket()
    {
        return backupContext.bucket();
    }

    public String getPrefix()
    {
        return backupContext.prefix();
    }

    public String getMetafilePrefix()
    {
        return backupContext.metafilePrefix();
    }

    public ObjectStoreConfiguration getAsyncS3AccessConfiguration()
    {
        return objectStoreConfiguration;
    }
}
