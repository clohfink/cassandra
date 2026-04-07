package com.netflix.cassandra.jvmdtest.bootstrap;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.gradle.api.file.FileCollection;

/**
 * Extension providing jvm-dtest configuration values. Consumers set {@link #shadedClasspath} and
 * reference the other properties in their test task configuration.
 *
 * <pre>{@code
 * cassandraJvmDtest {
 *     shadedClasspath = configurations.dtestShaded
 * }
 *
 * tasks.named('integTest', Test).configure {
 *     classpath = classpath + cassandraJvmDtest.shadedClasspath
 *     jvmArgs cassandraJvmDtest.jvmArgs
 *     systemProperties cassandraJvmDtest.systemProperties
 *     maxHeapSize = cassandraJvmDtest.maxHeapSize
 * }
 * }</pre>
 */
public class CassandraJvmDtestExtension {

    private FileCollection shadedClasspath;

    /** JVM flags required by Cassandra on JDK 21 (from nfcassandra conf/jvm21-server.options). */
    private static final List<String> JVM_ARGS = Collections.unmodifiableList(Arrays.asList(
        "--add-exports=java.base/jdk.internal.misc=ALL-UNNAMED",
        "--add-exports=java.base/jdk.internal.ref=ALL-UNNAMED",
        "--add-exports=java.rmi/sun.rmi.registry=ALL-UNNAMED",
        "--add-exports=java.rmi/sun.rmi.server=ALL-UNNAMED",
        "--add-exports=jdk.unsupported/sun.misc=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.io=ALL-UNNAMED",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.net=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.module=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.ref=ALL-UNNAMED",
        "--add-opens=java.base/jdk.internal.loader=ALL-UNNAMED",
        "--add-opens=java.base/jdk.internal.reflect=ALL-UNNAMED",
        "--add-opens=java.base/jdk.internal.math=ALL-UNNAMED",
        "--add-opens=java.base/jdk.internal.module=ALL-UNNAMED",
        "--add-opens=java.base/jdk.internal.vm=ALL-UNNAMED",
        "--add-opens=java.base/java.nio.charset=ALL-UNNAMED",
        "--add-opens=java.base/java.security=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent.locks=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.fs=ALL-UNNAMED",
        "--add-opens=java.base/sun.security.x509=ALL-UNNAMED",
        "--add-opens=java.base/sun.security.util=ALL-UNNAMED",
        "--add-opens=java.base/com.sun.crypto.provider=ALL-UNNAMED",
        "--add-opens=jdk.management/com.sun.management.internal=ALL-UNNAMED",
        "-Dio.netty.tryReflectionSetAccessible=true",
        "-Djdk.reflect.useDirectMethodHandle=false",
        // Force Spring Boot to use Log4j2 instead of auto-detecting logback.
        // The dtest fat JAR bundles logback 1.2.9, which is missing StatusPrinter2
        // (added in 1.3+). Without this, Spring Boot sees logback on the classpath
        // and tries to use LogbackLoggingSystem, which fails.
        "-Dorg.springframework.boot.logging.LoggingSystem=org.springframework.boot.logging.log4j2.Log4J2LoggingSystem"
    ));

    /**
     * Set this to the configuration containing the fat JAR. The plugin uses it for
     * {@link #getSystemProperties()} and consumers append it to their test classpath.
     */
    public void setShadedClasspath(FileCollection shadedClasspath) {
        this.shadedClasspath = shadedClasspath;
    }

    /** The fat JAR file collection, for appending last to the test task classpath. */
    public FileCollection getShadedClasspath() {
        if (shadedClasspath == null) {
            throw new IllegalStateException(
                "cassandraJvmDtest.shadedClasspath not set. "
                    + "Set it in your build.gradle: cassandraJvmDtest { shadedClasspath = configurations.dtestShaded }");
        }
        return shadedClasspath;
    }

    /** JVM args required by Cassandra on JDK 21. */
    public List<String> getJvmArgs() {
        return JVM_ARGS;
    }

    /**
     * System properties for dtest bootstrap classpath and logging configuration.
     * Requires {@link #shadedClasspath} to be set.
     */
    public Map<String, String> getSystemProperties() {
        Map<String, String> props = new LinkedHashMap<>();
        props.put("dtest.bootstrap.classpath", getShadedClasspath().getAsPath());
        return Collections.unmodifiableMap(props);
    }

    /** Recommended minimum heap size for running jvm-dtest clusters. */
    public String getMaxHeapSize() {
        return "4g";
    }
}
