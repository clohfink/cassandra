package com.netflix.cassandra.jvmdtest.bootstrap;

import org.gradle.api.Plugin;
import org.gradle.api.Project;

/**
 * Gradle plugin that provides classpath isolation configuration for jvm-dtest.
 *
 * <p>Creates a {@code cassandraJvmDtest} extension with pre-configured JVM args, system
 * properties, and heap size. The consumer creates their own configuration for the fat JAR
 * and passes it to the extension. The plugin does not create any configurations itself,
 * avoiding conflicts with SBN/Nebula configuration locking.
 *
 * <p>Usage:
 *
 * <pre>{@code
 * plugins {
 *     id 'com.netflix.cde.cassandra-jvm-dtest-bootstrap' version '4.1.8.915'
 * }
 *
 * configurations { dtestShaded }
 * dependencies {
 *     integTestImplementation 'com.netflix.cde:nfcassandra-jvm-dtest-bootstrap:4.1.8.915'
 *     integTestCompileOnly 'com.netflix.cde:nfcassandra-jvm-dtest:4.1.8.915'
 *     dtestShaded 'com.netflix.cde:nfcassandra-jvm-dtest:4.1.8.915'
 * }
 *
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
public class CassandraJvmDtestPlugin implements Plugin<Project> {

    @Override
    public void apply(Project project) {
        project.getExtensions().create("cassandraJvmDtest", CassandraJvmDtestExtension.class);
    }
}
