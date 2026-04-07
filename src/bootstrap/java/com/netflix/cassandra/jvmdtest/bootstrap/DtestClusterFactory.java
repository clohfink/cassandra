package com.netflix.cassandra.jvmdtest.bootstrap;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.AbstractBuilder;

/**
 * Manages classpath isolation for the jvm-dtest fat JAR. The fat JAR bundles dependencies
 * (snakeyaml, jackson, guava, logback, reflections, etc.) that conflict with app versions on the
 * main test classpath.
 *
 * <p>Two isolation mechanisms work together:
 *
 * <ol>
 *   <li><b>Bootstrap thread</b> with a child-first classloader runs cluster lifecycle operations
 *       ({@code Cluster.build()}, {@code startup()}, {@code close()}) so the fat JAR's dependency
 *       versions are used for Cassandra's internal initialization (which touches reflections,
 *       snakeyaml, etc.).
 *   <li><b>{@code cassandra.dtest.classpath}</b> system property tells the per-instance {@link
 *       org.apache.cassandra.distributed.shared.InstanceClassLoader}s to use a classpath with the
 *       fat JAR first, so each Cassandra node loads the correct dependency versions.
 * </ol>
 *
 * <p>The main test thread has the fat JAR last on its classpath, so app deps take priority but
 * Cassandra types (ReadCommand, etc.) are still resolvable for imports and lambda authoring. Lambdas
 * passed to {@code runOnInstance}/{@code callOnInstance} are serialized and transferred to the
 * instance classloader as designed by the jvm-dtest framework.
 */
public final class DtestClusterFactory {

  private static ExecutorService bootstrapExecutor;

  /**
   * Sets up classpath isolation, creates the bootstrap thread, and calls {@link ICluster#setup()}.
   * Must be called once before any cluster creation.
   */
  public static void init() throws Exception {
    ClassLoader mainCl = Thread.currentThread().getContextClassLoader();
    System.out.println(
        "DtestClusterFactory: main thread classloader: "
            + mainCl.getClass().getName()
            + " classpath: "
            + (mainCl instanceof URLClassLoader
                ? Arrays.toString(((URLClassLoader) mainCl).getURLs())
                : System.getProperty("java.class.path")));

    String cp = System.getProperty("dtest.bootstrap.classpath");
    if (cp == null || cp.isEmpty()) {
      throw new IllegalStateException(
          "dtest.bootstrap.classpath system property not set. "
              + "Ensure the test task sets: systemProperties cassandraJvmDtest.systemProperties");
    }

    URL[] urls =
        Arrays.stream(cp.split(File.pathSeparator))
            .map(
                p -> {
                  try {
                    return new File(p).toURI().toURL();
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  }
                })
            .toArray(URL[]::new);

    // Set cassandra.dtest.classpath with the fat JAR first. The overridden Versions.getClassPath()
    // in the fat JAR reads this instead of java.class.path, so InstanceClassLoaders (child-first)
    // load the fat JAR's dependency versions. We don't modify java.class.path to avoid leaking
    // into other test threads (Spring Boot, etc.).
    String dtestClasspath = cp + File.pathSeparator + System.getProperty("java.class.path");
    System.setProperty("cassandra.dtest.classpath", dtestClasspath);

    // Child-first classloader for the bootstrap thread. Cluster.build() and startup() touch
    // version-sensitive libraries (reflections, snakeyaml) that must come from the fat JAR,
    // not the app's classpath.
    ClassLoader parent = Thread.currentThread().getContextClassLoader();
    ClassLoader bootstrapCl =
        new URLClassLoader(urls, parent) {
          @Override
          protected Class<?> loadClass(String name, boolean resolve)
              throws ClassNotFoundException {
            synchronized (getClassLoadingLock(name)) {
              Class<?> c = findLoadedClass(name);
              if (c != null) {
                return c;
              }
              // Share dtest-api types and JDK classes with parent. Load the Versions class from
              // the child (fat JAR) so our cassandra.dtest.classpath override is used. Everything
              // else loads child-first so the fat JAR's dependency versions are used.
              if (!name.equals("org.apache.cassandra.distributed.shared.Versions")
                  && org.apache.cassandra.distributed.shared.InstanceClassLoader
                      .getDefaultLoadSharedFilter()
                      .test(name)) {
                return parent.loadClass(name);
              }
              try {
                c = findClass(name);
              } catch (ClassNotFoundException e) {
                c = parent.loadClass(name);
              }
              if (resolve) {
                resolveClass(c);
              }
              return c;
            }
          }
        };

    bootstrapExecutor =
        Executors.newSingleThreadExecutor(
            r -> {
              Thread t = new Thread(r, "dtest-bootstrap");
              t.setContextClassLoader(bootstrapCl);
              return t;
            });

    bootstrapExecutor
        .submit(
            () -> {
              try {
                ICluster.setup();
              } catch (Throwable t) {
                throw new RuntimeException("ICluster.setup() failed", t);
              }
              URLClassLoader bsCl =
                  (URLClassLoader) Thread.currentThread().getContextClassLoader();
              System.out.println(
                  "DtestClusterFactory: bootstrap thread classloader: "
                      + bsCl.getClass().getName()
                      + " classpath: "
                      + Arrays.toString(bsCl.getURLs())
                      + " parent: "
                      + bsCl.getParent().getClass().getName());
              return null;
            })
        .get();
  }

  /**
   * Builds, configures, and starts a cluster on the bootstrap thread. The configurator receives an
   * {@link AbstractBuilder} (from dtest-api, shared between classloaders) so callers can use {@code
   * withNodeIdTopology}, {@code withConfig}, {@code withInstanceInitializer}, etc.
   *
   * <p>{@code Cluster} is loaded reflectively from the bootstrap thread's context classloader
   * (child-first) rather than referencing it directly. Although the fat JAR is on the main classpath
   * (last), loading {@code Cluster} from the app classloader would cause its static initializer to
   * pull in the app's versions of reflections, snakeyaml, etc. — which are incompatible with what
   * Cassandra expects. Loading it from the child-first classloader ensures the fat JAR's bundled
   * versions are used.
   *
   * <p>We cannot use {@code IsolatedExecutor.transferAdhoc} here because it validates that the
   * lambda originates from the target classloader (designed for shared→isolated transfers), and the
   * configurator captures non-serializable test instance state.
   */
  @SuppressWarnings("unchecked")
  public static ICluster<IInvokableInstance> start(
      int nodeCount, Consumer<AbstractBuilder<IInvokableInstance, ?, ?>> configurator)
      throws Exception {
    return bootstrapExecutor
        .submit(
            () -> {
              Class<?> clusterClass =
                  Thread.currentThread()
                      .getContextClassLoader()
                      .loadClass("org.apache.cassandra.distributed.Cluster");
              AbstractBuilder<IInvokableInstance, ?, ?> builder =
                  (AbstractBuilder<IInvokableInstance, ?, ?>)
                      clusterClass.getMethod("build", int.class).invoke(null, nodeCount);
              configurator.accept(builder);
              ICluster<IInvokableInstance> cluster =
                  (ICluster<IInvokableInstance>) builder.createWithoutStarting();
              cluster.startup();
              return cluster;
            })
        .get();
  }

  /** Closes the cluster on the bootstrap thread, then shuts down the thread. */
  public static void shutdown(ICluster<?> cluster) throws Exception {
    if (cluster != null) {
      bootstrapExecutor
          .submit(
              () -> {
                cluster.close();
                return null;
              })
          .get();
    }
    if (bootstrapExecutor != null) {
      bootstrapExecutor.shutdownNow();
    }
  }

  private DtestClusterFactory() {}
}
