package com.sindicetech.mixedemotions.etl.main;

import com.sindicetech.mixedemotions.etl.DwRoute;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.camel.CamelContext;
import org.apache.camel.component.log.LogComponent;
import org.apache.camel.component.properties.PropertiesComponent;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.impl.SimpleRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.concurrent.CountDownLatch;

/**
 * Main application class of the Flow.
 *
 * Provides a command-line interface, starts and stops CamelContext.
 */
public class Main {
  public static StopSwitch stopSwitch = new StopSwitch();

  protected OptionSet opts;
  protected MainArgs mainArgs;
  private Hawtio hawt;
  private HangupInterceptor interceptor;

  protected final static Logger logger = LoggerFactory.getLogger(Main.class);
  private final static OptionParser parser = new OptionParser();
  private final static String HELP  = "help";
  private final static String CONF  = "conf";
  private final static String STOP  = "stop";

  private void initOptionParser() {
    parser.accepts(HELP, "print this help");
    parser.accepts(CONF, "configuration file").withOptionalArg().ofType(File.class).describedAs("file");
    parser.accepts(STOP, "stop application when the flow is completed");
  }

  private void printHelp() throws IOException {
    System.out.println("Runs the Json-Builder Camel flow");
    parser.printHelpOn(System.out);
  }

  private static class HangupInterceptor extends Thread {
    private final CountDownLatch latch;
    private final StopSwitch stopSwitch;

    public HangupInterceptor(StopSwitch stopSwitch) {
      super("ETL-HangupInterceptor");
      this.stopSwitch = stopSwitch;
      this.latch = new CountDownLatch(1);
    }

    @Override
    public void run() {
      logger.info("Received a hang up signal. Stopping...");
      stopSwitch.stop();
      try {
        latch.await();
      } catch (InterruptedException e) {
        throw new RuntimeException("Interrupted while waiting for Main to finish: " + e.getMessage(), e);
      }
      logger.info("Application stopped cleanly.");
    }

    /**
     * Gives the interceptor a signal that everything has been stopped successfully.
     */
    public void countDown() {
      latch.countDown();
    }
  }

  private void describeEnvironment() {
    logger.info("Current environment:");
    logger.info("\tCurrent working directory: " + System.getProperty("user.dir"));
    logger.info("\tUser home directory: " + System.getProperty("user.home"));
    logger.info(String.format("\tJava: %s %s %s", System.getProperty("java.vendor"), System.getProperty("java.version"),
        System.getProperty("java.vendor.url")));

    ClassLoader cl = ClassLoader.getSystemClassLoader();
    URL[] urls = ((URLClassLoader)cl).getURLs();
    logger.info("\tClasspath: ");
    for(URL url: urls){
      logger.info("\t\t" + url.getFile());
    }
  }

  private void parseOptions(String[] args) throws IOException {
    opts = parser.parse(args);

    if (opts.has(HELP)) {
      printHelp();
      System.exit(0);
    }
  }

  protected void loadConfig(OptionSet opts, String[] args) {
    File confFile = (File) opts.valueOf(CONF);
    mainArgs = new MainArgs(confFile, opts.has(STOP), args);
    mainArgs.loadConf();
  }

  private void startHawtio(MainArgs mainArgs) {
    if (mainArgs.getHawtioWar() != null) {
      logger.info("Starting Hawt.io");
      HawtioConf hawtioConf = new HawtioConf(8080, "/hawtio", mainArgs.getHawtioWar().getAbsolutePath(), mainArgs.getConf());

      hawt = new Hawtio(hawtioConf);
      try {
        hawt.start();
      } catch (Exception e) {
        throw new RuntimeException("Problem starting hawt.io: " + e.getMessage(), e);
      }
    } else {
      logger.info("Not starting Hawt.io because we don't have its war. " +
          "TIP: put the war in the conf folder or set its path in application.conf");
    }
  }

  private void stopHawtio() throws Exception {
    if (hawt != null) {
      logger.info("About to stop Hawtio...");
      hawt.stop();
    }
  }

  private void executeCamelFlow() throws Exception {
    logger.info("Starting CamelContext...");

    SimpleRegistry registry = new SimpleRegistry();
    // add POJOs to the registry here using registry.put("name", <object reference>)

    CamelContext camelContext = new DefaultCamelContext(registry);

    camelContext.addComponent("log", new LogComponent());

    PropertiesComponent pc = new PropertiesComponent();
    pc.addFunction(new ApplicationConfPropertiesFunction(mainArgs));
    camelContext.addComponent("properties", pc);

    camelContext.addRoutes(new DwRoute(mainArgs));

    camelContext.getShutdownStrategy().setLogInflightExchangesOnTimeout(true);
    camelContext.getShutdownStrategy().setShutdownNowOnTimeout(false);
    camelContext.start();

    // enable Ctrl+C hangup if it shouldn't stop at the end of the flow
    if (!opts.has(STOP)) {
      interceptor = new HangupInterceptor(stopSwitch);
      Runtime.getRuntime().addShutdownHook(interceptor);
      System.out.println("\n *** Use Ctrl+C to stop the application (NOT configured to stop when the route finishes). ***\n");
      //TODO is this waiting for a latch in an interceptor really correct and safe?
    }

    stopSwitch.await();

    logger.info("Got a signal to stop.");

    logger.info("Stopping CamelContext...");
    camelContext.stop();
  }

  private void stop() {
    if (interceptor != null) {
      interceptor.countDown();
    } else {
      // better say it here too, not only after Ctrl+C
      logger.info("Application stopped cleanly.");
    }
  }

  public void run(String[] args) throws Exception {
    // Parse cli parameters
    this.initOptionParser();
    this.parseOptions(args);

    // Display information about the environment
    this.describeEnvironment();

    // Load the application config
    this.loadConfig(opts, args);

    this.startHawtio(mainArgs);

    this.executeCamelFlow();

    this.stopHawtio();

    // Stop the application
    this.stop();
  }

  public static void main(String...args) throws Exception {
    Main main = new Main();
    main.run(args);
  }

}
