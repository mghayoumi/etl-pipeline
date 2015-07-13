package com.sindicetech.mixedemotions.etl.main;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.webapp.WebAppContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 *
 * Mostly taken from {@link io.hawt.embedded.Main}.
 *
 * We have our own version because the original does not allow for stopping the Hawtio Jetty server.
 */
public class Hawtio {
  private final static Logger logger = LoggerFactory.getLogger(Hawtio.class);
  private final HawtioConf conf;
  private Server server;

  public Hawtio(HawtioConf conf) {
    this.conf = conf;
  }

  public void start() throws Exception {
    if (server != null && server.isRunning()) {
      throw new IllegalStateException("Server is already running, cannot start it.");
    }
    if (server != null) {
      //TODO test and implement in future if needed
      throw new UnsupportedOperationException("Can't start a stopped server.");
    }

    logger.info("Disabling Hawt.io authentication.");
    // http://hawt.io/configuration/index.html#Configuring_or_disabling_security_in_web_containers
    System.setProperty("hawtio.authenticationEnabled", "false");

    server = new Server(conf.port);

    HandlerCollection handlers = new HandlerCollection();
    handlers.setServer(server);
    server.setHandler(handlers);

    WebAppContext webapp = new WebAppContext();
    webapp.setServer(server);
    webapp.setContextPath(conf.contextPath);
    webapp.setWar(conf.warPath);
    webapp.setParentLoaderPriority(true);
    webapp.setLogUrlOnStart(true);

    // lets set a temporary directory so jetty doesn't bork if some process zaps /tmp/*
    String userDir = System.getProperty("user.home");
    String tempDirPath = userDir + "/.hawtio/tmp";
    File tempDir = new File(tempDirPath);
    tempDir.mkdirs();

    logger.info("Using temp directory for jetty: " + tempDir.getPath());
    webapp.setTempDirectory(tempDir);

    //TODO checking for 3rd party plugins, see the original Main

    handlers.addHandler(webapp);

    logger.info("About to start hawtio " + conf.warPath);
    server.start();
    System.out.println("\nSindice Hawt.io dashboard: http://localhost:8080/hawtio/dashboard/id/512cefc2bdb5015495\n");
  }

  public void stop() throws Exception {
    if (server == null) {
      throw new IllegalStateException("Server hasn't been started yet, can't stop it.");
    }
    logger.info("Stopping hawtio...");
    server.stop();
  }
}
