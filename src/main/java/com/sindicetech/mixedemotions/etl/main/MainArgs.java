package com.sindicetech.mixedemotions.etl.main;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Represents partially pre-processed command line arguments given to the main class {@link Main}.
 */
public class MainArgs {
  private static final Logger logger = LoggerFactory.getLogger(MainArgs.class);
  public static final String CONFDIR = "confdir";
  public static final String HAWTIO_WAR_CONF = "hawtio.war";

  private final String[] args;
  private final File confFile;
  private final boolean stop;

  private Config conf;
  private File confDir;
  private File hawtioWar;

  public MainArgs(File confFile, boolean stop, String... args) {
    this.confFile = confFile;
    this.args = args;
    this.stop = stop;
  }

  /**
   * Loads configuration from confFile.
   * If confFile is null, loads config from classpath.
   */
  public void loadConf() {
    if (confFile == null) {
      conf = ConfigFactory.load();
    } else {
      if (!confFile.exists()) {
        throw new RuntimeException("File " + confFile.getAbsolutePath() + " doesn't exist. Can't load configuration.");
      }

      conf = ConfigFactory.parseFile(confFile);
      conf = ConfigFactory.load(conf);

      // by default, assume the confdir is the parent dir of conffile
      this.confDir = confFile.getParentFile();
    }

    determineConfigDir(conf);
    determineHawtioWar();
  }

  private void determineHawtioWar() {
    File war = null;

    // take war path from config first
    if (conf.hasPath(HAWTIO_WAR_CONF)) {
      war = new File(conf.getString(HAWTIO_WAR_CONF));
    }

    if (war == null) {
      logger.warn("No hawt.io configuration provided.");
      return;
    }

    if (! war.exists()) {
      logger.warn("Couldn't find hawt.io war. " + war.getAbsolutePath() + " doesn't exist.");
      return;
    }

    this.hawtioWar = war;
  }

  private void determineConfigDir(Config conf) {
    // confdir specified in the conffile has precedence
    if (conf.hasPath(CONFDIR)) {
      this.confDir = new File(conf.getString(CONFDIR));
      if (!this.confDir.isDirectory()) {
        throw new IllegalArgumentException("Configuration directory " + confDir.getAbsolutePath() + " does not exist " +
            "or it is not a directory.");
      }
    }
  }

  public String[] getArgs() {
    return args;
  }

  public File getConfFile() {
    return confFile;
  }

  public File getConfDir() {
    return confDir;
  }

  public Config getConf() {
    return conf;
  }

  public File getHawtioWar() {
    return hawtioWar;
  }

  public boolean isStop() {
    return stop;
  }
}
