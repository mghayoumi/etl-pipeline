package com.sindicetech.mixedemotions.etl.main;

import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * Hawtio configuration holder.
 *
 * Holds configuration info for Hawtio.
 * Sets Hawt.io environment variables.
 */
public class HawtioConf {
  // see http://hawt.io/configuration/index.html#Configuration_Properties
  public static final String HAWTIO_CONFIG_DIR = "hawtio.config.dir";
  public static final String HAWTIO_DIRNAME = "hawtio.dirname";
  public static final String HAWTIO_CONFIG_REPO = "hawtio.config.repo";
  public static final String HAWTIO_CONFIG_CLONEONSTARTUP = "hawtio.config.cloneOnStartup";
  public static final String HAWTIO_CONFIG_PULLONSTARTUP = "hawtio.config.pullOnStartup";
  public static final String HAWTIO_CONFIG_IMPORTURLS = "hawtio.config.importURLs";
  public static final String HAWTIO_WAR_CONF = "hawtio.war";
  public static final Pattern HAWTIO_WAR_PATTERN = Pattern.compile("hawtio.*\\.war");

  public final int port;
  public final String contextPath;
  public final String warPath;
  private final Logger logger = LoggerFactory.getLogger(this.getClass());

  private String toAbsolutePath(String path) {
    return Paths.get(path).toAbsolutePath().normalize().toString();
  }

  private void setSystemProperty(String property, Config config) {
    if (config.hasPath(property)) {
      String path;
      // if it's a path, make it an absolute path
      if (HAWTIO_CONFIG_DIR.equals(property)) {
        path = toAbsolutePath(config.getString(property));
      } else if (HAWTIO_CONFIG_REPO.equals(property)) {
        // the value of this property can be a path or a URL to a Git repo, which could be http:// https:// or probably
        // even git@github.com:sindicetech/...
        // instead of anticipating all the possible URL schemes, let's just try to convert it to an absolute path and
        // see if it exists
        String absPath = toAbsolutePath(config.getString(property));
        if (new File(absPath).exists()) {
          path = absPath;
        } else {
          // if it doesn't exist, it might be a URL that git is able to clone
          path = config.getString(property);
        }
      } else {
        path = config.getString(property);
      }

      logger.info(String.format("Setting %s to %s", property, path));
      System.setProperty(property, path);
    }
  }

  public HawtioConf(int port, String contextPath, String warPath, Config config) {
    this.port = port;
    this.contextPath = contextPath;
    this.warPath = warPath;

    setSystemProperty(HAWTIO_CONFIG_DIR, config);
    setSystemProperty(HAWTIO_DIRNAME, config);
    setSystemProperty(HAWTIO_CONFIG_REPO, config);
    setSystemProperty(HAWTIO_CONFIG_CLONEONSTARTUP, config);
    setSystemProperty(HAWTIO_CONFIG_PULLONSTARTUP, config);
    setSystemProperty(HAWTIO_CONFIG_IMPORTURLS, config);
  }
}
