package com.sindicetech.mixedemotions.etl.main;

import com.typesafe.config.Config;
import org.apache.camel.component.properties.PropertiesFunction;

/**
 * Resolves properties in route specifications based on the {@link Config} loaded from an application.conf.
 *
 * Supports default values: <pre>{{appconf:port:9300}}</pre> - everything after the last colon ':' is the default value.
 */
public class ApplicationConfPropertiesFunction implements PropertiesFunction {
  public static final String NAME = "appconf";
  private final Config conf;

  public ApplicationConfPropertiesFunction(MainArgs mainArgs) {
    this.conf = mainArgs.getConf();
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public String apply(String remainder) {
    if (conf == null) {
      throw new IllegalArgumentException(String.format("Cannot reslove %s: properties when no application.conf is loaded." +
          " Tip: start the application pointing to an application.conf file.", NAME));
    }

    String defaultValue = null;
    int lastColon = remainder.lastIndexOf(':');
    if (lastColon != -1) {
      defaultValue = remainder.substring(lastColon + 1);
      remainder = remainder.substring(0, lastColon);
    }

    if (conf.hasPath(remainder)) {
      return conf.getAnyRef(remainder).toString();
    } else if (defaultValue != null) {
      return defaultValue;
    }

    throw new IllegalArgumentException("Path " + remainder + " not found in the application.conf Config.");
  }
}
