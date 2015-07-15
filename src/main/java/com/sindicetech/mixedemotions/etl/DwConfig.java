package com.sindicetech.mixedemotions.etl;

import com.sindicetech.mixedemotions.etl.elasticsearch.ElasticsearchMappings;
import com.sindicetech.mixedemotions.etl.main.MainArgs;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;

/**
 * A helper class to load and hold {@link DwApiBean}'s configuration from {@link MainArgs}.
 */
public class DwConfig {
  private static final Logger logger = LoggerFactory.getLogger(DwConfig.class.getName());

  public final static Duration DEFAULT_TIMER_PERIOD = Duration.ofMinutes(1);
  public final static String DEFAULT_FROM_DATE = "1982-10-27T00:00:00.000Z";
  public static final int DEFAULT_MAX_ITEMS = 1000;
  private static final String DEFAULT_INDEX_NAME = "mixedemotions";
  private static final String DEFAULT_INDEX_TYPE = "doc";

  private final MainArgs mainArgs;

  public int maxItems;
  public Instant fromDate;
  public String indexName;
  public String indexType;
  public Duration timerPeriod;
  public ElasticsearchMappings elasticsearchMappings;

  public DwConfig(MainArgs mainArgs) {
    this.mainArgs = mainArgs;
  }

  private String getConfString(String path, String defaultValue) {
    Config config = mainArgs.getConf();

    if (config.hasPath(path)) {
      return config.getString(path);
    }

    logger.info(String.format("No value set for %s. Using %s", path, defaultValue));

    return defaultValue;
  }

  private int getConfInt(String path, int defaultValue) {
    Config config = mainArgs.getConf();

    if (config.hasPath(path)) {
      return config.getInt(path);
    }

    logger.info(String.format("No value set for %s. Using %s", path, defaultValue));

    return defaultValue;
  }

  private Duration getConfDuration(String path, Duration defaultValue) {
    Config config = mainArgs.getConf();

    if (config.hasPath(path)) {
      return config.getDuration(path);
    }

    logger.info(String.format("No value set for %s. Using %s", path, defaultValue));

    return defaultValue;
  }

  public void configure() throws IOException {
    this.maxItems = getConfInt("dwapi.items.max", DEFAULT_MAX_ITEMS);
    try {
      this.fromDate = Instant.parse(getConfString("dwapi.items.fromDate", DEFAULT_FROM_DATE));
    } catch (DateTimeParseException ex) {
      throw new RuntimeException("Couldn't parse dwapi.items.fromDate configuration value (it must be of the form \""+ DEFAULT_FROM_DATE +"\"): "
        + ex.getMessage(), ex);
    }

    this.indexName = getConfString("elasticsearch.indexName", DEFAULT_INDEX_NAME);
    this.indexType = getConfString("elasticsearch.indexType", DEFAULT_INDEX_TYPE);
    this.timerPeriod = getConfDuration("timer.period", DEFAULT_TIMER_PERIOD);

    this.elasticsearchMappings = new ElasticsearchMappings(mainArgs);
  }
}
