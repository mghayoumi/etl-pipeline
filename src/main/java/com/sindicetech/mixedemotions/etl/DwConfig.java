package com.sindicetech.mixedemotions.etl;

import com.sindicetech.mixedemotions.etl.elasticsearch.ElasticsearchMappings;
import com.sindicetech.mixedemotions.etl.main.MainArgs;
import com.typesafe.config.Config;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.metrics.max.InternalMax;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
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
  public String elasticsearchClusterName;
  public String elasticsearchIp;
  public int elasticsearchTransportPort;
  public boolean fromElasticsearch;

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

  private boolean getConfBoolean(String path, boolean defaultValue) {
    Config config = mainArgs.getConf();

    if (config.hasPath(path)) {
      return config.getBoolean(path);
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
    this.fromElasticsearch = getConfBoolean("dwapi.items.fromDatefromElasticsearch", false);
    this.elasticsearchClusterName = getConfString("elasticsearch.clusterName", "elasticsearch");
    this.elasticsearchIp = getConfString("elasticsearch.ip", "localhost");
    this.elasticsearchTransportPort = getConfInt("elasticsearch.port", 9300);

    if (fromElasticsearch) {
      this.fromDate = queryNewestElasticsearchItem();
    }


    this.elasticsearchMappings = new ElasticsearchMappings(mainArgs);
  }

  private Instant queryNewestElasticsearchItem() {
    Settings settings = ImmutableSettings.settingsBuilder()
        .put("cluster.name", elasticsearchClusterName)
        .put("client.transport.ignore_cluster_name", false)
        .put("node.client", true)
        .put("client.transport.sniff", true)
        .build();
    TransportClient client = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(elasticsearchIp, elasticsearchTransportPort));

    String query = "{\n" +
        "  \"aggs\": {\n" +
        "    \"newestDate\": {\n" +
        "      \"max\": {\n" +
        "        \"field\": \"displayDate\"\n" +
        "      }\n" +
        "    }\n" +
        "  }\n" +
        "}";

    SearchResponse response = client.search(new SearchRequest().indices(indexName).types(indexType).source(query)).actionGet();
    InternalMax agg = response.getAggregations().get("newestDate");

    Instant instant;

    if (agg.getValue() == Double.NEGATIVE_INFINITY) {
      logger.info(
          String.format("Elasticsearch %s %s/%s/%s probably contains no data (returned negative infinity as latest displayDate). " +
              "Using fromDate instead: %s",
              elasticsearchClusterName, elasticsearchIp, indexName, indexType, fromDate)
      );
      instant = fromDate;
    } else {
      instant = OffsetDateTime.parse(agg.getValueAsString(), DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant();
      logger.info(String.format("Newest displayDate found in Elasticsearch is %s. Using it as fromDate.", instant.toString()));
    }

    return instant;
  }
}
