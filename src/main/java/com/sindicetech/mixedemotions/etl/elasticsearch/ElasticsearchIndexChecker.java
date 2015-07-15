package com.sindicetech.mixedemotions.etl.elasticsearch;

import com.sindicetech.mixedemotions.etl.util.StreamUtils;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Component that checks if the index and index type that are provided as part of the message header are
 * configured. It the index does not exist, the component sends a request to the Elasticsearch cluster to create it.
 * This component will also automatically set the default mapping and the index type mapping (if provided) after
 * the creation of the index.
 */
public class ElasticsearchIndexChecker {

  private final Logger logger = LoggerFactory.getLogger(this.getClass());
  public static final String INDEX_HEADER = "indexName";
  public static final String TYPE_HEADER = "indexType";
  private static final String DEFAULT_MAPPING = "default-mapping.json";

  private final Set<String> indexTypesConfigured = new HashSet<>();
  private final ElasticsearchMappings mappings;
  private final String defaultMapping;

  public ElasticsearchIndexChecker(ElasticsearchMappings mappings) {
    this.mappings = mappings;

    // load default mapping
    defaultMapping = StreamUtils.streamToString(ClassLoader.getSystemResourceAsStream(DEFAULT_MAPPING));
  }

  private String key(String indexName, String indexType) {
    return indexName + "." + indexType;
  }



  public void checkIndex(Exchange exchange, String clusterName, String ip, Integer port) {
    Message message = exchange.getIn();

    // we're assuming that all requests are IndexRequests with the same index and type
    // --> check the first one
    List<ActionRequest> requests = ((BulkRequest)exchange.getIn().getBody()).requests();

    IndexRequest indexRequest = (IndexRequest) requests.get(0);
    String indexName = indexRequest.index();
    String indexType = indexRequest.type();

    if (!indexTypesConfigured.contains(key(indexName, indexType))) {
      configureMapping(indexName, indexType, clusterName, ip, port);
    }
  }

  /**
   * Creates an Elasticsearch Transport client.
   *
   * We instantiate a client per mapping - not ideal but not that bad either.
   * This way we can be sure that the client is always properly closed.
   * Guice unfortunately doesn't support JSR-250 lifecycle methods such as @PreDestroy.
   *
   * Taken from {@link org.apache.camel.component.elasticsearch.ElasticsearchEndpoint}
   *
   */
  private Client getClient(String clusterName, String ip, int port) {
    Settings settings = ImmutableSettings.settingsBuilder()
        .put("cluster.name", clusterName)
        .put("client.transport.ignore_cluster_name", false)
        .put("node.client", true)
        .put("client.transport.sniff", true)
        .build();
    return new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(ip, port));
  }

  private void configureMapping(String indexName, String indexType, String clusterName, String ip, int port) {
    try(Client client = getClient(clusterName, ip, port))  {
      ensureIndexExists(indexName, client);

      logger.info(String.format("Mapping for index %s type %s hasn't been set yet. Setting it...", indexName, indexType));

      if (mappings.containsKey(indexType)) {
        PutMappingResponse response = client.admin().indices().preparePutMapping(indexName).setType(indexType).setSource(mappings.get(indexType)).get();
        logger.info(String.format("Set mapping for index %s type %s: acknowledged: %s", indexName, indexType, response.isAcknowledged()));
      }
      else {
        logger.info(String.format("No mapping for %s/%s found, not setting it.", indexName, indexType));
      }

      PutMappingResponse response = client.admin().indices().preparePutMapping(indexName).setType(indexType).setSource(defaultMapping).get();
      logger.info(String.format("Set default mapping for index %s type %s: acknowledged: %s", indexName, indexType, response.isAcknowledged()));

      indexTypesConfigured.add(key(indexName, indexType));
    }
    catch (MergeMappingException e) {
      logger.error(String.format("An incompatible mapping is already set for index %s type %s. You should delete your index and try again.", indexName, indexType));
      throw e;
    }
    catch (ElasticsearchException e) {
      logger.error(String.format("Unexpected error while trying to set the mapping for index %s type %s.", indexName, indexType));
      throw e;
    }
  }

  private void ensureIndexExists(String indexName, Client client) {
    if (!client.admin().indices().exists(new IndicesExistsRequest(indexName)).actionGet().isExists()) {
      logger.info(String.format("Index %s does not exist. Creating it...", indexName));
      CreateIndexResponse response = client.admin().indices().create(new CreateIndexRequest(indexName)).actionGet();
      logger.info(String.format("Created index %s: acknowledged: %s", indexName, response.isAcknowledged()));
    }
  }

}
