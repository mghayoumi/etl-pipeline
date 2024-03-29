/**
 * Copyright (c) 2015 Renaud Delbru. All Rights Reserved.
 */
package com.sindicetech.mixedemotions.etl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sindicetech.mixedemotions.etl.elasticsearch.ElasticsearchIndexChecker;
import com.sindicetech.mixedemotions.etl.processor.ExpertSystemTopicExtraction;
import com.sindicetech.mixedemotions.etl.processor.VideoTranscriptionProcessor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.elasticsearch.aggregation.BulkRequestAggregationStrategy;
import org.apache.camel.model.dataformat.JsonLibrary;
import org.elasticsearch.action.index.IndexRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DwRoute extends RouteBuilder {


  private static final Logger logger = LoggerFactory.getLogger(DwRoute.class.getName());
  private static final ObjectMapper mapper = new ObjectMapper();
  private final DwConfig config;


  public DwRoute(DwConfig config) {
    this.config = config;
  }

  @Override
  public void configure() throws Exception {
    errorHandler(
        // When the processing of a message fails, the message is sent to the errors endpoint
        deadLetterChannel("seda:errors")
            .log(logger).logHandled(true)
            //.loggingLevel(LoggingLevel.INFO).logStackTrace(true)
            // retry 3 times to process the message
            .maximumRedeliveries(3).redeliveryDelay(50)
    );

    // Trigger pipeline every TIMER_PERIOD ms


    DwApiBean dwApiBean = new DwApiBean(getContext().createProducerTemplate(), config.maxItems, config.fromDate);
    from("timer:ping?period=" + config.timerPeriod.toMillis()).routeId("DW-API-processor")
        .bean(dwApiBean, "process")
        .log("Processing ${header." + DwApiBean.DW_ITEM_URL + "} (${id})");

    // Redirect messages from DW's API to a queue for video transcription
    from("direct:DwApi").routeId("queue-for-video-transcription")
        //.to("direct:unmarshal")
        .to("seda:VideoTranscription");

    // The video transcription route
    // Perform video transcription and send to the TopicExtraction endpoint
    from("seda:VideoTranscription?concurrentConsumers=4").routeId("VideoTranscription")
//        .to("log:VideoTranscriptionProcessor?showHeaders=true")
        .log("Processing ${header." + DwApiBean.DW_ITEM_URL + "} (${id})")
        .process(new VideoTranscriptionProcessor())
        .to("direct:TopicExtraction");

    // The topic extraction route
    // Perform topic extraction on the video transcription and send to the ElasticsearchLoad endpoint
    from("direct:TopicExtraction").routeId("TopicExtraction")
//        .to("log:EsTopicExtraction?showHeaders=true")
        .process(new ExpertSystemTopicExtraction())
        .log("Processing ${header." + DwApiBean.DW_ITEM_URL + "} (${id})")
        .to("direct:TopicExtractionJsonTransformation");

    from("direct:TopicExtractionJsonTransformation").routeId("TopicExtractionJsonTransformation")
//        .to("log:EsTopicExtraction?showHeaders=true")
        .process(exchange -> {
              ObjectNode node = exchange.getIn().getBody(ObjectNode.class);

              JsonNode sizes = node.path("mainContent").path("previewImage").path("sizes");
              if (sizes.isMissingNode() || !sizes.isArray()) {
                return;
              }

              ((ObjectNode)node.path("mainContent").path("previewImage")).set("singleUrl", sizes.path(0).path("url"));
            }
        )
        .log("Processing ${header." + DwApiBean.DW_ITEM_URL + "} (${id})")
        .to("direct:ElasticsearchBulk");

    ElasticsearchIndexChecker elasticsearchIndexChecker = new ElasticsearchIndexChecker(config.elasticsearchMappings);

    from("direct:ElasticsearchBulk").routeId("ElasticsearchIndexing")
        .process(exchange -> {
          JsonNode node = (JsonNode) exchange.getIn().getBody();
          exchange.getIn().setBody(
              new IndexRequest(config.indexName, config.indexType, node.get("id").asText())
                  .source(mapper.writeValueAsString(node))
          );
        })
        .aggregate(constant(true), new BulkRequestAggregationStrategy()).completionSize(100).completionTimeout(10_000)
        .bean(elasticsearchIndexChecker, "checkIndex(Exchange,{{appconf:elasticsearch.clusterName:elasticsearch}},{{appconf:elasticsearch.ip:localhost}},{{appconf:elasticsearch.port:9300}})")
        .log("Processing ${header." + DwApiBean.DW_ITEM_URL + "} (${id})")
        .to("elasticsearch://{{appconf:elasticsearch.clusterName}}?ip=localhost&operation=BULK_INDEX");


    // Convert body from Json string to JsonNode
    from("direct:unmarshal")
        .unmarshal().json(JsonLibrary.Jackson, JsonNode.class);

    // Convert body from JsonNode to Json string
    from("direct:marshal")
        .marshal().json(JsonLibrary.Jackson, String.class);

    // The errors endpoint. It will save the headers and body of the message to a file,
    // for further inspection and debug.
    from("seda:errors")
        .transform(simple("Headers: ${headers}, Body: ${body}"))
        .to("file:errors?fileName=${date:now:yyyyMMdd}/${file:name}");

  }


}
