/**
 * Copyright (c) 2015 Renaud Delbru. All Rights Reserved.
 */
package com.sindicetech.mixedemotions.etl;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.dataformat.JsonLibrary;

public class DwRoute extends RouteBuilder {

  public final static int TIMER_PERIOD = 60000;

  @Override
  public void configure() throws Exception {
    errorHandler(
      // When the processing of a message fails, the message is sent to the errors endpoint
      deadLetterChannel("seda:errors")
        // retry 3 times to process the message
        .maximumRedeliveries(3).redeliveryDelay(50));

    // Trigger pipeline every TIMER_PERIOD ms
    DwApiBean dwApiBean = new DwApiBean(getContext().createProducerTemplate());
    from("timer:ping?period=" + TIMER_PERIOD)
        .bean(dwApiBean, "process");

    // Redirect messages from DW's API to a queue for video transcription
    from("direct:DwApi")
        .to("direct:unmarshal")
        .to("seda:VideoTranscription");

    // The video transcription route
    // Perform video transcription and send to the TopicExtraction endpoint
    from("seda:VideoTranscription?concurrentConsumers=4")
        .to("log:VideoTranscriptionProcessor?showHeaders=true")
        .process(new VideoTranscriptionProcessor())
        .to("direct:TopicExtraction");

    // The topic extraction route
    // Perform topic extraction on the video transcription and send to the ElasticsearchLoad endpoint
    from("direct:TopicExtraction")
        .to("log:EsTopicExtraction?showHeaders=true")
        .process(new ExpertSystemTopicExtraction())
        .to("direct:ElasticsearchLoad");

    // The elasticsearch loading route
    from("direct:ElasticsearchLoad")
        .to("direct:marshal")
        .process(new ElasticsearchLoad())
        .to("log:Load?showHeaders=true");

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
