/**
 * Copyright (c) 2015 Renaud Delbru. All Rights Reserved.
 */
package com.sindicetech.mixedemotions.etl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;

import java.util.HashMap;
import java.util.Map;

public class VideoTranscriptionProcessor extends RestCallProcessor {

  final public static int DELAY_TIME = 3000;

  public static final String RESPONSE_KEY = "video-transcription";

  @Override
  protected String getResponseKey() {
    return RESPONSE_KEY;
  }

  @Override
  protected void process(final JsonNode body, ObjectNode response) throws Exception {
    Thread.sleep(DELAY_TIME);
    String teaser = body.path("teaser").asText();
    response.put("transcription", teaser);
  }

}
