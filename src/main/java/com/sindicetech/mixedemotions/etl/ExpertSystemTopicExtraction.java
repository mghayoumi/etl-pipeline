/**
 * Copyright (c) 2015 Renaud Delbru. All Rights Reserved.
 */
package com.sindicetech.mixedemotions.etl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class ExpertSystemTopicExtraction extends RestCallProcessor {

  public static final String RESPONSE_KEY = "topic-extraction";

  @Override
  protected String getResponseKey() {
    return RESPONSE_KEY;
  }

  @Override
  protected void process(final JsonNode body, ObjectNode response) throws Exception {
    String transcription = body.path(VideoTranscriptionProcessor.RESPONSE_KEY).path("transcription").asText();

    String[] terms = transcription.split(" ");
    Random r = new Random();
    int size = r.nextInt(5);
    ArrayNode array = response.putArray("topics");
    for (int i = 0; i < size; i++) {
      int index = r.nextInt(terms.length);
      array.add(terms[index]);
    }
  }

}
