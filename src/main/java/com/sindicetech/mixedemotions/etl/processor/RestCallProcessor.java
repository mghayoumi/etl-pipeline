/**
 * Copyright (c) 2015 Renaud Delbru. All Rights Reserved.
 */
package com.sindicetech.mixedemotions.etl.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;

public abstract class RestCallProcessor implements Processor {

  @Override
  public void process(final Exchange exchange) throws Exception {
    ObjectNode body = exchange.getIn().getBody(ObjectNode.class);
    ObjectNode response = body.putObject(this.getResponseKey());

    // Call the rest service and enrich the message
    long start = System.currentTimeMillis();
    this.process(body.deepCopy(), response);
    long timeElapsed = System.currentTimeMillis() - start;

    // Add time statistic about the rest call and processing
    response.put("took", timeElapsed);
  }

  /**
   * The key in which the response will be stored.
   */
  protected abstract String getResponseKey();

  /**
   * To be implemented by sub-classes. Given the incoming json message 'body', executes the request to the rest
   * service and write the result to 'response'.
   *
   * @param body The incoming message
   * @param response The response of the call to the rest service
   */
  protected abstract void process(JsonNode body, ObjectNode response) throws Exception;

}
