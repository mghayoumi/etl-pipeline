/**
 * Copyright (c) 2015 Renaud Delbru. All Rights Reserved.
 */
package com.sindicetech.mixedemotions.etl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOError;
import java.io.IOException;
import java.util.Random;

public class ElasticsearchLoad implements Processor {

  private Logger logger = LoggerFactory.getLogger(this.getClass());

  @Override
  public void process(final Exchange exchange) throws Exception {
    logger.info("Loading message {}", exchange.getIn());
    throw new IOException("Error while loading");
  }

}
