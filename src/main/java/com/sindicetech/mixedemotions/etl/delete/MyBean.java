package com.sindicetech.mixedemotions.etl.delete;

import org.apache.camel.ConsumerTemplate;
import org.apache.camel.ProducerTemplate;

/**
 *
 */
public class MyBean {

  private final ConsumerTemplate consumer;
  private final ProducerTemplate producer;

  public MyBean(ConsumerTemplate consumer, ProducerTemplate producer) {
    this.consumer = consumer;
    this.producer = producer;
  }

  public void doIt(String body) {
    System.out.println("received body: " + body);
    for (int i = 0; i < 2; i++) {
      producer.sendBody("direct:asdf", String.format("Item %s: %s", i, Math.random()));
    }
  }
}
