/**
 * Copyright (c) 2015 Renaud Delbru. All Rights Reserved.
 */
package com.sindicetech.mixedemotions.etl.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sindicetech.mixedemotions.etl.DwApiBean;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.fluent.Content;
import org.apache.http.client.fluent.Request;
import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicNameValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;

public class ExpertSystemTopicExtraction extends RestCallProcessor {

  public static final String RESPONSE_KEY = "topic-extraction";

  private static final Logger logger = LoggerFactory.getLogger(ExpertSystemTopicExtraction.class);
  private static ObjectMapper mapper = new ObjectMapper();

  private enum Header {
    APPJSON("Accept", ContentType.APPLICATION_JSON.getMimeType()),
    APIKEY("apikey", "bd49-97d792374e7d");

    private final String value;
    private final String name;

    Header(String name, String value) {
      this.name = name;
      this.value = value;
    }
  }

  private enum CogitoService {
    KERNEL_PEOPLE("http://217.26.90.230/cogitoapi/1.1/kernel/ext/people"),
    KERNEL_ORGANIZATIONS("http://217.26.90.230/cogitoapi/1.1/kernel/ext/organizations"),
    KERNEL_PLACES("http://217.26.90.230/cogitoapi/1.1/kernel/ext/places"),
    KERNEL_ADDITIONAL("http://217.26.90.230/cogitoapi/1.1/kernel/ext/additional"),
    KERNEL_CATEGORIZATION("http://217.26.90.230/cogitoapi/1.1/kernel/categorization"),
    KERNEL_SUMMARY("http://217.26.90.230/cogitoapi/1.1/kernel/summary"),
    KERNEL_SENTIMENT("http://217.26.90.230/cogitoapi/1.1/kernel/sentiment"),
    KERNEL_EXTENDEDSENTIMENT("http://217.26.90.230/cogitoapi/1.1/kernel/extendedsentiment"),
    KERNEL_PACK("http://217.26.90.230/cogitoapi/1.1/kernel/pack"),
    MEDIA_PEOPLE("http://217.26.90.230/cogitoapi/1.1/media/ext/people"),
    MEDIA_PLACES("http://217.26.90.230/cogitoapi/1.1/media/ext/places"),
    MEDIA_ORGANIZATIONS("http://217.26.90.230/cogitoapi/1.1/media/ext/organizations"),
    MEDIA_GROUPSOFPEOPLE("http://217.26.90.230/cogitoapi/1.1/media/ext/groupsofpeople"),
    MEDIA_BRANDS("http://217.26.90.230/cogitoapi/1.1/media/ext/brands"),
    MEDIA_PRODUCTS("http://217.26.90.230/cogitoapi/1.1/media/ext/products"),
    MEDIA_MAINELEMENTS("http://217.26.90.230/cogitoapi/1.1/media/tag/mainelements"),
    MEDIA_CHANNEL("http://217.26.90.230/cogitoapi/1.1/media/cat/channel"),
    MEDIA_IPTC("http://217.26.90.230/cogitoapi/1.1/media/cat/iptc"),
    MEDIA_PACK("http://217.26.90.230/cogitoapi/1.1/media/pack");

    private final String url;

    CogitoService(String url) {
      this.url = url;
    }
  };


  @Override
  protected String getResponseKey() {
    return RESPONSE_KEY;
  }

  private JsonNode callApi(CogitoService service, String text) throws IOException {
    Content content = Request.Post(service.url)
        .addHeader(Header.APPJSON.name, Header.APPJSON.value)
        .addHeader(Header.APIKEY.name, Header.APIKEY.value)
        .bodyForm(new BasicNameValuePair("text", text))
        .execute().returnContent();

    JsonNode analysisResult = mapper.readTree(content.asStream())
        .get("analysisResult").get("extraction").get("entities");

    return analysisResult;
  }

  private JsonNode callPackApi(CogitoService service, String text) throws IOException {
    HttpResponse response = Request.Post(service.url)
        .addHeader(Header.APPJSON.name, Header.APPJSON.value)
        .addHeader(Header.APIKEY.name, Header.APIKEY.value)
        .bodyForm(new BasicNameValuePair("text", text))
        .execute().returnResponse();

    StatusLine statusLine = response.getStatusLine();
    if (statusLine.getStatusCode() / 100 != 2) {
      String msg = String.format("Request for item %s to %s failed with text '%s'. Response status: %s",
          exchange.getIn().getHeader(DwApiBean.DW_ITEM_URL), service.url, text, statusLine.toString());
      logger.warn(msg);

      //fail fast
      throw new RuntimeException(msg);
    }

    JsonNode analysisResult = mapper.readTree(response.getEntity().getContent());

    if (!analysisResult.has("analysisResult")) {
      String msg = String.format("Wrong response from %s. Doesn't contain field 'analysisResult'", service.url);
      logger.warn(msg);

      //fail fast
      throw new RuntimeException(msg);
    }
    analysisResult = analysisResult.get("analysisResult");

    return analysisResult;
  }

  private JsonNode extractEntities(String text) throws IOException {
    ObjectNode entities = mapper.createObjectNode();

    JsonNode analysisResult = callPackApi(CogitoService.KERNEL_PACK, text);

    entities.set("result", analysisResult.deepCopy());

//    analysisResult = callApi(CogitoService.KERNEL_PLACES, text);
//    entities.set("places_kernel", analysisResult.deepCopy());
//
//    analysisResult = callApi(CogitoService.KERNEL_ORGANIZATIONS, text);
//    entities.set("organizations_kernel", analysisResult.deepCopy());
//
//    analysisResult = callApi(CogitoService.KERNEL_ADDITIONAL, text);
//    entities.set("additional_kernel", analysisResult.deepCopy());

    return entities;
  }

  @Override
  protected void process(final JsonNode body, ObjectNode response) throws Exception {
    String transcription = body.path(VideoTranscriptionProcessor.RESPONSE_KEY).path("transcription").asText();

    JsonNode entities;

    if (transcription.trim().isEmpty()) {
      logger.info(String.format("Item %s has an empty transcription. Skpping topic extraction.",
          exchange.getIn().getHeader(DwApiBean.DW_ITEM_URL)));
      entities = mapper.createObjectNode();
    } else {
      entities = extractEntities(transcription);
    }

    response.set("entities", entities);

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
