package com.sindicetech.mixedemotions.etl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.http.HttpResponse;
import org.apache.http.client.fluent.Content;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ExecutionException;

/**
 *
 */
public class DwApiBean {
  private Logger logger = LoggerFactory.getLogger(this.getClass());
  private static ObjectMapper mapper = new ObjectMapper();

  private CloseableHttpAsyncClient httpClient;

  public DwApiBean() {
    this.httpClient = HttpAsyncClients.custom()
        .setMaxConnPerRoute(2)
        .setMaxConnTotal(2)
        .build();
    httpClient.start();
  }

  public void download() throws ExecutionException, InterruptedException, IOException {
    Content content = Request.Get("http://www.dw.com/api/list/mediacenter/2?pageIndex=1").execute().returnContent();
    JsonNode node = mapper.readTree(content.asStream());
    ArrayNode items = (ArrayNode) node.get("items");

    System.out.println(String.format("Got %s items", items.size()));
    logger.info(String.format("Got %s items", items.size()));


//    RequestBuilder builder = RequestBuilder.create("GET").setUri("http://www.dw.com/api/list/mediacenter/2?pageIndex=1");
//    httpClient.execute(builder.build(), new RequestCallback()).get();
  }


  class RequestCallback implements FutureCallback<HttpResponse> {
    @Override
    public void completed(HttpResponse result) {

      InputStream is;
      try {
        is = result.getEntity().getContent();
      } catch (IOException ex) {
        throw new RuntimeException(String.format("Request failed: %s", ex.getMessage()));
      }
    }

    @Override
    public void failed(Exception ex) {
      logger.error(String.format("Request failed: %s", ex.getMessage()));
    }

    @Override
    public void cancelled() {
      logger.error(String.format("Request was cancelled"));
    }
  }
}


