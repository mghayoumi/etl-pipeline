package com.sindicetech.mixedemotions.etl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.camel.Exchange;
import org.apache.camel.ProducerTemplate;
import org.apache.http.HttpResponse;
import org.apache.http.client.fluent.Content;
import org.apache.http.client.fluent.Request;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 *
 */
public class DwApiBean {

  private Logger logger = LoggerFactory.getLogger(this.getClass());
  private static ObjectMapper mapper = new ObjectMapper();

  private final ProducerTemplate producer;
  public static final String ENDPOINT = "direct:DwApi";

  private LocalDate lastItemDate = LocalDate.parse("1015-07-11T00:00:00.000Z", DateTimeFormatter.ISO_ZONED_DATE_TIME);

  private CloseableHttpAsyncClient httpClient;

  public DwApiBean(ProducerTemplate producer) {
    mapper.enable(SerializationFeature.INDENT_OUTPUT);

    this.producer = producer;

    this.httpClient = HttpAsyncClients.custom()
        .setMaxConnPerRoute(2)
        .setMaxConnTotal(2)
        .build();
    httpClient.start();
  }

  public void process() throws InterruptedException, ExecutionException, IOException {
    for (int p = 1; p < 2; p++) {
      ArrayNode items = download(p);

      LocalDate newestDate = LocalDate.parse(items.get(0).get("displayDate").asText(), DateTimeFormatter.ISO_ZONED_DATE_TIME);

      if (! newestDate.isAfter(lastItemDate)) {
        logger.info("No new updates.");
        return;
      }

      lastItemDate = newestDate;

      logger.info("First item date: " + items.get(0).get("displayDate"));
      logger.info("Last item date: " + items.get(items.size() - 1).get("displayDate"));

      for (int i = 0; i < Math.min(5, items.size()); i++) {
        JsonNode item = items.get(i);
        String url = item.get("reference").get("url").asText();
        logger.info("Item URL: " + url);

        Content content = Request.Get(url).execute().returnContent();
        JsonNode node = mapper.readTree(content.asStream());

        Map<String, Object> headers = new HashMap<>();
        headers.put(Exchange.FILE_NAME, node.get("id").asText());
        headers.put("DwItemUrl", url);
        producer.sendBodyAndHeaders(ENDPOINT, mapper.writeValueAsString(node), headers);
      }

    }
  }

  private boolean responseOk(JsonNode node) {
    JsonNode params = node.get("filterParameters");

    // we assume the response is sorted by date
    return params.get("sortByDate").asBoolean();
  }

  private ArrayNode download(int page) throws ExecutionException, InterruptedException, IOException {
    Content content = Request.Get("http://www.dw.com/api/list/mediacenter/2?pageIndex=" + page).execute().returnContent();
    JsonNode node = mapper.readTree(content.asStream());

    if (!responseOk(node)) {
      throw new RuntimeException("Teaser stream is not sorted by date. From the response: " + node.get("filterParameters"));
    }

    ArrayNode items = (ArrayNode) node.get("items");

    logger.info(String.format("Got %s items", items.size()));

    return items;
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


