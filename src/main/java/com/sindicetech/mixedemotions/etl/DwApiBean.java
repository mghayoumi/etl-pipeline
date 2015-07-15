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
import java.time.Instant;
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
  private final Instant fromDate;
  private final int maxItems;

  public static final String ENDPOINT = "direct:DwApi";

  private Instant lastItemInstant = Instant.parse("1015-07-11T00:00:00.000Z");

  private CloseableHttpAsyncClient httpClient;

  public DwApiBean(ProducerTemplate producer, int maxItems, Instant fromDate) {
    mapper.enable(SerializationFeature.INDENT_OUTPUT);

    this.producer = producer;
    this.maxItems = maxItems;
    this.fromDate = fromDate;

    this.httpClient = HttpAsyncClients.custom()
        .setMaxConnPerRoute(2)
        .setMaxConnTotal(2)
        .build();
    httpClient.start();
  }

  public void process() throws InterruptedException, ExecutionException, IOException {
    int itemsProcessed = 0;

    int p = 1;
    ApiResponse response = new ApiResponse();
    response.availablePages = 1;

    while (p <= response.availablePages) {
      response = download(p);

      Instant newestInstant = Instant.parse(response.items.get(0).get("displayDate").asText());

      if (! newestInstant.isAfter(lastItemInstant)) {
        logger.info("No new updates.");
        return;
      }

      lastItemInstant = newestInstant;

      logger.info("First item date: " + response.items.get(0).get("displayDate"));
      logger.info("Last item date: " + response.items.get(response.items.size() - 1).get("displayDate"));

      for (int i = 0; i < response.items.size(); i++) {
        JsonNode item = response.items.get(i);
        Instant itemInstant = Instant.parse(item.get("displayDate").asText());
        if (itemInstant.isBefore(fromDate)) {
          logger.info("Reached oldest allowed item, date: " + fromDate + " Stopping load.");
          return;
        }

        String url = item.get("reference").get("url").asText();
        logger.info("Item URL: " + url);

        Content content = Request.Get(url).execute().returnContent();
        JsonNode node = mapper.readTree(content.asStream());

        Map<String, Object> headers = new HashMap<>();
        headers.put(Exchange.FILE_NAME, node.get("id").asText());
        headers.put("DwItemUrl", url);
        producer.sendBodyAndHeaders(ENDPOINT, node, headers);

        itemsProcessed++;

        if (maxItems != -1 && itemsProcessed >= maxItems) {
          logger.info("Loaded maximum configured number of items: " + maxItems + " Stopping load.");
          return;
        }
      }

      p++;
    }
  }

  private boolean responseOk(JsonNode node) {
    JsonNode params = node.get("filterParameters");

    // we assume the response is sorted by date
    return params.get("sortByDate").asBoolean();
  }

  public class ApiResponse {
    public ArrayNode items;
    public int availablePages;
  }

  private ApiResponse download(int page) throws ExecutionException, InterruptedException, IOException {
    Content content = Request.Get("http://www.dw.com/api/list/mediacenter/2?pageIndex=" + page).execute().returnContent();
    JsonNode node = mapper.readTree(content.asStream());

    int availablePages = node.get("paginationInfo").get("availablePages").asInt();

    if (!responseOk(node)) {
      throw new RuntimeException("Teaser stream is not sorted by date. From the response: " + node.get("filterParameters"));
    }

    ArrayNode items = (ArrayNode) node.get("items");

    logger.info(String.format("Got %s items", items.size()));

    ApiResponse response = new ApiResponse();
    response.items = items;
    response.availablePages = availablePages;

    return response;
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


