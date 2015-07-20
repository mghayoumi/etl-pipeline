package com.sindicetech.mixedemotions.etl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.sindicetech.mixedemotions.etl.util.StreamUtils;
import org.apache.camel.Exchange;
import org.apache.camel.ProducerTemplate;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 *
 */
public class DwApiBean {

  public static final String DW_ITEM_URL = "DwItemUrl";

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

  /**
   * Checks for updates loads new ones if available.
   *
   * Loads updates until any limit is reached (fromDate, or maximum number of items).
   *
   * @throws InterruptedException
   * @throws ExecutionException
   * @throws IOException
   */
  public void process() throws InterruptedException, ExecutionException, IOException {
    int itemsProcessed = 0;

    int p = 1;
    ApiResponse response = downloadPage(p);
    if (! response.ok) {
      logger.error("Couldn't download the first page of the DW list API: " + response.statusLine);
      return;
    }
    Instant newestItemInstant = Instant.parse(response.items.get(0).get("displayDate").asText());

    if (! newestItemInstant.isAfter(lastItemInstant)) {
      logger.info("No new updates.");
      return;
    }

    loadPages: while (p <= response.availablePages) {
      logger.info("Downloading page " + p);
      int lastAvailablePages = response.availablePages;
      response = downloadPage(p);
      if (!response.ok) {
        if (p < lastAvailablePages) {
          logger.warn(String.format("Problem downloading page %s. Trying page %s out of %s.", p, p+1, lastAvailablePages));
          response.availablePages = lastAvailablePages;
          p++;
          continue;
        }

        logger.warn(String.format("Problem downloading page %s out of %s.", p, lastAvailablePages));
        return;
      }

      logger.info("\tFirst item date: " + response.items.get(0).get("displayDate"));
      logger.info("\tLast item date: " + response.items.get(response.items.size() - 1).get("displayDate"));

      for (int i = 0; i < response.items.size(); i++) {
        JsonNode item = response.items.get(i);
        Instant itemInstant = Instant.parse(item.get("displayDate").asText());
        if (itemInstant.isBefore(fromDate)) {
          logger.info(String.format("Reached an item older (%s) than the oldest allowed date (%s). Stopping load.", itemInstant, fromDate));
          break loadPages;
        }

        if (itemInstant.isBefore(lastItemInstant)) {
          logger.info(String.format("Reached a previously loaded item (%s). Stopping load.", itemInstant));
          break loadPages;
        }

        String url = item.get("reference").get("url").asText();
        logger.info("Item URL: " + url);

        JsonNode node = downloadItem(url);
        if (node == null) {
          continue;
        }

        Map<String, Object> headers = new HashMap<>();
        headers.put(Exchange.FILE_NAME, node.get("id").asText());
        headers.put(DW_ITEM_URL, url);
        producer.sendBodyAndHeaders(ENDPOINT, node, headers);

        itemsProcessed++;

        if (maxItems != -1 && itemsProcessed >= maxItems) {
          logger.info("Loaded maximum configured number of items: " + maxItems + " Stopping load.");
          break loadPages;
        }
      }

      p++;
    }
    lastItemInstant = newestItemInstant;
  }

  public class ApiResponse {
    public StatusLine statusLine;
    public boolean ok = false;
    public ArrayNode items;
    public int availablePages;
  }

  private JsonNode downloadItem(String url) throws IOException {
    HttpResponse response = Request.Get(url).execute().returnResponse();
    StatusLine statusLine = response.getStatusLine();

    if (statusLine.getStatusCode() / 100 != 2) {
      logger.warn(String.format("Couldn't download item %s. Response status: %s", url, statusLine.toString()));
      return null;
    }

    return mapper.readTree(StreamUtils.streamToString(response.getEntity().getContent()));
  }

  private ApiResponse downloadPage(int page) throws InterruptedException, IOException {
    HttpResponse httpResponse = Request.Get("http://www.dw.com/api/list/mediacenter/2?pageIndex=" + page).execute().returnResponse();
    ApiResponse response = new ApiResponse();

    StatusLine statusLine = httpResponse.getStatusLine();
    response.statusLine = statusLine;
    if (statusLine.getStatusCode() / 100 != 2) {
      logger.warn(String.format("Problem downloading page %s: %s", page, statusLine.toString()));
      response.ok = false;
    } else {
      response.ok = true;
    }

    JsonNode node = mapper.readTree(httpResponse.getEntity().getContent());

    if (!(node.has("paginationInfo") && node.get("paginationInfo").has("availablePages"))) {
      response.ok = false;
      logger.warn(String.format("Page %s doesn't have field 'paginationInfo.availablePages'", page));
    } else {
      response.availablePages = node.get("paginationInfo").get("availablePages").asInt();
    }

    if (!(node.has("filterParameters") && node.get("filterParameters").has("sortByDate"))) {
      response.ok = false;
      logger.warn(String.format("Page %s doesn't have field 'filterParameters.sortByDate'", page));
    }
    else {
      JsonNode params = node.get("filterParameters");

      // we assume the response is sorted by date
      if (! params.get("sortByDate").asBoolean()) {
        String msg = "Teaser stream is not sorted by date. From the response: " + node.get("filterParameters");
        logger.error(msg);
        throw new RuntimeException(msg);
      }
    }

    if (!node.has("items")) {
      response.ok = false;
      logger.warn(String.format("Page %s doesn't have field 'items'", page));
    }  else {
      response.items = (ArrayNode) node.get("items");
      logger.debug(String.format("Got %s items", response.items.size()));
    }

    return response;
  }
}


