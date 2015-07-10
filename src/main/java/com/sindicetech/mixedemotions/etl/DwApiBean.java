package com.sindicetech.mixedemotions.etl;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;

import java.io.IOException;
import java.io.InputStream;

/**
 *
 */
public class DwApiBean {
  CloseableHttpAsyncClient httpClient;

  public DwApiBean() {
    this.httpClient = HttpAsyncClients.custom()
        .setMaxConnPerRoute(2)
        .setMaxConnTotal(2)
        .build();
    httpClient.start();
  }

//  private void download() {
//    RequestBuilder builder = RequestBuilder.create("GET").setUri("http://www.dw.com/api/list/mediacenter/2?pageIndex=1");
//    httpClient.execute(builder.build(), new RequestCallback()).get();
//  }
//
//
//  class RequestCallback implements FutureCallback<HttpResponse> {
//    @Override
//    public void completed(HttpResponse result) {
//      long count = completed_count.incrementAndGet();
//
//      InputStream is;
//      try {
//        is = result.getEntity().getContent();
//        if (verbose) {
//          logger.info("Sent an event: " + StreamUtils.streamToString(is));
//        } else {
//          if (count % EVENTS_BATCH_COUNT == 0) {
//            logger.info("Sent {} events.", count);
//          }
//        }
//      } catch (IOException ex) {
//        throw new RuntimeException(String.format("Couldn't send request to %s: %s", cepEventsUri, ex.getMessage()));
//      }
//    }
//
//    @Override
//    public void failed(Exception ex) {
//      failures++;
//      logger.error(String.format("Couldn't send request to %s: %s", cepEventsUri, ex.getMessage()));
//    }
//
//    @Override
//    public void cancelled() {
//      failures++;
//      logger.error(String.format("Request to %s was cancelled", cepEventsUri));
//    }
//  }
}


