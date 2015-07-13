package com.sindicetech.mixedemotions.etl.main;

import java.util.concurrent.CountDownLatch;

/**
 * Allows anyone to stop the CamelContext.
 *
 * Calling {@link #stop()} counts down a latch for which {@link Main} is waiting.
 */
public class StopSwitch {
  private final CountDownLatch latch = new CountDownLatch(1);

  /**
   * Signals {@link Main} to stop CamelContext.
   */
  public void stop() {
    latch.countDown();
  }

  /**
   * Calls CountDownLatch.await(). To be called only by {@link Main}.
   *
   * @throws InterruptedException
   */
  public void await() throws InterruptedException {
    latch.await();
  }
}
