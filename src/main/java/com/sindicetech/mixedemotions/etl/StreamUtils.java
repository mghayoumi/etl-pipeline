package com.sindicetech.mixedemotions.etl;

import java.io.InputStream;
import java.util.Scanner;

/**
 * A helper class for Java streams.
 */
public class StreamUtils {
  /**
   * Reads the whole stream and returns it as a String.
   *
   * @param is Stream to read.
   * @return The content of the stream as a String.
   */
  public static String streamToString(InputStream is) {
    Scanner s = new Scanner(is, "UTF-8").useDelimiter("\\A");
    return s.hasNext() ? s.next() : "";
  }
}
