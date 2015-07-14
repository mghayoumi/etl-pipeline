package com.sindicetech.mixedemotions.etl;

import com.sindicetech.mixedemotions.etl.main.MainArgs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;

/**
 * Loads Elasticsearch mappings from files in a directory specified in configuration loaded by {@link MainArgs}.
 */
public class ElasticsearchMappings extends HashMap<String, String> {

  private Logger logger = LoggerFactory.getLogger(this.getClass());

  public ElasticsearchMappings(MainArgs mainArgs) throws IOException {
    File mappingsDir = new File(mainArgs.getConfDir(), "mappings");

    if (!mappingsDir.isDirectory()) {
      logger.warn("Mappings directory doesn't exist: " + mappingsDir.getAbsolutePath());
      return;
    }

    for (File file : mappingsDir.listFiles()) {
      put(file.getName(), new String(Files.readAllBytes(Paths.get(file.toURI()))));
    }

    logger.info(String.format("Loaded mappings for classes %s from %s", keySet(), mappingsDir.getAbsolutePath()));
  }
}
