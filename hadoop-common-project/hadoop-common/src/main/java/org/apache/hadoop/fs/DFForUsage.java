/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Shell;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/** Filesystem disk space usage statistics.  Uses the unix 'df' program*/
public class DFForUsage extends DU {
  /**
   * Keeps track of disk usage.
   * @param path the path to check disk usage in
   * @param interval refresh the disk usage at this interval
   * @throws IOException if we fail to refresh the disk usage
   */
  public DFForUsage(File path, long interval) throws IOException {
    super(path, interval, -1L);
  }
  
  /**
   * Keeps track of disk usage.
   * @param path the path to check disk usage in
   * @param conf configuration object
   * @throws IOException if we fail to refresh the disk usage
   */
  public DFForUsage(File path, Configuration conf, long initialUsed) throws IOException {
    //this(path, 600000L);
    super(path, conf, initialUsed);
    //10 minutes default refresh interval
  }
  
  public String toString() {
    return
      "df " + dirPath +"\n" +
      used + "\t" + dirPath;
  }

  protected String[] getExecString() {
    return new String[] {"df", dirPath};
  }
  
  protected void parseExecResult(BufferedReader lines) throws IOException {
    String line = lines.readLine();
    if (line == null) {
      throw new IOException("Expecting a line not the end of stream");
    }
    line = lines.readLine();

    String[] tokens = line.split("\\s+");
    if(tokens.length == 0) {
      throw new IOException("Illegal df output");
    }
    LOG.info("**********DF**************");
	LOG.info("xxx dir:" + dirPath);
	LOG.info("xxx dfsUsage:" + Long.parseLong(tokens[2])*1024);
	LOG.info("************************");
    this.used.set(Long.parseLong(tokens[2]) * 1024);
  }
}
