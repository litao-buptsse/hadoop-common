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
package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DF;
import org.apache.hadoop.fs.DU;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.StorageType;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.datanode.DataStorage;
import org.apache.hadoop.hdfs.server.datanode.DatanodeUtil;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.Time;

import java.io.*;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.*;

/**
 * The underlying volume used to store replica.
 * 
 * It uses the {@link FsDatasetImpl} object for synchronization.
 */
@InterfaceAudience.Private
class FsVolumeImpl implements FsVolumeSpi {
  public static final Log LOG = LogFactory.getLog(FsVolumeImpl.class.getName());

  private static final String DU_CACHE_FILE = "dfsUsed";
  private static final int SHUTDOWN_HOOK_PRIORITY = 30;

  private final FsDatasetImpl dataset;
  private final String storageID;
  private final StorageType storageType;
  private final Map<String, BlockPoolSlice> bpSlices
      = new ConcurrentHashMap<String, BlockPoolSlice>();
  private final File currentDir;    // <StorageDirectory>/current
  private final DF usage;           
  private final long reserved;
	private boolean useDF = false;
  protected Configuration conf = null;
  private volatile boolean dfsUsedSaved = false;
  private VolumnDU volumeUsage = null;
	
  /**
   * Per-volume worker pool that processes new blocks to cache.
   * The maximum number of workers per volume is bounded (configurable via
   * dfs.datanode.fsdatasetcache.max.threads.per.volume) to limit resource
   * contention.
   */
  private final ThreadPoolExecutor cacheExecutor;
  
  FsVolumeImpl(FsDatasetImpl dataset, String storageID, File currentDir,
      Configuration conf, StorageType storageType) throws IOException {
    this.dataset = dataset;
    this.storageID = storageID;
    this.reserved = conf.getLong(
        DFSConfigKeys.DFS_DATANODE_DU_RESERVED_KEY,
        DFSConfigKeys.DFS_DATANODE_DU_RESERVED_DEFAULT);
    this.currentDir = currentDir; 
    File parent = currentDir.getParentFile();
    this.usage = new DF(parent, conf);
    this.storageType = storageType;
		this.useDF = conf.getBoolean("hdfs.use.df.check.usage", false);
    this.conf = conf;

    final int maxNumThreads = dataset.datanode.getConf().getInt(
        DFSConfigKeys.DFS_DATANODE_FSDATASETCACHE_MAX_THREADS_PER_VOLUME_KEY,
        DFSConfigKeys.DFS_DATANODE_FSDATASETCACHE_MAX_THREADS_PER_VOLUME_DEFAULT
        );
    ThreadFactory workerFactory = new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat("FsVolumeImplWorker-" + parent.toString() + "-%d")
        .build();
    cacheExecutor = new ThreadPoolExecutor(
        1, maxNumThreads,
        60, TimeUnit.SECONDS,
        new LinkedBlockingQueue<Runnable>(),
        workerFactory);
    cacheExecutor.allowCoreThreadTimeOut(true);
  }
  
  File getCurrentDir() {
    return currentDir;
  }
  
  File getRbwDir(String bpid) throws IOException {
    return getBlockPoolSlice(bpid).getRbwDir();
  }
  
  void decDfsUsed(String bpid, long value) {
    try {
      this.getVolumeUsage().decDfsUsed(value);
    } catch (IOException e) {
      LOG.error("ERROR when get volumn DU", e);
    }
    synchronized(dataset) {
      BlockPoolSlice bp = bpSlices.get(bpid);
      if (bp != null) {
        bp.decDfsUsed(value);
      }
    }
  }

  void incDfsUsed(String bpid, long value) {
    try {
      this.getVolumeUsage().incDfsUsed(value);
    } catch (IOException e) {
      LOG.error("ERROR when get volumn DU", e);
    }
    synchronized(dataset) {
      BlockPoolSlice bp = bpSlices.get(bpid);
      if (bp != null) {
        bp.incDfsUsed(value);
      }
    }
  }

  long getDfsUsed() throws IOException {
    long volumnUesd = this.getVolumeUsage().getUsed();
    return volumnUesd;
  }

  class VolumnDU extends DU {
    public VolumnDU() throws IOException {
      super(currentDir, conf, loadDfsUsed());
    }

    @Override
    protected String[] getExecString() {
      synchronized(dataset) {
        String[] cmd = new String[bpSlices.size() + 2];
        cmd[0] = "du";
        cmd[1] = "-sk";
        int i = 2;
        for(BlockPoolSlice s : bpSlices.values()) {
          cmd[i] = s.getDirectory().getAbsolutePath();
          ++ i;
        }
        return cmd;
      }
    }

    @Override
    protected Runnable getNewRefreshThreadInstance() {
      return new Runnable() {
        @Override
        public void run() {
          boolean interrupted = false;
          while(isShouldRun()) {
            try {
              if (!interrupted) {
                Thread.sleep(getRefreshInterval());
              }
              interrupted = false;
              try {
                //update the used variable
                VolumnDU.this.run();
              } catch (IOException e) {
                synchronized (VolumnDU.this) {
                  //save the latest exception so we can return it in getUsed()
                  setDuException(e);
                }

                LOG.warn("Could not get disk usage information", e);
              }
            } catch (InterruptedException e) {
              interrupted = true;
            }
          }
        }
      };
    }

    public void recompute() {
      getRefreshUsed().interrupt();
    }

    @Override
    protected void parseExecResult(BufferedReader lines) throws IOException {
      String line = null;
      long usedBytes = 0;
      while ((line = lines.readLine()) != null) {
        String[] tokens = line.split("\t");
        if(tokens.length < 2) {
          throw new IOException("Illegal du output : " + line);
        }
        if (tokens[1].trim().equals(".")) {
          continue;
        }
        if (NumberUtils.isNumber(tokens[0])) {
          usedBytes += Long.parseLong(tokens[0]) * 1024;
        }
      }
      if (usedBytes < 0) {
        throw new IOException("Illegal du output:" + usedBytes);
      }
      setUsed(usedBytes);
    }
  }

  private DU getVolumeUsage() throws IOException {
    if (this.volumeUsage == null) {
      synchronized (this) {
        if (this.volumeUsage == null) {
          this.volumeUsage = new VolumnDU();
          volumeUsage.start();
        }
        ShutdownHookManager.get().addShutdownHook(
            new Runnable() {
              @Override
              public void run() {
                if (!dfsUsedSaved) {
                  saveDfsUsed();
                }
              }
            }, SHUTDOWN_HOOK_PRIORITY);
      }
    }
    return volumeUsage;
  }

  void saveDfsUsed() {
    File outFile = new File(currentDir, DU_CACHE_FILE);
    if (outFile.exists() && !outFile.delete()) {
      LOG.warn("Failed to delete old dfsUsed file in " +
          outFile.getParent());
    }

    FileWriter out = null;
    try {
      long used = getDfsUsed();
      if (used > 0) {
        out = new FileWriter(outFile);
        // mtime is written last, so that truncated writes won't be valid.
        out.write(Long.toString(used) + " " + Long.toString(Time.now()));
        out.flush();
        out.close();
        out = null;
      }
    } catch (IOException ioe) {
      // If write failed, the volume might be bad. Since the cache file is
      // not critical, log the error and continue.
      LOG.warn("Failed to write dfsUsed to " + outFile, ioe);
    } finally {
      IOUtils.cleanup(null, out);
    }
  }

  long loadDfsUsed() {
    long cachedDfsUsed;
    long mtime;
    Scanner sc;

    try {
      sc = new Scanner(new File(currentDir, DU_CACHE_FILE));
    } catch (FileNotFoundException fnfe) {
      return -1;
    }

    try {
      // Get the recorded dfsUsed from the file.
      if (sc.hasNextLong()) {
        cachedDfsUsed = sc.nextLong();
      } else {
        return -1;
      }
      // Get the recorded mtime from the file.
      if (sc.hasNextLong()) {
        mtime = sc.nextLong();
      } else {
        return -1;
      }

      // Return the cached value if mtime is okay.
      if (mtime > 0 && (Time.now() - mtime < 600000L)) {
        LOG.info("Cached dfsUsed found for " + currentDir + ": " +
            cachedDfsUsed);
        return cachedDfsUsed;
      }
      return -1;
    } finally {
      sc.close();
    }
  }

  long getBlockPoolUsed(String bpid) throws IOException {
    return getBlockPoolSlice(bpid).getDfsUsed();
  }
  
  /**
   * Calculate the capacity of the filesystem, after removing any
   * reserved capacity.
   * @return the unreserved number of bytes left in this filesystem. May be zero.
   */
  long getCapacity() {
    long remaining = usage.getCapacity() - reserved;
    return remaining > 0 ? remaining : 0;
  }

  @Override
  public long getAvailable() throws IOException {
    long remaining = getCapacity()-getDfsUsed();
    long available = usage.getAvailable();
    if (remaining > available) {
      remaining = available;
    }
    return (remaining > 0) ? remaining : 0;
  }
    
  long getReserved(){
    return reserved;
  }

  BlockPoolSlice getBlockPoolSlice(String bpid) throws IOException {
    BlockPoolSlice bp = bpSlices.get(bpid);
    if (bp == null) {
      throw new IOException("block pool " + bpid + " is not found");
    }
    return bp;
  }

  @Override
  public String getBasePath() {
    return currentDir.getParent();
  }
  
  @Override
  public String getPath(String bpid) throws IOException {
    return getBlockPoolSlice(bpid).getDirectory().getAbsolutePath();
  }

  @Override
  public File getFinalizedDir(String bpid) throws IOException {
    return getBlockPoolSlice(bpid).getFinalizedDir();
  }

  /**
   * Make a deep copy of the list of currently active BPIDs
   */
  @Override
  public String[] getBlockPoolList() {
    return bpSlices.keySet().toArray(new String[bpSlices.keySet().size()]);   
  }
    
  /**
   * Temporary files. They get moved to the finalized block directory when
   * the block is finalized.
   */
  File createTmpFile(String bpid, Block b) throws IOException {
    return getBlockPoolSlice(bpid).createTmpFile(b);
  }

  /**
   * RBW files. They get moved to the finalized block directory when
   * the block is finalized.
   */
  File createRbwFile(String bpid, Block b) throws IOException {
    return getBlockPoolSlice(bpid).createRbwFile(b);
  }

  File addBlock(String bpid, Block b, File f) throws IOException {
    File blockFile =  getBlockPoolSlice(bpid).addBlock(b, f);
    File metaFile = FsDatasetUtil.getMetaFile(blockFile, b.getGenerationStamp());
    try {
      this.getVolumeUsage().incDfsUsed(b.getNumBytes() + metaFile.length());
    } catch (IOException e) {
      LOG.error("ERROR when get volumn DU", e);
    }
    return blockFile;
    return getBlockPoolSlice(bpid).addBlock(b, f);
  }

  Executor getCacheExecutor() {
    return cacheExecutor;
  }

  void checkDirs() throws DiskErrorException {
    // TODO:FEDERATION valid synchronization
    for(BlockPoolSlice s : bpSlices.values()) {
      s.checkDirs();
    }
  }
    
  void getVolumeMap(ReplicaMap volumeMap) throws IOException {
    for(BlockPoolSlice s : bpSlices.values()) {
      s.getVolumeMap(volumeMap);
    }
  }
  
  void getVolumeMap(String bpid, ReplicaMap volumeMap) throws IOException {
    getBlockPoolSlice(bpid).getVolumeMap(volumeMap);
  }
  
  /**
   * Add replicas under the given directory to the volume map
   * @param volumeMap the replicas map
   * @param dir an input directory
   * @param isFinalized true if the directory has finalized replicas;
   *                    false if the directory has rbw replicas
   * @throws IOException 
   */
  void addToReplicasMap(String bpid, ReplicaMap volumeMap, 
      File dir, boolean isFinalized) throws IOException {
    BlockPoolSlice bp = getBlockPoolSlice(bpid);
    // TODO move this up
    // dfsUsage.incDfsUsed(b.getNumBytes()+metaFile.length());
    bp.addToReplicasMap(volumeMap, dir, isFinalized);
  }

  @Override
  public String toString() {
    return currentDir.getAbsolutePath();
  }

  void shutdown() {
    if (cacheExecutor != null) {
      cacheExecutor.shutdown();
    }
    saveDfsUsed();
    dfsUsedSaved = true;
    Set<Entry<String, BlockPoolSlice>> set = bpSlices.entrySet();
    for (Entry<String, BlockPoolSlice> entry : set) {
      entry.getValue().shutdown();
    }
    if (volumeUsage != null) {
      volumeUsage.shutdown();
    }
  }

  void addBlockPool(String bpid, Configuration conf) throws IOException {
    File bpdir = new File(currentDir, bpid);
    BlockPoolSlice bp = new BlockPoolSlice(bpid, this, bpdir, conf);
    bpSlices.put(bpid, bp);
  }
  
  void shutdownBlockPool(String bpid) {
    BlockPoolSlice bp = bpSlices.get(bpid);
    if (bp != null) {
      try {
        volumeUsage.decDfsUsed(bp.getDfsUsed());
      } catch (IOException e) {
        LOG.error("ERROR when gdec volumn DU", e);
      }
    }
    bpSlices.remove(bpid);
    volumeUsage.recompute();
  }

  boolean isBPDirEmpty(String bpid) throws IOException {
    File volumeCurrentDir = this.getCurrentDir();
    File bpDir = new File(volumeCurrentDir, bpid);
    File bpCurrentDir = new File(bpDir, DataStorage.STORAGE_DIR_CURRENT);
    File finalizedDir = new File(bpCurrentDir,
        DataStorage.STORAGE_DIR_FINALIZED);
    File rbwDir = new File(bpCurrentDir, DataStorage.STORAGE_DIR_RBW);
    if (finalizedDir.exists() && !DatanodeUtil.dirNoFilesRecursive(
        finalizedDir)) {
      return false;
    }
    if (rbwDir.exists() && FileUtil.list(rbwDir).length != 0) {
      return false;
    }
    return true;
  }
  
  void deleteBPDirectories(String bpid, boolean force) throws IOException {
    File volumeCurrentDir = this.getCurrentDir();
    File bpDir = new File(volumeCurrentDir, bpid);
    if (!bpDir.isDirectory()) {
      // nothing to be deleted
      return;
    }
    File tmpDir = new File(bpDir, DataStorage.STORAGE_DIR_TMP); 
    File bpCurrentDir = new File(bpDir, DataStorage.STORAGE_DIR_CURRENT);
    File finalizedDir = new File(bpCurrentDir,
        DataStorage.STORAGE_DIR_FINALIZED);
    File rbwDir = new File(bpCurrentDir, DataStorage.STORAGE_DIR_RBW);
    if (force) {
      FileUtil.fullyDelete(bpDir);
    } else {
      if (!rbwDir.delete()) {
        throw new IOException("Failed to delete " + rbwDir);
      }
      if (!DatanodeUtil.dirNoFilesRecursive(finalizedDir) ||
          !FileUtil.fullyDelete(finalizedDir)) {
        throw new IOException("Failed to delete " + finalizedDir);
      }
      FileUtil.fullyDelete(tmpDir);
      for (File f : FileUtil.listFiles(bpCurrentDir)) {
        if (!f.delete()) {
          throw new IOException("Failed to delete " + f);
        }
      }
      if (!bpCurrentDir.delete()) {
        throw new IOException("Failed to delete " + bpCurrentDir);
      }
      for (File f : FileUtil.listFiles(bpDir)) {
        if (!f.delete()) {
          throw new IOException("Failed to delete " + f);
        }
      }
      if (!bpDir.delete()) {
        throw new IOException("Failed to delete " + bpDir);
      }
    }
  }

  @Override
  public String getStorageID() {
    return storageID;
  }
  
  @Override
  public StorageType getStorageType() {
    return storageType;
  }
  
  DatanodeStorage toDatanodeStorage() {
    return new DatanodeStorage(storageID, DatanodeStorage.State.NORMAL, storageType);
  }

}

