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

package org.apache.hadoop.raid;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.EnumSet;
import java.util.concurrent.TimeUnit;
import java.util.Map;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SocketChannel;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.StorageType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.net.NetUtils;

import org.apache.hadoop.raid.RaidNode;
import org.apache.hadoop.raid.RaidUtils;

import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.ipc.RPC;

import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.datanode.BlockMetadataHeader;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage;

import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.hdfs.protocolPB.NamenodeProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.NamenodeProtocolTranslatorPB;

/**
 * contains the core functionality of the block fixer
 *
 * configuration options:
 * raid.blockfix.classname         - the class name of the block fixer 
 *                                   implementation to use
 *
 * raid.blockfix.interval          - interval between checks for corrupt files
 *
 * raid.blockfix.history.interval  - interval before fixing same file again
 *
 * raid.blockfix.read.timeout      - read time out
 *
 * raid.blockfix.write.timeout     - write time out
 */
public abstract class BlockFixer extends Configured implements Runnable {

  public static final Log LOG = LogFactory.getLog(BlockFixer.class);
  public static final String BLOCKFIX_CLASSNAME = "raid.blockfix.classname"; 
  public static final String BLOCKFIX_INTERVAL = "raid.blockfix.interval";
  public static final String BLOCKFIX_HISTORY_INTERVAL =
    "raid.blockfix.history.interval";
  public static final String BLOCKFIX_READ_TIMEOUT =
    "raid.blockfix.read.timeout";
  public static final String BLOCKFIX_WRITE_TIMEOUT =
    "raid.blockfix.write.timeout";

  public static final long DEFAULT_BLOCKFIX_INTERVAL = 60 * 1000; // 1 min
  public static final long DEFAULT_BLOCKFIX_HISTORY_INTERVAL =
    60 * 60 * 1000; // 60 mins

  public static BlockFixer createBlockFixer(Configuration conf)
    throws ClassNotFoundException {
    try {
      // default to distributed block fixer
      Class<?> blockFixerClass =
        conf.getClass(BLOCKFIX_CLASSNAME, DistBlockFixer.class);
      if (!BlockFixer.class.isAssignableFrom(blockFixerClass)) {
        throw new ClassNotFoundException("not an implementation of blockfixer");
      }
      Constructor<?> constructor =
        blockFixerClass.getConstructor(new Class[] {Configuration.class} );
      return (BlockFixer) constructor.newInstance(conf);
    } catch (NoSuchMethodException e) {
      throw new ClassNotFoundException("cannot construct blockfixer", e);
    } catch (InstantiationException e) {
      throw new ClassNotFoundException("cannot construct blockfixer", e);
    } catch (IllegalAccessException e) {
      throw new ClassNotFoundException("cannot construct blockfixer", e);
    } catch (InvocationTargetException e) {
      throw new ClassNotFoundException("cannot construct blockfixer", e);
    }
  }

  private long numFilesFixed = 0;
  public volatile boolean running = true;

  Configuration conf;

  // interval between checks for corrupt files
  protected long blockFixInterval;

  // interval between kerberos ticket renew
  final protected long ticketRenewInterval = 600 * 1000;

  static final byte OP_WRITE_BLOCK = (byte) 80;

  static String encryptionAlgorithm; 

  protected long ticketRenewTime;

  // interval before fixing same file again
  protected long historyInterval;

  public BlockFixer(Configuration conf) {
    super(conf);

	encryptionAlgorithm = conf.get(DFSConfigKeys.DFS_DATA_ENCRYPTION_ALGORITHM_KEY);

    try {
      this.conf = conf;
      securityLogin(conf);

      blockFixInterval =
        getConf().getLong(BLOCKFIX_INTERVAL, DEFAULT_BLOCKFIX_INTERVAL);
      historyInterval =
        getConf().getLong(BLOCKFIX_HISTORY_INTERVAL,
                        DEFAULT_BLOCKFIX_HISTORY_INTERVAL);

      NamenodeProtocol namenodeProtocol = null;
      ExportedBlockKeys keys = null;
      if(!FED_HAUtil.checkIfViewFileSystem(conf)) {
    	  namenodeProtocol = createNamenode(conf);
    	  keys = namenodeProtocol.getBlockKeys();      
      
          this.isBlockTokenEnabled = keys.isBlockTokenEnabled();
          long blockKeyUpdateInterval = keys.getKeyUpdateInterval();  
          long blockTokenLifetime = keys.getTokenLifetime();
          LOG.info("Block token params received from NN: keyUpdateInterval="
            + blockKeyUpdateInterval / (60 * 1000) + " min(s), tokenLifetime="
            + blockTokenLifetime / (60 * 1000) + " min(s)");
          blockTokenSecretManager = new BlockTokenSecretManager(blockKeyUpdateInterval, 
			  blockTokenLifetime, null, encryptionAlgorithm);
          blockTokenSecretManager.addKeys(keys);
      } else {
    	  for(String hdfsUriStr : FED_HAUtil.getDfsNameServices(conf)) {
    		  Configuration hdfsConf = conf;    		  
    		  hdfsConf.set("dfs.nameservices", hdfsUriStr);    		  
    		  InetSocketAddress nameNodeAddr = FED_HAUtil.getAvailableNNAddrFromServiceName(hdfsUriStr, hdfsConf);
    		  if(nameNodeAddr == null) {
    			  throw new IOException("can not get available nn addr for " + hdfsUriStr + ".");
    		  }
    		  System.out.println("xxx nameNodeAddr:" + nameNodeAddr.toString() + "\thdfsUriStr:" + hdfsUriStr);
    		  namenodeProtocol = createNamenode(nameNodeAddr, hdfsConf);
    		  keys = namenodeProtocol.getBlockKeys();      
      
    		  if(blockTokenSecretManager == null) {
    			  this.isBlockTokenEnabled = keys.isBlockTokenEnabled();
    			  long blockKeyUpdateInterval = keys.getKeyUpdateInterval();  
    			  long blockTokenLifetime = keys.getTokenLifetime();
    			  LOG.info("Block token params received from NN: keyUpdateInterval="
    			      + blockKeyUpdateInterval / (60 * 1000) + " min(s), tokenLifetime="
                      + blockTokenLifetime / (60 * 1000) + " min(s)");    		  
                  blockTokenSecretManager = new BlockTokenSecretManager(blockKeyUpdateInterval, 
			         blockTokenLifetime, null, encryptionAlgorithm);
    		  }
              blockTokenSecretManager.addKeys(keys);
    	  }
      }
    } catch (Exception e) {
      e.printStackTrace();
      System.err.println("[Error] Exception happened in BlockFixe contruction " + e);
    }
  }

  protected static BlockTokenSecretManager blockTokenSecretManager;
  protected static boolean isBlockTokenEnabled;

  @Override
  public abstract void run();

  /**
   * returns the number of files that have been fixed by this block fixer
   */
  public synchronized long filesFixed() {
    return numFilesFixed;
  }

  /**
   * increments the number of files that have been fixed by this block fixer
   */
  protected synchronized void incrFilesFixed() {
    numFilesFixed++;
  }

  /**
   * increments the number of files that have been fixed by this block fixer
   */
  protected synchronized void incrFilesFixed(long incr) {
    if (incr < 0) {
      throw new IllegalArgumentException("cannot increment by negative value " +
                                         incr);
    }
    
    numFilesFixed += incr;
  }

  static boolean isSourceFile(Path p, String[] destPrefixes) {
    String pathStr = p.toUri().getPath();
    for (String destPrefix: destPrefixes) {
      if (pathStr.startsWith(destPrefix)) {
        return false;
      }
    }
    return true;
  }

  void filterUnfixableSourceFiles(Iterator<Path> it) throws IOException {
	Configuration conf = new Configuration();  
    String xorPrefix = RaidNode.xorDestinationPath(conf).toUri().getPath();
    if (!xorPrefix.endsWith(Path.SEPARATOR)) {
      xorPrefix += Path.SEPARATOR;
    }
    String rsPrefix = RaidNode.rsDestinationPath(conf).toUri().getPath();
    if (!rsPrefix.endsWith(Path.SEPARATOR)) {
      rsPrefix += Path.SEPARATOR;
    }
    String[] destPrefixes = new String[]{xorPrefix, rsPrefix};
    while (it.hasNext()) {
      Path p = it.next();
      if (isSourceFile(p, destPrefixes) &&
          RaidNode.xorParityForSource(p, conf) == null &&
          RaidNode.rsParityForSource(p, conf) == null) {
        it.remove();
      }
    }
  }

  /* Build a NamenodeProtocol connection to the namenode and
   * set up the retry policy */ 
  protected static NamenodeProtocol createNamenode(Configuration conf)
    throws IOException {
    InetSocketAddress nameNodeAddr = NameNode.getServiceAddress(conf, true);    
    return createNamenode(nameNodeAddr, conf);    
  }
  
  /* Build a NamenodeProtocol connection to the namenode and
   * set up the retry policy */ 
  protected static NamenodeProtocol createNamenode(InetSocketAddress nameNodeAddr, Configuration conf)
    throws IOException {
    RetryPolicy timeoutPolicy = RetryPolicies.exponentialBackoffRetry(
      5, 200, TimeUnit.MILLISECONDS);
    Map<Class<? extends Exception>,RetryPolicy> exceptionToPolicyMap =
    new HashMap<Class<? extends Exception>, RetryPolicy>();
    RetryPolicy methodPolicy = RetryPolicies.retryByException(
      timeoutPolicy, exceptionToPolicyMap);
    Map<String,RetryPolicy> methodNameToPolicyMap =
      new HashMap<String, RetryPolicy>();
    methodNameToPolicyMap.put("getBlocks", methodPolicy);
    methodNameToPolicyMap.put("getAccessKeys", methodPolicy);

    UserGroupInformation ugi = UserGroupInformation.getLoginUser();
	RPC.setProtocolEngine(conf, NamenodeProtocolPB.class, ProtobufRpcEngine.class);
	NamenodeProtocolPB proxy = (NamenodeProtocolPB)RPC.getProxy(NamenodeProtocolPB.class, 
      RPC.getProtocolVersion(NamenodeProtocolPB.class), nameNodeAddr, ugi, conf, 
	  NetUtils.getDefaultSocketFactory(conf));
    proxy = (NamenodeProtocolPB) RetryProxy.create(NamenodeProtocolPB.class,
      proxy, methodNameToPolicyMap);
    return new NamenodeProtocolTranslatorPB(proxy);
  }

  protected static void securityLogin(Configuration conf) throws IOException {
    if (UserGroupInformation.isSecurityEnabled()) {
      String raidBlockfixerKeyTabPath = conf.get("raid.blockfixer.keytab.file", "/usr/lib/hadoop/conf/hdfs.keytab");
      String raidBlockfixerUserName = conf.get("raid.blockfixer.user.name", "hdfs");
      UserGroupInformation.loginUserFromKeytab(raidBlockfixerUserName, raidBlockfixerKeyTabPath);
    }
  }

  protected static void initBlockTokenSecretManager(Configuration conf, String path) throws IOException {	    
	NamenodeProtocol namenodeProtocol = null;
	ExportedBlockKeys keys = null;
	if(!FED_HAUtil.checkIfViewFileSystem(conf)) {
	  namenodeProtocol = createNamenode(conf);
	  keys = namenodeProtocol.getBlockKeys();      
	      
	  isBlockTokenEnabled = keys.isBlockTokenEnabled();
	  long blockKeyUpdateInterval = keys.getKeyUpdateInterval();  
	  long blockTokenLifetime = keys.getTokenLifetime();
	  LOG.info("Block token params received from NN: keyUpdateInterval="
	      + blockKeyUpdateInterval / (60 * 1000) + " min(s), tokenLifetime="
	      + blockTokenLifetime / (60 * 1000) + " min(s)");
	  blockTokenSecretManager = new BlockTokenSecretManager(blockKeyUpdateInterval, 
		  blockTokenLifetime, null, encryptionAlgorithm);
	  blockTokenSecretManager.addKeys(keys);
	} else {
	  for(String hdfsUriStr : FED_HAUtil.getDfsNameServices(conf)) {
	    Configuration hdfsConf = conf;    		  
	    hdfsConf.set("dfs.nameservices", hdfsUriStr);
	    InetSocketAddress nameNodeAddr = FED_HAUtil.getAvailableNNAddrFromServiceName(hdfsUriStr, hdfsConf, path);
	    if(nameNodeAddr == null) {
	      System.out.println("xxxx servicename:" + hdfsUriStr + " is not available\tpath:" + path);	
	  	  continue;
	    }
	    System.out.println("xxx nameNodeAddr:" + nameNodeAddr.toString() + "\thdfsUriStr:" + hdfsUriStr);
	    namenodeProtocol = createNamenode(nameNodeAddr, hdfsConf);
	    keys = namenodeProtocol.getBlockKeys();      
	     
	    isBlockTokenEnabled = keys.isBlockTokenEnabled();
	    long blockKeyUpdateInterval = keys.getKeyUpdateInterval();  
	    long blockTokenLifetime = keys.getTokenLifetime();
	    LOG.info("Block token params received from NN: keyUpdateInterval="
	      + blockKeyUpdateInterval / (60 * 1000) + " min(s), tokenLifetime="
	      + blockTokenLifetime / (60 * 1000) + " min(s)");    		  
	    blockTokenSecretManager = new BlockTokenSecretManager(blockKeyUpdateInterval, 
		      blockTokenLifetime, null, encryptionAlgorithm);
	    blockTokenSecretManager.addKeys(keys);
	    System.out.println("xxxx servicename:" + hdfsUriStr + "\tpath:" + path);
	  }
	}
  }
  
  /**
   * this class implements the actual fixing functionality
   * we keep this in a separate class so that 
   * the distributed block fixer can use it
   */ 
  static class BlockFixerHelper extends Configured {
    public static final Log LOG = LogFactory.getLog(BlockFixer.
        BlockFixerHelper.class);

    private String xorPrefix;
    private String rsPrefix;
    private XOREncoder xorEncoder;
    private XORDecoder xorDecoder;
    private ReedSolomonEncoder rsEncoder;
    private ReedSolomonDecoder rsDecoder;

    public BlockFixerHelper(Configuration conf) throws IOException {
      super(conf);

      xorPrefix = RaidNode.xorDestinationPath(getConf()).toUri().getPath();
      if (!xorPrefix.endsWith(Path.SEPARATOR)) {
        xorPrefix += Path.SEPARATOR;
      }
      rsPrefix = RaidNode.rsDestinationPath(getConf()).toUri().getPath();
      if (!rsPrefix.endsWith(Path.SEPARATOR)) {
        rsPrefix += Path.SEPARATOR;
      }
      int stripeLength = RaidNode.getStripeLength(getConf());
      xorEncoder = new XOREncoder(getConf(), stripeLength);
      xorDecoder = new XORDecoder(getConf(), stripeLength);
      int parityLength = RaidNode.rsParityLength(getConf());
      rsEncoder = new ReedSolomonEncoder(getConf(), stripeLength, parityLength);
      rsDecoder = new ReedSolomonDecoder(getConf(), stripeLength, parityLength);
    }

    /**
     * checks whether file is xor parity file
     */
    boolean isXorParityFile(Path p) {
      String pathStr = p.toUri().getPath();
      if (pathStr.contains(RaidNode.HAR_SUFFIX)) {
        return false;
      }
      return pathStr.startsWith(xorPrefix);
    }

   /**
     * checks whether file is rs parity file
     */
    boolean isRsParityFile(Path p) {
      String pathStr = p.toUri().getPath();
      if (pathStr.contains(RaidNode.HAR_SUFFIX)) {
        return false;
      }
      return pathStr.startsWith(rsPrefix);
    }
   
   /**
     * Fix a file, do not report progess.
     *
     * @return true if file has been fixed, false if no fixing 
     * was necessary or possible.
     */
    boolean fixFile(Path srcPath) throws IOException {
      return fixFile(srcPath, new RaidUtils.DummyProgressable());
    }

   /**
     * Fix a file, report progess.
     *
     * @return true if file has been fixed, false if no fixing 
     * was necessary or possible.
     */
    boolean fixFile(Path srcPath, Progressable progress) throws IOException {	
      BlockFixer.initBlockTokenSecretManager(new Configuration(), FED_HAUtil.removeFSPrefix(srcPath, FED_HAUtil.VIEW_FS_PREFIX));	
    	
      if (RaidNode.isParityHarPartFile(srcPath)) {
        return processCorruptParityHarPartFile(srcPath, progress);
      }

      // The corrupted file is a XOR parity file
      if (isXorParityFile(srcPath)) {
        return processCorruptParityFile(srcPath, xorEncoder, progress);
      }
    
      // The corrupted file is a ReedSolomon parity file
      if (isRsParityFile(srcPath)) {
        return processCorruptParityFile(srcPath, rsEncoder, progress);
      }

      // The corrupted file is a source file
      RaidNode.ParityFilePair ppair =
        RaidNode.xorParityForSource(srcPath, getConf());
      Decoder decoder = null;
      if (ppair != null) {
        decoder = xorDecoder;
      } else  {
        ppair = RaidNode.rsParityForSource(srcPath, getConf());
        if (ppair != null) {
          decoder = rsDecoder;
        }
      }

      // If we have a parity file, process the file and fix it.
      if (ppair != null) {
        return processCorruptFile(srcPath, ppair, decoder, progress);
      } 

      // there was nothing to do
      return false;
    }
    
    /**
     * Sorts source files ahead of parity files.
     */
    void sortCorruptFiles(List<Path> files) {
      //  TODO: We should first fix the files that lose more blocks
      Comparator<Path> comp = new Comparator<Path>() {
        public int compare(Path p1, Path p2) {
          if (isXorParityFile(p2) || isRsParityFile(p2)) {
            // If p2 is a parity file, p1 is smaller.
            return -1;
          }
          if (isXorParityFile(p1) || isRsParityFile(p1)) {
            // If p1 is a parity file, p2 is smaller.
            return 1;
          }
          // If both are source files, they are equal.
          return 0;
        }
      };
      Collections.sort(files, comp);
    }
      
    /**
      * Returns a DistributedFileSystem hosting the path supplied.
      */
    protected DistributedFileSystem getDFS(Path p) throws IOException {
      FileSystem fs = p.getFileSystem(new Configuration());
      if(fs instanceof org.apache.hadoop.fs.viewfs.ViewFileSystem) {
    	for(FileSystem fss : fs.getChildFileSystems()) {
    		if(fss.exists(p)) {
    		  return (DistributedFileSystem)fss;	
    		}
    	}
      } else if(fs instanceof DistributedFileSystem) {
          return (DistributedFileSystem)fs;
      } else {
    	  throw new IOException("can not get DistributedFileSystem from path " + p.toString());
      }
      return null;
    }
    

   /**
     * Reads through a corrupt source file fixing corrupt blocks on the way.
     * @param srcPath Path identifying the corrupt file.
     * @throws IOException
     * @return true if file has been fixed, false if no fixing 
     * was necessary or possible.
     */
    boolean processCorruptFile(Path srcPath, RaidNode.ParityFilePair parityPair,
                               Decoder decoder, Progressable progress)
      throws IOException {
          LOG.info("Processing corrupt file " + srcPath);
      DistributedFileSystem srcFs = getDFS(srcPath);
      FileStatus srcStat = srcFs.getFileStatus(srcPath);
      long blockSize = srcStat.getBlockSize();
      long srcFileSize = srcStat.getLen();
      String uriPath = srcPath.toUri().getPath();
      
      int numBlocksFixed = 0;
      List<LocatedBlock> corrupt =
        RaidDFSUtil.corruptBlocksInFile(srcFs, uriPath, 0, srcFileSize);
      if (corrupt.size() == 0) {
        return false;
      }
      for (LocatedBlock lb: corrupt) {
        Block corruptBlock = lb.getBlock().getLocalBlock();
        long corruptOffset = lb.getStartOffset();
        
        LOG.info("Found corrupt block " + corruptBlock +
                 ", offset " + corruptOffset);
        
        final long blockContentsSize =
          Math.min(blockSize, srcFileSize - corruptOffset);
        File localBlockFile =
          File.createTempFile(corruptBlock.getBlockName(), ".tmp");
        localBlockFile.deleteOnExit();
        
        try {
          decoder.recoverBlockToFile(srcFs, srcPath, parityPair.getFileSystem(),
                                     parityPair.getPath(), blockSize,
                                     corruptOffset, localBlockFile,
                                     blockContentsSize);
          
          // We have a the contents of the block, send them.
          DatanodeInfo datanode = chooseDatanode(lb.getLocations());
          computeMetadataAndSendFixedBlock(datanode, localBlockFile,
                                          lb, blockContentsSize);
          numBlocksFixed++;
        } finally {
          localBlockFile.delete();
        }
        progress.progress();
      }
      LOG.info("Fixed " + numBlocksFixed + " blocks in " + srcPath);
      return true;
    }

   /**
     * Fixes corrupt blocks in a parity file.
     * This function uses the corresponding source file to regenerate parity
     * file blocks.
     * @return true if file has been fixed, false if no fixing 
     * was necessary or possible.
     */
    boolean processCorruptParityFile(Path parityPath, Encoder encoder,
                                     Progressable progress)
      throws IOException {
      LOG.info("Processing corrupt file " + parityPath);
      Path srcPath = sourcePathFromParityPath(parityPath);
      if (srcPath == null) {
        LOG.warn("Unusable parity file " + parityPath);
        return false;
      }

      DistributedFileSystem parityFs = getDFS(parityPath);
      FileStatus parityStat = parityFs.getFileStatus(parityPath);
      long blockSize = parityStat.getBlockSize();
      long parityFileSize = parityStat.getLen();
      FileStatus srcStat = getDFS(srcPath).getFileStatus(srcPath);
      long srcFileSize = srcStat.getLen();

          // Check timestamp.
      if (srcStat.getModificationTime() != parityStat.getModificationTime()) {
        LOG.info("Mismatching timestamp for " + srcPath + " and " + parityPath +
                 ", moving on...");
        return false;
      }

      String uriPath = parityPath.toUri().getPath();
      int numBlocksFixed = 0;
      List<LocatedBlock> corrupt =
        RaidDFSUtil.corruptBlocksInFile(parityFs, uriPath, 0, parityFileSize);
      if (corrupt.size() == 0) {
        return false;
      }
      for (LocatedBlock lb: corrupt) {
        Block corruptBlock = lb.getBlock().getLocalBlock();
        long corruptOffset = lb.getStartOffset();
        
        LOG.info("Found corrupt block " + corruptBlock +
                 ", offset " + corruptOffset);
        
        File localBlockFile =
          File.createTempFile(corruptBlock.getBlockName(), ".tmp");
        localBlockFile.deleteOnExit();
        
        try {
          encoder.recoverParityBlockToFile(parityFs, srcPath, srcFileSize,
                                           blockSize, parityPath,
                                           corruptOffset, localBlockFile);
          // We have a the contents of the block, send them.
          DatanodeInfo datanode = chooseDatanode(lb.getLocations());
          computeMetadataAndSendFixedBlock(datanode, localBlockFile, lb,
                                          blockSize);
          
          numBlocksFixed++;
        } finally {
          localBlockFile.delete();
        }
        progress.progress();
      }
      LOG.info("Fixed " + numBlocksFixed + " blocks in " + parityPath);
      return true;
    }

   /**
     * Reads through a parity HAR part file, fixing corrupt blocks on the way.
     * A HAR block can contain many file blocks, as long as the HAR part file
     * block size is a multiple of the file block size.
     * @return true if file has been fixed, false if no fixing 
     * was necessary or possible.
     */
    boolean processCorruptParityHarPartFile(Path partFile,
                                            Progressable progress)
      throws IOException {
      LOG.info("Processing corrupt file " + partFile);
      // Get some basic information.
      DistributedFileSystem dfs = getDFS(partFile);
      FileStatus partFileStat = dfs.getFileStatus(partFile);
      long partFileSize = partFileStat.getLen();
      long partFileBlockSize = partFileStat.getBlockSize();
      LOG.info(partFile + " has block size " + partFileBlockSize);

      // Find the path to the index file.
      // Parity file HARs are only one level deep, so the index files is at the
      // same level as the part file.
      String harDirectory = partFile.toUri().getPath(); // Temporarily.
      harDirectory =
        harDirectory.substring(0, harDirectory.lastIndexOf(Path.SEPARATOR));
      Path indexFile = new Path(harDirectory + "/" + HarIndex.indexFileName);
      FileStatus indexStat = dfs.getFileStatus(indexFile);
      // Parses through the HAR index file.
      HarIndex harIndex = new HarIndex(dfs.open(indexFile), indexStat.getLen());

      String uriPath = partFile.toUri().getPath();
      int numBlocksFixed = 0;
      List<LocatedBlock> corrupt =
        RaidDFSUtil.corruptBlocksInFile(dfs, uriPath, 0, partFileSize);
      if (corrupt.size() == 0) {
        return false;
      }
      for (LocatedBlock lb: corrupt) {
        Block corruptBlock = lb.getBlock().getLocalBlock();
        long corruptOffset = lb.getStartOffset();

        File localBlockFile =
          File.createTempFile(corruptBlock.getBlockName(), ".tmp");
        localBlockFile.deleteOnExit();
        processCorruptParityHarPartBlock(dfs, partFile, corruptBlock,
                                         corruptOffset, partFileStat, harIndex,
                                         localBlockFile, progress);
        // Now we have recovered the part file block locally, send it.
        try {
          DatanodeInfo datanode = chooseDatanode(lb.getLocations());
          computeMetadataAndSendFixedBlock(datanode, localBlockFile,
                                          lb, localBlockFile.length());
          numBlocksFixed++;
        } finally {
          localBlockFile.delete();
        }
        progress.progress();
      }
      LOG.info("Fixed " + numBlocksFixed + " blocks in " + partFile);
      return true;
    }
    

   /**
     * This fixes a single part file block by recovering in sequence each
     * parity block in the part file block.
     */
    private void processCorruptParityHarPartBlock(FileSystem dfs, Path partFile,
                                                  Block corruptBlock,
                                                  long corruptOffset,
                                                  FileStatus partFileStat,
                                                  HarIndex harIndex,
                                                  File localBlockFile,
                                                  Progressable progress)
      throws IOException {
      String partName = partFile.toUri().getPath(); // Temporarily.
      partName = partName.substring(1 + partName.lastIndexOf(Path.SEPARATOR));

      OutputStream out = new FileOutputStream(localBlockFile);
      try {
        // A HAR part file block could map to several parity files. We need to
        // use all of them to recover this block.
        final long corruptEnd = Math.min(corruptOffset +
                                         partFileStat.getBlockSize(),
                                         partFileStat.getLen());
        for (long offset = corruptOffset; offset < corruptEnd; ) {
          HarIndex.IndexEntry entry = harIndex.findEntry(partName, offset);
          if (entry == null) {
            String msg = "Corrupt index file has no matching index entry for " +
              partName + ":" + offset;
            LOG.warn(msg);
            throw new IOException(msg);
          }
          Path parityFile = new Path(entry.fileName);
          Encoder encoder;
          if (isXorParityFile(parityFile)) {
            encoder = xorEncoder;
          } else if (isRsParityFile(parityFile)) {
            encoder = rsEncoder;
          } else {
            String msg = "Could not figure out parity file correctly";
            LOG.warn(msg);
            throw new IOException(msg);
          }
          Path srcFile = sourcePathFromParityPath(parityFile);
          FileStatus srcStat = dfs.getFileStatus(srcFile);
          if (srcStat.getModificationTime() != entry.mtime) {
            String msg = "Modification times of " + parityFile + " and " +
              srcFile + " do not match.";
            LOG.warn(msg);
            throw new IOException(msg);
          }
          long corruptOffsetInParity = offset - entry.startOffset;
          LOG.info(partFile + ":" + offset + " maps to " +
                   parityFile + ":" + corruptOffsetInParity +
                   " and will be recovered from " + srcFile);
          encoder.recoverParityBlockToStream(dfs, srcFile, srcStat.getLen(),
                                             srcStat.getBlockSize(), parityFile,
                                             corruptOffsetInParity, out);
          // Finished recovery of one parity block. Since a parity block has the
          // same size as a source block, we can move offset by source block size.
          offset += srcStat.getBlockSize();
          LOG.info("Recovered " + srcStat.getBlockSize() + " part file bytes ");
          if (offset > corruptEnd) {
            String msg =
              "Recovered block spills across part file blocks. Cannot continue.";
            throw new IOException(msg);
          }
          progress.progress();
        }
      } finally {
        out.close();
      }
    }
    

   /**
     * Choose a datanode (hostname:portnumber). The datanode is chosen at
     * random from the live datanodes.
     * @param locationsToAvoid locations to avoid.
     * @return A datanode
     * @throws IOException
     */
    private DatanodeInfo chooseDatanode(DatanodeInfo[] locationsToAvoid)
      throws IOException {
      DistributedFileSystem dfs = getDFS(new Path("/"));
      DatanodeInfo[] live =
        dfs.getClient().datanodeReport(DatanodeReportType.LIVE);
      LOG.info("Choosing a datanode from " + live.length +
               " live nodes while avoiding " + locationsToAvoid.length);
      Random rand = new Random();
      DatanodeInfo chosen = null;
      int maxAttempts = 1000;
      for (int i = 0; i < maxAttempts && chosen == null; i++) {
        int idx = rand.nextInt(live.length);
        chosen = live[idx];
        for (DatanodeInfo avoid: locationsToAvoid) {
          if (chosen.getDatanodeUuid().equals(avoid.getDatanodeUuid())) {
            LOG.info("Avoiding " + avoid.getDatanodeUuid());
            chosen = null;
            break;
          }
        }
      }
      if (chosen == null) {
        throw new IOException("Could not choose datanode");
      }
      LOG.info("Choosing datanode " + chosen.getDatanodeUuid());
      return chosen;
    }
  
   /**
     * Reads data from the data stream provided and computes metadata.
     */
    static DataInputStream computeMetadata(Configuration conf,
                                           InputStream dataStream)
      throws IOException {
      ByteArrayOutputStream mdOutBase = new ByteArrayOutputStream(1024*1024);
      DataOutputStream mdOut = new DataOutputStream(mdOutBase);
      
      // First, write out the version.
      mdOut.writeShort(BlockMetadataHeader.VERSION);
      
      // Create a summer and write out its header.
      int bytesPerChecksum = conf.getInt("io.bytes.per.checksum", 512);
      DataChecksum sum =
        DataChecksum.newDataChecksum(DataChecksum.Type.valueOf(DataChecksum.CHECKSUM_CRC32),
                                     bytesPerChecksum);
      sum.writeHeader(mdOut);
      
      // Buffer to read in a chunk of data.
      byte[] buf = new byte[bytesPerChecksum];
      // Buffer to store the checksum bytes.
      byte[] chk = new byte[sum.getChecksumSize()];
      
      // Read data till we reach the end of the input stream.
      int bytesSinceFlush = 0;
      while (true) {
        // Read some bytes.
        int bytesRead = dataStream.read(buf, bytesSinceFlush,
                                        bytesPerChecksum-bytesSinceFlush);
        if (bytesRead == -1) {
          if (bytesSinceFlush > 0) {
            boolean reset = true;
            sum.writeValue(chk, 0, reset); // This also resets the sum.
            // Write the checksum to the stream.
            mdOut.write(chk, 0, chk.length);
            bytesSinceFlush = 0;
          }
          break;
        }
        // Update the checksum.
        sum.update(buf, bytesSinceFlush, bytesRead);
        bytesSinceFlush += bytesRead;
        
        // Flush the checksum if necessary.
        if (bytesSinceFlush == bytesPerChecksum) {
          boolean reset = true;
          sum.writeValue(chk, 0, reset); // This also resets the sum.
          // Write the checksum to the stream.
          mdOut.write(chk, 0, chk.length);
          bytesSinceFlush = 0;
        }
      }

      byte[] mdBytes = mdOutBase.toByteArray();
      return new DataInputStream(new ByteArrayInputStream(mdBytes));
    }

    private void computeMetadataAndSendFixedBlock(DatanodeInfo datanode,
                                                  File localBlockFile,
                                                  LocatedBlock block,
                                                  long blockSize)
      throws IOException {
      LOG.info("Computing metdata");
      InputStream blockContents = null;
      DataInputStream blockMetadata = null;
      try {
        blockContents = new FileInputStream(localBlockFile);
        blockMetadata = computeMetadata(getConf(), blockContents);
        blockContents.close();
        // Reopen
        blockContents = new FileInputStream(localBlockFile);
        sendFixedBlock(datanode, blockContents, blockMetadata, block,
                       blockSize);
      } finally {
        if (blockContents != null) {
          blockContents.close();
          blockContents = null;
        }
        if (blockMetadata != null) {
          blockMetadata.close();
          blockMetadata = null;
        }
      }
    }

    /**
     * Send a generated block to a datanode.
     * @param datanode Chosen datanode name in host:port form.
     * @param blockContents Stream with the block contents.
     * @param corruptBlock Block identifying the block to be sent.
     * @param blockSize size of the block.
     * @throws IOException
     */
    private void sendFixedBlock(DatanodeInfo datanode,
                                final InputStream blockContents,
                                DataInputStream metadataIn,
                                LocatedBlock block, long blockSize)
      throws IOException {
      InetSocketAddress target = NetUtils.createSocketAddr(datanode.getXferAddr());
      Socket sock = SocketChannel.open().socket();
      
      int readTimeout =
        getConf().getInt(BLOCKFIX_READ_TIMEOUT,
                         HdfsServerConstants.READ_TIMEOUT);
      NetUtils.connect(sock, target, readTimeout);
      sock.setSoTimeout(readTimeout);
      
      int writeTimeout = getConf().getInt(BLOCKFIX_WRITE_TIMEOUT,
                                          HdfsServerConstants.WRITE_TIMEOUT);
      
      OutputStream baseStream = NetUtils.getOutputStream(sock, writeTimeout);
      DataOutputStream out = 
        new DataOutputStream(new BufferedOutputStream(baseStream,
                                                      FSConstants.
                                                      SMALL_BUFFER_SIZE));
      
      boolean corruptChecksumOk = false;
      boolean verifyChecksum = true;
      boolean sendChecksum = verifyChecksum;
      boolean transferToAllowed = false;
      
      try {
        LOG.info("Sending block " + block.getBlock() +
                 " from " + sock.getLocalSocketAddress().toString() +
                 " to " + sock.getRemoteSocketAddress().toString() +
                 " " + blockSize + " bytes");

        RaidBlockSender blockSender =
          new RaidBlockSender(block.getBlock(), blockSize, 0, blockSize,
                              corruptChecksumOk, verifyChecksum, sendChecksum,
                              transferToAllowed, metadataIn,
                              new RaidBlockSender.InputStreamFactory() {
                                @Override
                                public InputStream
                                  createStream(long offset) throws IOException {
                                  // we are passing 0 as the offset above,
                                  // so we can safely ignore
                                  // the offset passed
                                  return blockContents;
                                }
                              });
        
       	DatanodeInfo srcNode = new DatanodeInfo(datanode);
		DatanodeInfo[] targets = new DatanodeInfo[]{datanode};
        Token<BlockTokenIdentifier> accessToken = BlockTokenSecretManager.DUMMY_TOKEN;
        if (isBlockTokenEnabled) {
          accessToken = blockTokenSecretManager.generateToken(null, block.getBlock(), 
            EnumSet.of(BlockTokenSecretManager.AccessMode.WRITE));
        }

		CachingStrategy cachingStrategy = new CachingStrategy(true, getConf().getLong(DFSConfigKeys.DFS_DATANODE_READAHEAD_BYTES_KEY,
							        DFSConfigKeys.DFS_DATANODE_READAHEAD_BYTES_DEFAULT));
		String clientname = getDFS(new Path("/")).getClient().getClientName();
		new Sender(out).writeBlock(block.getBlock(), StorageType.DEFAULT, accessToken, clientname, targets, new StorageType[0], srcNode,
				            BlockConstructionStage.TRANSFER_FINALIZED, 0, 0, 0, 0, blockSender.getChecksum(), cachingStrategy);

		LOG.info("Sending block " + block.getBlock() + " to " + datanode.getXferAddr());
		
        blockSender.sendBlock(out, baseStream);

        LOG.info("Sent block " + block.getBlock() + " to " + datanode.getXferAddr());
      } finally {
        out.close();
      }
    }
  
    /**
     * returns the source file corresponding to a parity file
     */
    Path sourcePathFromParityPath(Path parityPath) {
      String parityPathStr = parityPath.toUri().getPath();
      if (parityPathStr.startsWith(xorPrefix)) {
        // Remove the prefix to get the source file.
        String src = parityPathStr.replaceFirst(xorPrefix, "/");
        return new Path(src);
      } else if (parityPathStr.startsWith(rsPrefix)) {
        // Remove the prefix to get the source file.
        String src = parityPathStr.replaceFirst(rsPrefix, "/");
        return new Path(src);
      }
      return null;
    }

    /**
     * Returns the corrupt blocks in a file.
     */
    List<LocatedBlock> corruptBlocksInFile(DistributedFileSystem fs,
                                           String uriPath, FileStatus stat)
      throws IOException {
      List<LocatedBlock> corrupt = new LinkedList<LocatedBlock>();
      LocatedBlocks locatedBlocks =
        RaidDFSUtil.getBlockLocations(fs, uriPath, 0, stat.getLen());
      for (LocatedBlock b: locatedBlocks.getLocatedBlocks()) {
        if (b.isCorrupt() ||
            (b.getLocations().length == 0 && b.getBlockSize() > 0)) {
          corrupt.add(b);
        }
      }
      return corrupt;
    }
  }

}

