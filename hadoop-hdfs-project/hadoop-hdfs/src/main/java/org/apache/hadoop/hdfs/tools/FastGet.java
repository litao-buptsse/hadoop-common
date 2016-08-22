package org.apache.hadoop.hdfs.tools;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by Tao Li on 2016/8/16.
 */
public class FastGet {
  private static final Log LOG = LogFactory.getLog(DFSAdmin.class);

  private final static long DEFAULT_BLOCK_SIZE = 128;
  private final static int DEFAULT_THREAD_NUM = 10;

  public static void main(String[] args) {
    if (args.length < 2) {
      LOG.error("Usage: hdfs fastGet <src> <localdst> [threadNum] [blockSize(MB)]");
      System.exit(1);
    }

    String src = args[0];
    String localdst = args[1];

    int threadNum = DEFAULT_THREAD_NUM;
    if (args.length >= 3) {
      threadNum = Integer.parseInt(args[2]);
    }

    long blockSize = DEFAULT_BLOCK_SIZE;
    if (args.length >= 4) {
      blockSize = Long.parseLong(args[3]);
    }
    blockSize *= 1024 * 1024;

    FSDataInputStream input = null;
    RandomAccessFile dstFile = null;
    ExecutorService service = null;

    try {
      Path srcPath = new Path(src);
      Configuration conf = new Configuration();
      FileSystem srcFS = srcPath.getFileSystem(conf);
      FileStatus srcFile = srcFS.getFileStatus(srcPath);

      if (srcFile.isDirectory()) {
        LOG.error("Not support FastGet for directory");
        System.exit(1);
      }

      input = srcFS.open(srcPath);
      long length = srcFile.getLen();

      File dst = new File(localdst);
      if (dst.exists()) {
        if (dst.isFile()) {
          LOG.error(String.format("%s is already exist", localdst));
          System.exit(1);
        } else if (dst.isDirectory()) {
          localdst = srcPath.getName();
        }
      }

      dstFile = new RandomAccessFile(localdst, "rw");

      service = Executors.newFixedThreadPool(threadNum);
      List<Future<Void>> futures = new ArrayList<Future<Void>>();
      long position = 0;

      // submit tasks for each block
      for (; position <= length - blockSize; position += blockSize) {
        futures.add(service.submit(new GetTask(input, dstFile, position, blockSize)));
      }
      if (length > position) {
        futures.add(service.submit(new GetTask(input, dstFile, position, length - position)));
      }

      // wait for each task complete
      for (Future<Void> future : futures) {
        future.get();
      }
    } catch (Exception e) {
      LOG.error("Exception Occured: ", e);
      System.exit(1);
    } finally {
      if (input != null) {
        try {
          input.close();
        } catch (IOException e) {
          // ignore it
        }
      }
      if (dstFile != null) {
        try {
          dstFile.close();
        } catch (IOException e) {
          // ignore it
        }
      }
      service.shutdown();
    }
  }

  static class GetTask implements Callable<Void> {
    private final static int BUFFER_SIZE = 256 * 1024;

    private FSDataInputStream input;
    RandomAccessFile dst;
    private long offset;
    private long length;
    private long totalBytesRead = 0;
    private byte[] buffer;

    public GetTask(FSDataInputStream input, RandomAccessFile dst, long offset, long length) {
      this.input = input;
      this.dst = dst;
      this.offset = offset;
      this.length = length;
      buffer = new byte[BUFFER_SIZE];
    }

    @Override
    public Void call() throws Exception {
      int bytesRead = 0;
      while (bytesRead >= 0 && totalBytesRead < length) {
        int toRead = (int) Math.min(buffer.length, length - bytesRead);
        bytesRead = input.read(offset + totalBytesRead, buffer, 0, toRead);

        if (bytesRead < 0) {
          break;
        }

        synchronized (dst) {
          dst.seek(offset + totalBytesRead);
          dst.write(buffer, 0, bytesRead);
          dst.getFD().sync();
        }

        totalBytesRead += bytesRead;
      }
      return null;
    }
  }
}