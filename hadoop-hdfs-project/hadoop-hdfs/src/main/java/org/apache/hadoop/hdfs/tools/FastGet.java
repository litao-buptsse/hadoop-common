package org.apache.hadoop.hdfs.tools;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by Tao Li on 2016/8/16.
 */
public class FastGet {
  private static final Log LOG = LogFactory.getLog(DFSAdmin.class);

  private final static long DEFAULT_BLOCK_SIZE = 128 * 1024 * 1024;
  private final static int DEFAULT_THREAD_NUM = 10;

  public static void main(String[] args) throws IOException {
    if (args.length < 2) {
      LOG.error("Usage: hdfs fastGet <src> <localdst> [threadNum] [blockSize]");
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

    Path srcPath = new Path(src);
    Configuration conf = new Configuration();
    FileSystem srcFS = srcPath.getFileSystem(conf);
    FileStatus srcFile = srcFS.getFileStatus(srcPath);
    FSDataInputStream input = srcFS.open(srcPath);
    long length = srcFile.getLen();

    RandomAccessFile dstFile = new RandomAccessFile(localdst, "rw");

    ExecutorService service = Executors.newFixedThreadPool(threadNum);
    List<Future> futures = new ArrayList<Future>();

    long position = 0;
    for (; position <= length - blockSize; position += blockSize) {
      futures.add(service.submit(new GetTask(input, dstFile, position, blockSize)));
    }
    if (length > position) {
      futures.add(service.submit(new GetTask(input, dstFile, position, length - position)));
    }

    for (Future future : futures) {
      try {
        future.get();
      } catch (InterruptedException e) {
        e.printStackTrace();
      } catch (ExecutionException e) {
        e.printStackTrace();
      }
    }

    input.close();
    dstFile.close();
    service.shutdown();
  }

  static class GetTask implements Runnable {
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

    public void run() {
      try {
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
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
}