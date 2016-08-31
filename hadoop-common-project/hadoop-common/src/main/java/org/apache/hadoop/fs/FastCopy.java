package org.apache.hadoop.fs;

import java.io.IOException;
import java.util.List;

/**
 * Created by Tao Li on 2016/8/31.
 */
public abstract class FastCopy {
  public enum CopyResult {
    SUCCESS,
    SKIP,
    FAIL
  }

  public static class FastFileCopyRequest {
    private final String src;
    private final String dst;
    private Path srcPath = null;
    private Path dstPath = null;
    private final FileSystem srcFs;
    private final FileSystem dstFs;

    public FastFileCopyRequest(String src, String dst,
                               FileSystem srcFs, FileSystem dstFs) {
      this.src = src;
      this.dst = dst;
      this.srcFs = srcFs;
      this.dstFs = dstFs;
    }

    public FastFileCopyRequest(Path src, Path dst,
                               FileSystem srcFs, FileSystem dstFs) {
      this(src.toString(), dst.toString(), srcFs, dstFs);
      this.srcPath = src;
      this.dstPath = dst;
    }

    public String getSrc() {
      return src;
    }

    public String getDestination() {
      return dst;
    }

    public String getDst() {
      return dst;
    }

    public FileSystem getSrcFs() {
      return srcFs;
    }

    public FileSystem getDstFs() {
      return dstFs;
    }

    public Path getSrcPath() {
      if (srcPath == null) {
        srcPath = new Path(src);
      }
      return srcPath;
    }

    public Path getDstPath() {
      if (dstPath == null) {
        dstPath = new Path(dst);
      }
      return dstPath;
    }
  }

  /**
   * Wrapper class that holds the source and destination path for a file to be
   * copied. This is to help in easy computation of source and destination files
   * while copying directories.
   */
  public static class CopyPath {
    private final Path srcPath;
    private final Path dstPath;

    /**
     * @param srcPath source path from where the file should be copied from.
     * @param dstPath destination path where the file should be copied to
     */
    public CopyPath(Path srcPath, Path dstPath) {
      this.srcPath = srcPath;
      this.dstPath = dstPath;
    }

    /**
     * @return the srcPath
     */
    public Path getSrcPath() {
      return srcPath;
    }

    /**
     * @return the dstPath
     */
    public Path getDstPath() {
      return dstPath;
    }

  }

  public abstract void copy(String src, String destination) throws Exception;

  public abstract CopyResult copy(String src, String destination,
                                  FileSystem srcFs, FileSystem dstFs) throws Exception;

  public abstract void copy(List<FastFileCopyRequest> requests) throws Exception;

  public abstract void shutdown() throws IOException;
}