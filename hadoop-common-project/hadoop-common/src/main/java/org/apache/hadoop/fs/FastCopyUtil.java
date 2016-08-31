package org.apache.hadoop.fs;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FastCopy.FastFileCopyRequest;
import org.apache.hadoop.fs.FastCopy.CopyPath;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Tao Li on 2016/9/2.
 */
public class FastCopyUtil {
  private static final Log LOG = LogFactory.getLog(FastCopyUtil.class);

  public static List<String> fastCopy(Configuration conf,
                                      String[] srcs, Path dstPath,
                                      FileSystem sysFs, FileSystem dstFs,
                                      int threadPoolSize) throws Exception {
    List<CopyPath> copyPaths = new ArrayList<CopyPath>();
    parseFiles(copyPaths, srcs, dstPath, sysFs, dstFs);

    Class<? extends FastCopy> clazz =
        (Class<? extends FastCopy>) conf.getClassByName("org.apache.hadoop.hdfs.FastCopyImpl");
    Class[] cArgs = new Class[3];
    cArgs[0] = Configuration.class;
    cArgs[1] = int.class;
    cArgs[2] = boolean.class;
    FastCopy fcp = clazz.getDeclaredConstructor(cArgs).newInstance(conf, threadPoolSize, false);

    List<FastFileCopyRequest> requests = new ArrayList<FastFileCopyRequest>();
    List<String> failedSrcs = new ArrayList<String>();

    try {
      for (CopyPath copyPath : copyPaths) {
        Path srcPath = copyPath.getSrcPath();
        String src = srcPath.toString();
        try {
          // Perform some error checking and path manipulation.
          if (!sysFs.exists(srcPath)) {
            throw new IOException("File : " + src + " does not exists on " + sysFs);
          }

          String destination = copyPath.getDstPath().toString();
          requests.add(new FastFileCopyRequest(src, destination, sysFs, dstFs));
        } catch (Exception e) {
          LOG.error("Fast Copy failed for file : " + src, e);
          failedSrcs.add(src);
        }
      }
      fcp.copy(requests);
    } finally {
      fcp.shutdown();
      return failedSrcs;
    }
  }

  private static void parseFiles(List<CopyPath> result,
                                 String srcs[], Path dstPath,
                                 FileSystem srcFs, FileSystem dstFs) throws IOException {
    List<Path> srcPaths = new ArrayList<Path>();
    for (int i = 0; i < srcs.length; i++) {
      srcPaths.add(new Path(srcs[i]));
    }

    result.clear();
    result.addAll(expandSrcs(srcPaths, dstPath, srcFs, dstFs));

    // If we have multiple source files, the destination has to be a directory.
    if (dstFs.exists(dstPath) && !dstFs.getFileStatus(dstPath).isDirectory()
        && result.size() > 1) {
      throw new IllegalArgumentException("Path : " + dstPath
          + " is not a directory");
    }

    // If the expected destination is a directory and it does not exist throw
    // an error.
    if (!dstFs.exists(dstPath) && srcPaths.size() > 1) {
      throw new IllegalArgumentException("Path : " + dstPath
          + " does not exist");
    }
  }

  /**
   * Expands all sources, if they are file pattern expand to list out all files
   * matching the pattern, if they are a directory, expand to list out all files
   * under the directory.
   *
   * @param srcPaths the files to be expanded
   * @param dstPath  the destination
   * @return the fully expanded list of all files for all file/filepatterns
   * provided.
   * @throws IOException
   */
  private static List<CopyPath> expandSrcs(List<Path> srcPaths, Path dstPath,
                                           FileSystem srcFs, FileSystem dstFs)
      throws IOException {
    List<CopyPath> expandedSrcs = new ArrayList<CopyPath>();
    for (Path src : srcPaths) {
      expandedSrcs.addAll(expandSingle(src, dstPath, srcFs, dstFs));
    }
    return expandedSrcs;
  }

  /**
   * Expand a single file, if its a file pattern list out all files matching the
   * pattern, if its a directory return all files under the directory.
   *
   * @param srcPath the file to be expanded
   * @param dstPath the destination
   * @return the expanded file list for this file/filepattern
   * @throws IOException
   */
  private static List<CopyPath> expandSingle(Path srcPath, Path dstPath,
                                             FileSystem srcFs, FileSystem dstFs)
      throws IOException {
    List<Path> expandedPaths = new ArrayList<Path>();
    FileStatus[] stats = srcFs.globStatus(srcPath);
    if (stats == null || stats.length == 0) {
      throw new IOException("Path : " + srcPath + " is invalid");
    }
    for (FileStatus stat : stats) {
      expandedPaths.add(stat.getPath());
    }
    List<CopyPath> expandedDirs = expandDirectories(expandedPaths, dstPath, srcFs, dstFs);
    return expandedDirs;
  }

  /**
   * Get the listing of all files under the given directories.
   *
   * @param srcFs the filesystem
   * @param paths the paths whose directory listing is to be retrieved
   * @return the directory expansion for all paths provided
   * @throws IOException
   */
  private static List<CopyPath> expandDirectories(List<Path> paths, Path dstPath,
                                                  FileSystem srcFs, FileSystem dstFs)
      throws IOException {
    List<CopyPath> newList = new ArrayList<CopyPath>();
    boolean isDstFile = false;
    try {
      FileStatus dstPathStatus = dstFs.getFileStatus(dstPath);
      if (!dstPathStatus.isDirectory()) {
        isDstFile = true;
      }
    } catch (FileNotFoundException e) {
      isDstFile = true;
    }

    for (Path path : paths) {
      FileStatus pathStatus = srcFs.getFileStatus(path);
      if (!pathStatus.isDirectory()) {
        // This is the case where the destination is a file, in this case, we
        // allow only a single source file. This check has been done below in
        // FastCopy#parseFiles(List, String[])
        if (isDstFile) {
          newList.add(new CopyPath(path, dstPath));
        } else {
          newList.add(new CopyPath(path, new Path(dstPath, path.getName())));
        }
      } else {
        // If we are copying /a/b/c into /x/y/z and 'z' does not exist, we
        // create the structure /x/y/z/f*, where f* represents all files and
        // directories in c/
        Path rootPath = dstPath;
        // This ensures if we copy a directory like /a/b/c to a directory
        // /x/y/z/, we will create the directory structure /x/y/z/c, if 'z'
        // exists.
        if (dstFs.exists(dstPath)) {
          rootPath = new Path(dstPath, pathStatus.getPath().getName());
        }
        getDirectoryListing(newList, pathStatus, rootPath, srcFs, dstFs);
      }
    }
    return newList;
  }

  /**
   * Recursively lists out all the files under a given path.
   *
   * @param root   the path under which we want to list out files
   * @param srcFs  the filesystem
   * @param result the list which holds all the files.
   * @throws IOException
   */
  private static void getDirectoryListing(List<CopyPath> result,
                                          FileStatus root, Path dstPath,
                                          FileSystem srcFs, FileSystem dstFs)
      throws IOException {
    if (!root.isDirectory()) {
      result.add(new CopyPath(root.getPath(), dstPath));
      return;
    }

    dstFs.mkdirs(dstPath, root.getPermission());

    for (FileStatus child : srcFs.listStatus(root.getPath())) {
      getDirectoryListing(result, child, new Path(dstPath, child.getPath().getName()), srcFs, dstFs);
    }
  }
}