package org.apache.hadoop.raid;

import java.io.IOException;
import java.net.URI;
import java.net.InetSocketAddress;
import java.util.Collection;

import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.fs.viewfs.ViewFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

public class FED_HAUtil {

	protected static final String VIEW_FS_PREFIX = "viewfs://";
	protected static final String HDFS_FS_PREFIX = "hdfs://";

	public static Path removeViewFSPrefix(Path path) {
		return new Path(removeFSPrefix(path, VIEW_FS_PREFIX));
	}
	
	public static Path removeHDFSPrefix(Path path) {
		return new Path(removeFSPrefix(path, HDFS_FS_PREFIX));
	}
	
	public static String removeFSPrefix(Path path, String tag) {
		String pathStr = path.toString().trim();
		if (pathStr.startsWith(tag)) {
			pathStr = pathStr.replaceAll(tag, "");
			int pos = pathStr.indexOf("/");
			if (pos > 0 && pos < pathStr.length()) {
				return pathStr.substring(pos);
			}
		}
		return pathStr;
	}

	public static FileSystem getDistributedFileSystem(FileSystem fs)
			throws IOException {		
		return getDistributedFileSystem(fs, new Path("/"));
	}
	
	public static FileSystem getDistributedFileSystem(FileSystem fs, Path path)
			throws IOException {		
		if (fs instanceof ViewFileSystem) {
			/*ViewFileSystem vfs = (ViewFileSystem) fs;
			FileSystem[] fss = vfs.getChildFileSystems();
			for (FileSystem fSys : fss) {
				if (fSys.exists(path)) {				
					fs = fSys;
					break;
				}
			}*/
			
			Configuration conf = fs.getConf();
			String services = conf.get("dfs.nameservices").trim();
			String[] servicesNames = null;
			if(services.contains(",")) {
				servicesNames = services.split(",");	
			} else {
				servicesNames = new String[] { services };
			}
			
			String validPath = removeFSPrefix(path, VIEW_FS_PREFIX);
			String raidPrefix = conf.get("hdfs.raidrs.locations");
			if(validPath.startsWith(raidPrefix)) {
				String firstDir = getRidOfPrefixSubDir(validPath, raidPrefix);
				String mountKey = "fs.viewfs.mounttable.nsX.link." + firstDir;
				String realMountHDFS = conf.get(mountKey);
				String realServiceName = servicesNames[0];
				for(String sN : servicesNames) {
					String mountValue = HDFS_FS_PREFIX + sN + raidPrefix;
					if(realMountHDFS.equals(mountValue)) {
						realServiceName = sN;
						break;
					}
 				}
				
				Collection<String> nnIds = conf.getTrimmedStringCollection(DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX + "." + realServiceName);
			    for(String id : nnIds) {
				  String key = DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY + "." + realServiceName + "." + id;
				  System.out.println("key :" + key);
				  if(conf.get(key) != null) {
					String hdfsStr = "hdfs://" + conf.get(key);
					conf.set("fs.defaultFS", hdfsStr);
					if(getAvailablePath(removeViewFSPrefix(path).toString(), conf) != null) {
					   return FileSystem.get(conf);
					}
				  }
			    }
			}
		} else if (fs instanceof DistributedFileSystem) {
			if (!fs.exists(path)) {
				throw new IOException(
						"FileSystem is not DistributedFileSystem.");
			}
		} else {
			throw new IOException("FileSystem is not DistributedFileSystem.");
		}
		return fs;
	}
	
	private static String getRidOfPrefixSubDir(String pathStr, String prefix) {
		String res = pathStr;
		
		if(pathStr.endsWith("/")) {
			pathStr = pathStr.substring(0, pathStr.length() - 1);
		}
		if(!pathStr.equals(prefix)) {
			pathStr = pathStr.replace(prefix, "").replace("/", "");
			int pos = pathStr.indexOf("/"); 
			if(pos > 0) {
				res = pathStr.substring(0, pos);
			}
		}
		
		return res;
	}
	
	public static Path getFirstSubDirOfPath(Path path) throws IOException {
		String pathStr = path.toString();
		if(pathStr.startsWith(VIEW_FS_PREFIX)) {
			pathStr = removeFSPrefix(path, VIEW_FS_PREFIX);
		} else if(pathStr.startsWith(HDFS_FS_PREFIX)) {
			pathStr = removeFSPrefix(path, HDFS_FS_PREFIX);
		}
		 
		if(pathStr.contains("/") && pathStr.length() > 1) {
			String[] paths = pathStr.split("/");
			if(paths.length > 1) {				
				return new Path("/" + paths[0]);
			}
		}
		
		return path;
	}
	
	public static boolean checkIfViewFileSystem(Configuration conf)
			throws IOException {
		URI filesystemURI = FileSystem.getDefaultUri(conf);
		if (filesystemURI.toString().startsWith(VIEW_FS_PREFIX)) {
			return true;
		}
		return false;
	}
	
	public static String[] getDfsNameServices(Configuration conf)
			throws IOException {
		if(checkIfViewFileSystem(conf)) {
			String services = conf.get("dfs.nameservices").trim();
			if(services.contains(",")) {
				String[] servicesNames = services.split(",");				
				return servicesNames;
			} else {
				String[] servicesNames = new String[] { services };
				return servicesNames;
			}
		}
		return null;
	}
	
	public static InetSocketAddress getAvailableNNAddrFromServiceName(String serviceName, Configuration conf, String fileName) 
			throws IllegalArgumentException, IOException {	
		Collection<String> nnIds = conf.getTrimmedStringCollection(DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX + "." + serviceName);
		for(String id : nnIds) {
			String key = DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY + "." + serviceName + "." + id;
			if(conf.get(key) != null) {
				String hdfsStr = "hdfs://" + conf.get(key);
				System.out.println("xxxxx nameService");
			    conf.set("fs.defaultFS", hdfsStr);
			    FileSystem fs = FileSystem.get(conf);
			    if(getAvailablePath("/", conf) != null && fs.exists(new Path(fileName))) {
			    	return NameNode.getAddress(URI.create(hdfsStr));
			    }
			}
		}
		return null;
	}
	
	public static InetSocketAddress getAvailableNNAddrFromServiceName(String serviceName, Configuration conf) {	
		Collection<String> nnIds = conf.getTrimmedStringCollection(DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX + "." + serviceName);
		for(String id : nnIds) {
			String key = DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY + "." + serviceName + "." + id;
			if(conf.get(key) != null) {
				String hdfsStr = "hdfs://" + conf.get(key);
			    conf.set("fs.defaultFS", hdfsStr);
			    if(getAvailablePath("/", conf) != null) {
			    	return NameNode.getAddress(URI.create(hdfsStr));
			    }
			}
		}
		return null;
	}
	
	public static Path getAvailablePath(String srcPath, Configuration conf) {
		Path path = null;
		if(srcPath.contains(",")) {
			for(String str : srcPath.trim().split(",")) {
				try {
				  Path p = new Path(str);
				  p = p.makeQualified(p.getFileSystem(conf));
				  if(p.getFileSystem(conf).exists(new Path("/"))) {
					path = p;
					break;
         		  }
				} catch(IOException ioe) {  
					/*do nothing*/
					path = null;
				}
			}
		} else {
			try {
			  Path p = new Path(srcPath);
			  p = p.makeQualified(p.getFileSystem(conf));
			  if(p.getFileSystem(conf).exists(new Path("/"))) {
				path = p;
       		  }
			} catch(IOException ioe) {  
				/*do nothing*/
				path = null;
			}
		}
	    return path;
	}
	
	public static FileStatus[] getGlobStatus(String srcPath, Configuration conf) throws IOException {
		Path path = getAvailablePath(srcPath, conf);
		FileSystem fs = path.getFileSystem(conf);	    
	    return fs.globStatus(path);
	}
}
