package hdfs;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HDFSCopyMerge {
		
	public static void copyMergeToLocal(String hdfsUri, String src, String dst) throws IOException, URISyntaxException{
		FileSystem srcFS = FileSystem.get(new URI(hdfsUri), new Configuration());
		OutputStream out = new FileOutputStream(dst);
		Path srcPath = new Path(src);
		if(!srcFS.getFileStatus(srcPath).isDirectory()){
			InputStream in = srcFS.open(srcPath);
			try{
				IOUtils.copyBytes(in, out, 4096, false);
			}
			finally{
				in.close();
				out.close();
			}
		}else{
		
			try{
				FileStatus[] contents = srcFS.listStatus(srcPath);
				Arrays.sort(contents);
				for(int i=0; i<contents.length; i++){
					if(contents[i].isFile()){
						InputStream in = srcFS.open(contents[i].getPath());
						try{
							IOUtils.copyBytes(in, out, 4096, false);
						}
						finally{
							in.close();
						}
					}
				}
			}
			finally{
				out.close();
			}
		}
		
	}
	public static void main(String[] args) throws IOException, URISyntaxException {
			copyMergeToLocal("hdfs://hadoop01:9000", "/data_p4", "/home/xugs/temp.tmp");
	}

}
