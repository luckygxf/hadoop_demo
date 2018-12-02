import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * @Author: <guanxianseng@163.com>
 * @Description:
 * @Date: Created in : 2018/12/2 11:14 PM
 **/
public class PutMerge {

  public static void main(String[] args) throws IOException {
    Configuration conf = new Configuration();
    FileSystem hdfs = FileSystem.get(conf);
    FileSystem local = FileSystem.getLocal(conf);

    Path inputDir = new Path(args[0]);
    Path hdfsFile = new Path(args[1]);

    FileStatus[] inputFiles = local.listStatus(inputDir);
    FSDataOutputStream out = hdfs.create(hdfsFile);

    for(int i = 0; i < inputFiles.length; i++){
      System.out.println(inputFiles[i].getPath().getName());
      FSDataInputStream in = local.open(inputFiles[i].getPath());
      byte[] buffer = new byte[256];
      int byteRead = 0;
      while((byteRead = in.read(buffer)) > 0){
        out.write(buffer, 0, byteRead);
        in.close();
      }
      out.close();
    }
  }
}
