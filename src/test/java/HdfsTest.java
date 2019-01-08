import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.IOException;

public class HdfsTest {
    private final String url = "hdfs://localhost:9000";

    @Test
    public void listFiles() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", url);
        FileSystem fs = FileSystem.newInstance(conf);
        // true 表示递归查找  false 不进行递归查找
        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path("/"), true);
        while (iterator.hasNext()) {
            LocatedFileStatus next = iterator.next();
            System.out.println(next.getPath());
        }
        System.out.println("----------------------------------------------------------");
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {
            System.out.println(fileStatus.getPath());
        }
    }

    /**
     * 上传文件到hdfs上
     */
    @Test
    public void upload() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", url);
        FileSystem fs = FileSystem.get(conf);
        fs.copyFromLocalFile(new Path("README.md"), new Path("/"));
        listFiles();
    }

    /**
     * 将hdfs上文件下载到本地
     */
    @Test
    public void download() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", url);
        FileSystem fs = FileSystem.newInstance(conf);
        fs.copyToLocalFile(new Path("/README.md"), new Path("testDir/README.md"));
        listFiles();
    }

    /**
     * 删除hdfs上的文件
     */
    @Test
    public void removeFile() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", url);
        FileSystem fs = FileSystem.newInstance(conf);
        fs.delete(new Path("/README.md"), true);
        listFiles();
    }

    /**
     * 在hdfs更目录下面创建test1文件夹
     */
    @Test
    public void mkdir() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", url);
        FileSystem fs = FileSystem.newInstance(conf);
        fs.mkdirs(new Path("/testDir"));
        listFiles();
    }
}
