package hdfsDemo;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created with IntelliJ IDEA
 *
 * @Author: mocas
 * @Date: 2020/7/15 20:31
 * @email: wangyuhang_mocas@163.com
 */
public class hdfsUtil {

    FileSystem fs=null;
    /**
    *@Description:
    *@Param:
    *@return:
    *@Author: mocas_wang
    *@date:
    */
    @Before
    public  void initial() throws IOException, URISyntaxException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://weekend01:9000/");
        fs=FileSystem.get(new URI("hdfs://weekend01:9000/"),conf,"wyh");
    }
    public static void main(String[] args) throws IOException {
        hdfsUtil hdfsUtil=new hdfsUtil();
        hdfsUtil.upload();
    }

    //上传文件
    @Test
    public  void  upload() throws IOException {

        Path dst=new Path("hdfs://weekend01:9000/aa/qingshu.txt");
        FSDataOutputStream os=fs.create(dst);
        FileInputStream is=new FileInputStream("E:/Hadoop/qingshu.txt");
        IOUtils.copy(is,os);

    }

    //第二种上传方法
    @Test
    public void upload2() throws IOException {

        fs.copyFromLocalFile(new Path("E:/Hadoop/qingshu.txt"),new Path("hdfs://weekend01:9000/aa/qingshu2.txt"));
    }

    //下载
    @Test
    public void download() throws IOException {
        fs.copyToLocalFile(new Path("hdfs://weekend01:9000/aa/qingshu2.txt"),new Path("E:/Hadoop/qingshu3.txt"));
    }

    //创建文件夹
    @Test
    public void  mkdir() throws IOException {
        fs.mkdirs(new Path("/AAA/BBB/CCC"));
        fs.copyFromLocalFile(new Path("E:/Hadoop/qingshu3.txt"),new Path("hdfs://weekend01:9000/AAA/BBB/qingshu2.txt"));
    }

    //删除文件
    @Test
    public void rm() throws IOException {
        fs.delete(new Path("/AAA"),true);
    }

    //查看文件信息
    @Test
    public void listFiles() throws IOException {
        RemoteIterator<LocatedFileStatus> files=fs.listFiles(new Path("/"),true);

        //打印文件名称
        while (files.hasNext())
        {
            LocatedFileStatus file=files.next();
            Path filePath=file.getPath();
            String fileName=filePath.getName();
            System.out.println(fileName);
        }
        //fs.listFiles();
        System.out.println("------------------------");
        FileStatus[] listStatues=fs.listStatus(new Path("/"));
        for (FileStatus status:listStatues)
        {
            String name=status.getPath().getName();
            System.out.println(name);
        }
    }
}
