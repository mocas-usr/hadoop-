package mrwordcount;/**
 * Created with IntelliJ IDEA
 *
 * @Author: mocas
 * @Date: 2020/7/19 16:22
 * @email: wangyuhang_mocas@163.com
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;


/**
 *@program: weekend01
 *@description: 用来描述一个特定的作业，比如
 * 该作业使用哪一个类作为逻辑处理的map，哪个作为reduce
 * 还可以指定该作业要使用数据的路径
 * 还可以指定结果的路径
 *@author: mocas_wang
 *@create: 2020-07-19 16:22
 */
public class wcRunner {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf= new Configuration();

        Job wcJob=Job.getInstance(conf);

        //设置整个job使用的那些类在哪个jar包
        wcJob.setJarByClass(wcRunner.class);

        //本job使用的mapper和reduce类
        wcJob.setMapperClass(wcMapper.class);
        wcJob.setReducerClass(wcReducer.class);

        //指定reduce的输出数据类型kv
        wcJob.setOutputKeyClass(Text.class);
        wcJob.setOutputValueClass(LongWritable.class);

        //指定mapper的输出数据类型，kv类型
        wcJob.setMapOutputKeyClass(Text.class);
        wcJob.setMapOutputValueClass(LongWritable.class);

        //指定原始输入路径在那
        FileInputFormat.setInputPaths(wcJob,new Path("/wc/"));
        //指定处理结果输出路径
        FileOutputFormat.setOutputPath(wcJob,new Path("ouyt"));
        //将job提交给集群运行
        wcJob.waitForCompletion(true);
    }

}
