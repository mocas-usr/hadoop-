package areapartion;
/**
 * Created with IntelliJ IDEA
 *
 * @Author: mocas
 * @Date: 2020/7/24 20:16
 * @email: wangyuhang_mocas@163.com
 */

import hadoop.wrFlowSum.FlowBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

/**
 *@program: weekend01
 *@description:需要改造两个机制，
 * 1.改造分期的逻辑，自定义partion
 * 2.自定义reducer task的并发任务数
 *@author: mocas_wang
 *@create: 2020-07-24 20:16
 */
public class flowSumArea {
    public static class flowSumMapper extends Mapper<LongWritable, Text,Text, FlowBean>
    {
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            //拿一行数据
            String line=value.toString();
            //切分字段
            String[] fileds= StringUtils.split(line,'\t');
            String phoneNum=fileds[1];
            long u_flow=Long.parseLong(fileds[7]);
            long d_flow=Long.parseLong(fileds[8]);
            //封装成kv并输出
            context.write(new Text(phoneNum),new FlowBean(phoneNum,u_flow,d_flow));
        }
    }

    public static class flowSumReducer extends Reducer<Text,FlowBean,Text,FlowBean>
    {
        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
            long u_count=0;
            long d_count=0;

            for (FlowBean bean:values)
            {
                u_count+=bean.getUp_flow();
                d_count+=bean.getD_flow();
            }
            context.write(key,new FlowBean(key.toString(),u_count,d_count));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf=new Configuration();
        Job job= Job.getInstance();
        job.setJarByClass(flowSumArea.class);
        job.setMapperClass(flowSumMapper.class);
        job.setReducerClass(flowSumReducer.class);
        //设置自定义的分组逻辑
        job.setPartitionerClass(areaPartioner.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);


        //设置reduce的并发数，要和分组数目一致

        job.setNumReduceTasks(6);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
