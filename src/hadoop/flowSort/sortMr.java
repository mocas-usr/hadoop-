package hadoop.flowSort;/**
 * Created with IntelliJ IDEA
 *
 * @Author: mocas
 * @Date: 2020/7/23 19:49
 * @email: wangyuhang_mocas@163.com
 */

import hadoop.wrFlowSum.FlowBean;
import hadoop.wrFlowSum.flowSumMapper;
import hadoop.wrFlowSum.flowSumReducer;
import hadoop.wrFlowSum.flowSumRunner;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.Sort;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 *@program: weekend01
 *@description:
 *@author: mocas_wang
 *@create: 2020-07-23 19:49
 */
public class sortMr {

    public  static  class SortMapper extends Mapper<LongWritable, Text, FlowBean, NullWritable>
    {
        //拿到一行数据，切分出字段，封装成一个flowbean
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException
        {

            String line=value.toString();
            String[] filds= StringUtils.split(line,'\t');
            String phoneNb=filds[0];
            long u_flow=Long.parseLong(filds[1]);
            long d_flow=Long.parseLong(filds[2]);
            context.write(new FlowBean(phoneNb,u_flow,d_flow),NullWritable.get());
        }
    }

    public static  class SortReducer extends Reducer<FlowBean,NullWritable,Text,FlowBean>
    {
        @Override
        protected void reduce(FlowBean key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {
            String phoneNb=key.getPhoneNum();
            context.write(new Text(phoneNb),key);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf=new Configuration();
        Job job= Job.getInstance();
        job.setJarByClass(sortMr.class);

        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);

        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        System.out.println(job.waitForCompletion(true)?0:1);
    }
}
