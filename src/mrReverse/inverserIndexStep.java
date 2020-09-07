package mrReverse;/**
 * Created with IntelliJ IDEA
 *
 * @Author: mocas
 * @Date: 2020/7/28 19:57
 * @email: wangyuhang_mocas@163.com
 */

import areapartion.areaPartioner;
import areapartion.flowSumArea;
import hadoop.wrFlowSum.FlowBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

/**
 *@program: weekend01
 *@description:
 *@author: mocas_wang
 *@create: 2020-07-28 19:57
 */
public class inverserIndexStep {
    public static  class StepOneMapper extends Mapper<LongWritable, Text,Text,LongWritable>
    {
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            //拿到一行数据
            String line=value.toString();
            //切分出各个单词
            String[] fields= StringUtils.split(line,'\t');
            //获取这一行数据所在的文件切片
            FileSplit inputSplit=(FileSplit)context.getInputSplit();
            //从文件切片中，获取文件名
            String fileName=inputSplit.getPath().getName();

            for (String  filed:fields)
            {
                //封装kv输出，k是一个单词，v是1
                context.write(new Text(filed+">>"+fileName),new LongWritable(1));
            }

        }

    }

    public static class StepOneReducer extends Reducer<Text,LongWritable,Text,LongWritable>
    {
        //每拿到一组数据，
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long count=0;
            for (LongWritable value:values)
            {
                count+=value.get();
            }
            context.write(key,new LongWritable(count));
        }
    }

    public static void main(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance();
        job.setJarByClass(inverserIndexStep.class);
        job.setMapperClass(StepOneMapper.class);
        job.setReducerClass(StepOneReducer.class);
        //设置自定义的分组逻辑
        job.setPartitionerClass(areaPartioner.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);


        //设置reduce的并发数，要和分组数目一致

        job.setNumReduceTasks(6);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        //检测下参数指定的输出路径是否存在，如果存在，则先删除
        Path outputPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath))
        {
            fs.delete(outputPath,true);

        }

        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
