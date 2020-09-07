package hadoop.wrFlowSum;/**
 * Created with IntelliJ IDEA
 *
 * @Author: mocas
 * @Date: 2020/7/22 20:06
 * @email: wangyuhang_mocas@163.com
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *@program: weekend01
 *@description:
 *@author: mocas_wang
 *@create: 2020-07-22 20:06
 */
public class flowSumRunner  extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf=new Configuration();
        Job job= Job.getInstance();
        job.setJarByClass(flowSumRunner.class);

        job.setMapperClass(flowSumMapper.class);
        job.setReducerClass(flowSumReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        FileInputFormat.setInputPaths(job,new Path(strings[0]));
        FileOutputFormat.setOutputPath(job,new Path(strings[1]));

        return job.waitForCompletion(true)?0:1;

    }

    public static void main(String[] args) throws Exception {
        int res=ToolRunner.run(new Configuration(),new flowSumRunner(),args);
        System.out.println(res);
    }
}
