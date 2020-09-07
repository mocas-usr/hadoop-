package hadoop.wrFlowSum;/**
 * Created with IntelliJ IDEA
 *
 * @Author: mocas
 * @Date: 2020/7/22 20:06
 * @email: wangyuhang_mocas@163.com
 */

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

/**
 *@program: weekend01
 *@description: mapper
 *@author: mocas_wang
 *@create: 2020-07-22 20:06
 */
 public class flowSumMapper extends Mapper<LongWritable, Text,Text,FlowBean> {

    //拿到一行数据，切分数据，拿到需要的字段
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
