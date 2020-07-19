package mrwordcount;/**
 * Created with IntelliJ IDEA
 *
 * @Author: mocas
 * @Date: 2020/7/19 15:32
 * @email: wangyuhang_mocas@163.com
 */

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 *@program: weekend01
 *@description: reducer类
 *@author: mocas_wang
 *@create: 2020-07-19 15:32
 */
public class wcReducer extends Reducer<Text, LongWritable,Text,LongWritable> {
    //map处理完毕之后，将所有kv缓存起来，进行分组，然后传递一个组<key，values>，调用reduce方法
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException
    {
        long count=0;
        //遍历valu的list，进行累加求和
        for (LongWritable value:values)
        {
            count += value.get();
        }
        //输出这一个单词的统计结果
        context.write(key,new LongWritable(count));

    }
}
