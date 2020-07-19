package mrwordcount;
/**
 * Created with IntelliJ IDEA
 *
 * @Author: mocas
 * @Date: 2020/7/19 15:31
 * @email: wangyuhang_mocas@163.com
 */

import org.apache.hadoop.hdfs.util.EnumCounters;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;
import org.mortbay.util.StringUtil;

import java.io.IOException;

/**
 *@program: weekend01
 *@description: 继承mapper
 *@author: mocas_wang
 *@create: 2020-07-19 15:31
 */
//四个泛型，前两个是指定mapper输入的数据类型，keyin是key的类型，value是输入的value类型
    //mapreduce 都是map和reduce的数据输入输出都是以key-value对的输入对形式
    //默认情况下，框架传递给我们的mapper的输入数据中，key是要处理的文本中要处理的文本偏移量
public class wcMapper extends Mapper<LongWritable, Text,Text,LongWritable>
{
    //MAPreduce每读一次数据，就调用一次方法
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {

        //具体业务逻辑就写在这个方法，而且我们业务要处理的数据已经被框架传递进来，在方法的参数中key-value
        //key是这一行数据的起始偏移量，value是这一行的文本内容
        //将这一行内容转换成string类型
        String line=value.toString();
        //对这一行的文本按特定分隔符区分
        String[] words= StringUtils.split(line, ' ');
        //遍历这个单词数组输出为kv形势
        for (String word:words)
        {
            context.write(new Text(word),new LongWritable(1));
        }


    }
}
