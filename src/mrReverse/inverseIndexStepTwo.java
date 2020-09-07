package mrReverse;/**
 * Created with IntelliJ IDEA
 *
 * @Author: mocas
 * @Date: 2020/7/28 20:40
 * @email: wangyuhang_mocas@163.com
 */

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *@program: weekend01
 *@description:
 *@author: mocas_wang
 *@create: 2020-07-28 20:40
 */
public class inverseIndexStepTwo {

    public static class StepTwoMapper extends Mapper<LongWritable, Text,Text,Text>
    {
        //k起始偏移量

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException
        {
            String line=value.toString();
            String[] fields= StringUtils.split(line,'\t');
            String[] wordAndfileName=StringUtils.split(fields[0],"-->>");
            String word=wordAndfileName[0];
            String fileName=wordAndfileName[1];
            long count=Long.parseLong(fields[1]);
        }
    }
}
