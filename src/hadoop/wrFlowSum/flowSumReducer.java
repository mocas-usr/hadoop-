package hadoop.wrFlowSum;/**
 * Created with IntelliJ IDEA
 *
 * @Author: mocas
 * @Date: 2020/7/22 20:06
 * @email: wangyuhang_mocas@163.com
 */

import org.apache.hadoop.mapreduce.Reducer;

import javax.xml.soap.Text;
import java.io.IOException;

/**
 *@program: weekend01
 *@description: reducer
 *@author: mocas_wang
 *@create: 2020-07-22 20:06
 */
public class flowSumReducer extends Reducer<Text,FlowBean, Text,FlowBean> {
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context)
            throws IOException, InterruptedException {
        long upCount=0;
        long dCount=0;
        for (FlowBean bean:values)
        {
            upCount+=bean.getUp_flow();
            dCount+=bean.getD_flow();
        }
        context.write(key,new FlowBean(key.toString(),upCount,dCount));
    }
}
