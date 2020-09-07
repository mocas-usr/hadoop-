package hadoop.wrFlowSum;/**
 * Created with IntelliJ IDEA
 *
 * @Author: mocas
 * @Date: 2020/7/22 20:09
 * @email: wangyuhang_mocas@163.com
 */

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *@program: weekend01
 *@description:
 *@author: mocas_wang
 *@create: 2020-07-22 20:09
 */
public class FlowBean implements Writable,Comparable<FlowBean> {

    private String phoneNum;
    private long up_flow;
    private long d_flow;
    private long s_flow;

    public String getPhoneNum() {
        return phoneNum;
    }

    public void setPhoneNum(String phoneNum) {
        this.phoneNum = phoneNum;
    }

    public long getUp_flow() {
        return up_flow;
    }

    public void setUp_flow(long up_flow) {
        this.up_flow = up_flow;
    }

    public long getD_flow() {
        return d_flow;
    }

    public void setD_flow(long d_flow) {
        this.d_flow = d_flow;
    }

    public long getS_flow() {
        return s_flow;
    }

    public void setS_flow(long s_flow) {
        this.s_flow = s_flow;
    }

    //为了对象初始化方便
    public FlowBean() {
    }

    public FlowBean(String phoneNum, long up_flow, long d_flow) {
        this.phoneNum = phoneNum;
        this.up_flow = up_flow;
        this.d_flow = d_flow;
        this.s_flow = up_flow+d_flow;
    }

    @Override
    public String toString() {
        return ""+up_flow+"\t"+d_flow+"\t"+s_flow;
    }

    //将对象序列化到流中
    @Override
    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeUTF(phoneNum);
        dataOutput.writeLong(up_flow);
        dataOutput.writeLong(d_flow);
        dataOutput.writeLong(s_flow);


    }

    //数据流中反序列出对象的数据
    @Override
    public void readFields(DataInput dataInput) throws IOException {

        phoneNum=dataInput.readUTF();
        up_flow=dataInput.readLong();
        d_flow=dataInput.readLong();
        s_flow=dataInput.readLong();
    }



    @Override
    public int compareTo(FlowBean o) {
        return s_flow>o.getS_flow()?-1:1;
    }
}
