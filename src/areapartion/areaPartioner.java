package areapartion;/**
 * Created with IntelliJ IDEA
 *
 * @Author: mocas
 * @Date: 2020/7/24 20:33
 * @email: wangyuhang_mocas@163.com
 */

import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

/**
 *@program: weekend01
 *@description:
 *@author: mocas_wang
 *@create: 2020-07-24 20:33
 */
public class areaPartioner<KEY,VALUES> extends Partitioner<KEY,VALUES> {

    private static HashMap<String,Integer> areaMap= new HashMap<>();

    static {
        //loadTableToareaMap(areaMap);
        areaMap.put("135",0);
        areaMap.put("136",1);
        areaMap.put("137",2);
        areaMap.put("138",3);
        areaMap.put("139",4);

    }
    @Override
    public int getPartition(KEY key, VALUES values, int i) {
        //从key中拿到手机号，查询手机归属地，不同的省份返回不同的组号
        int areaCode=areaMap.get(key.toString().substring(0,3))==null?5:areaMap.get(key.toString().substring(0,3));
        return areaCode;
    }
}
