package hadoop.rpc;
/**
 * Created with IntelliJ IDEA
 *
 * @Author: mocas
 * @Date: 2020/7/19 14:51
 * @email: wangyuhang_mocas@163.com
 */


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 *@program: weekend01
 *@description: login
 *@author: mocas_wang
 *@create: 2020-07-19 14:51
 */
public class loginCtronller {
    public static void main(String[] args) throws IOException {
        loginServiceInterface proxy=RPC.getProxy(loginServiceInterface.class,1L,
                new InetSocketAddress("weekend01",10000),new Configuration());
        String result=proxy.login("baby","123456");
        System.out.println(result);


    }

}
