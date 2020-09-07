/**
 * Created with IntelliJ IDEA
 *
 * @Author: mocas
 * @Date: 2020/7/25 9:25
 * @email: wangyuhang_mocas@163.com
 */

import java.util.ArrayList;
import java.util.Collection;

/**
 *@program: weekend01
 *@description:
 *@author: mocas_wang
 *@create: 2020-07-25 09:25
 */
public class test {
    public static void main(String[] args) {
        //创建Collection集合1
        Collection c1 = new ArrayList() ;

        //添加元素
        c1.add("abc1") ;
        c1.add("abc2") ;
        c1.add("abc3") ;
        c1.add("abc4") ;


        //创建第二个集合
        Collection c2 = new ArrayList() ;
        c2.add("abc1") ;
        c2.add("abc2") ;
        c2.add("abc3") ;
        c2.add("abc4") ;
        c2.add("abc5") ;
        c2.add("abc6") ;
        c2.add("abc7") ;
        System.out.println("未发生变化的数据:");
        System.out.println("c1:"+c1);
        System.out.println("c2:"+c2);
        System.out.println("--------------------------------");

        //boolean addAll(Collection c)  :添加一个集合中的所有元素
        System.out.println("addAll():"+c1.addAll(c2));   //将c2中的所有元素都添加到c1中
        System.out.println("c1:"+c1);
        System.out.println("c2:"+c2);
        System.out.println("--------------------------------");

        //boolean removeAll(Collection c):删除的高级功能(思考:删除一个算是删除还是删除所有算是删除?)
        //结论:删除一个算是删除...
        System.out.println("removeAll():"+c1.removeAll(c2));
        System.out.println("c1:"+c1);    //删除集合c2中的所有元素，并将删除元素后的集合赋给c1;
        System.out.println("c2:"+c2);
        System.out.println("--------------------------------");

        //boolean containsAll(Collection c):包含所有元素算是包含,还是包含一个算是包含
        //结论:包含所有算是包含
        System.out.println("containsAll():"+c1.containsAll(c2));
        System.out.println("c1:"+c1);    //c1的所有元素都要在c2中能找到才算包含，并将包含的元素赋给集合c1;
        System.out.println("c2:"+c2);
        System.out.println("--------------------------------");

        //boolean retainAll(Collection c):A集合对B集合取交集,交集的元素到底是去A集合还是去B集合中
        //返回值boolean表达什么意思?
        //结论:c1集合对c2集合取交集,交集的元素要存到c1集合中,boolean返回值表达的c1集合的元素是否发生变化,如果发生变化,则返回true,否则,返回false
        System.out.println("retainAll():"+c1.retainAll(c2));
        System.out.println("c1:"+c1);
        System.out.println("c2:"+c2);
    }

}
