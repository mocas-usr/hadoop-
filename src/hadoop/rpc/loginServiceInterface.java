package hadoop.rpc;

/**
 * Created with IntelliJ IDEA
 *
 * @Author: mocas
 * @Date: 2020/7/19 14:55
 * @email: wangyuhang_mocas@163.com
 */
public interface loginServiceInterface {
    public static final long versionID=1L;
    public String  login(String username,String password);
}
