package tech.jmeng.datahub.spring.boot.util;



import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Map;


/**
 * 公共工具类
 *
 * @author linx 2020-10-22 15:58:00
 */
public class CommonUtil {
    /**
     * 判断对象是否不为空
     *
     * @param o
     * @return
     */
    public static boolean exist(Object o) {
        if (o == null) {
            return false;
        }
        if (o instanceof String) {
            return ((String) o).trim().length() > 0;
        }
        if (o instanceof Collection) {
            return ((Collection) o).size() > 0;
        }
        if (o instanceof Map) {
            return ((Map) o).size() > 0;
        }
        if (o instanceof Object[]) {
            return ((Object[]) o).length > 0;
        }
        return true;
    }

    /**
     * 判断对象是否为空
     *
     * @param o
     * @return
     */
    public static boolean notExist(Object o) {
        return !exist(o);
    }

    /**
     * 判断一个集合是否不存在非空子集
     *
     * @param collection
     * @return
     */
    @SuppressWarnings("rawtypes")
    public static boolean isSetEmpty(Collection collection) {
        return collection == null || collection.size() == 0;
    }

    /**
     * 判断一个集合是否存在非空子集
     *
     * @param collection
     * @return
     */
    @SuppressWarnings("rawtypes")
    public static boolean isSetNotEmpty(Collection collection) {
        return !isSetEmpty(collection);
    }

    /**
     * 获取本机ip地址
     *
     * @return
     */
    public static String getIp() {
        try {
            InetAddress ia = InetAddress.getLocalHost();
            //获取计算机IP
            return ia.getHostAddress();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

}
