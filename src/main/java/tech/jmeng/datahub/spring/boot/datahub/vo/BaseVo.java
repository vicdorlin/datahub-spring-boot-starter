package tech.jmeng.datahub.spring.boot.datahub.vo;

import com.alibaba.fastjson.JSON;
import org.springframework.beans.BeanUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 统一基类
 *
 * @author linx 2019-12-21 23:38
 */
public class BaseVo implements Serializable {
    private static final long serialVersionUID = -1L;

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }

    /**
     * 将非空对象中的非空属性值拷贝到当前对象
     *
     * @param t   如果空将不会copy
     * @param <T>
     */
    public <T> void copyFrom(T t) {
        if (t == null) {
            return;
        }
        BeanUtils.copyProperties(t, this);
    }

    /**
     * 把自身属性拷贝到其他变量中,一般比正常拷贝慢，不过比对象映射快，尽量不使用。
     */
    public <T> T copyTo(Class<T> clz) {
        T t = createBean(clz);
        BeanUtils.copyProperties(this, t);
        return t;
    }

    /**
     * 拷贝一个列表
     */
    public static <A extends BaseVo, B extends BaseVo> List<B> copyList(List<A> sources, Class<B> clz) {
        if (sources == null) {
            return null;
        }
        List<B> list = new ArrayList<>();
        if (sources.size() == 0) {
            return list;
        }
        sources.stream().forEach(a -> list.add(a.copyTo(clz)));
        return list;
    }

    public static <T> T createBean(Class<T> clazz) {
        if (clazz == null) {
            throw new RuntimeException("createBean失败:clazz参数缺失");
        }
        try {
            return clazz.newInstance();
        } catch (IllegalAccessException e) {
            throw new RuntimeException(String.format("createBean失败:无法访问%s类中的无参构造器", clazz.getSimpleName()));
        } catch (InstantiationException e) {
            throw new RuntimeException(String.format("createBean失败:请检查%s类中是否存在无参构造器", clazz.getSimpleName()));
        }
    }

}
