package tech.jmeng.datahub.spring.boot.util;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.util.ArrayList;
import java.util.List;

/**
 * Bean处理工具类
 *
 * @author linx 2019-12-25 10:48
 */
public class BeanUtil {
    /**
     * 得到一个Bean类型的所有字段名
     *
     * @param clazz
     * @return
     */
    public static List<String> extractFieldNames(Class<?> clazz) {
        List<String> fieldNames = new ArrayList<>();
        try {
            while (clazz != null) {
                BeanInfo beanInfo = Introspector.getBeanInfo(clazz);
                PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();
                for (PropertyDescriptor property : propertyDescriptors) {
                    fieldNames.add(property.getName());
                }
                fieldNames.remove("class");
                clazz = clazz.getSuperclass();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return fieldNames;
    }
}
