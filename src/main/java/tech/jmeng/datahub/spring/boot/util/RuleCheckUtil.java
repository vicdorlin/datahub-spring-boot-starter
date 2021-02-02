package tech.jmeng.datahub.spring.boot.util;

import lombok.extern.slf4j.Slf4j;
import org.springframework.util.CollectionUtils;
import tech.jmeng.datahub.spring.boot.exception.RuntimeBusinessException;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.util.*;

import static tech.jmeng.datahub.spring.boot.util.BeanUtil.extractFieldNames;

/**
 * 参数规则校验处理工具类
 *
 * @author linx 2016-06-22 13:35
 */
@Slf4j
public final class RuleCheckUtil {
    /**
     * 空验证
     *
     * @param key
     * @param msg
     */
    public static final void checkEmpty(Object key, String msg, boolean checkEmptyStringOrSet) {
        if (key == null) {
            throw new RuntimeBusinessException("参数:" + msg + "不能为空");
        }

        if (checkEmptyStringOrSet) {
            if (key instanceof String && ((String) key).length() == 0) {
                throw new RuntimeBusinessException("参数:" + msg + "不能为空字符串");
            }

            if (key instanceof Collection && ((Collection) key).size() == 0) {
                throw new RuntimeBusinessException("参数:" + msg + "不能为空集合");
            }

            if (key instanceof Map && ((Map) key).size() == 0) {
                throw new RuntimeBusinessException("参数:" + msg + "不能为空字典");
            }
        }
    }

    public static final void checkEmpty(Object javaBean, boolean checkEmptyStringOrSet, Object... fields) {
        if (fields != null && fields.length > 0) {
            List<String> keywords = new ArrayList<>();
            for (Object field : fields) {
                keywords.add((String) field);
            }
            checkEmpty(javaBean, keywords, checkEmptyStringOrSet);
            return;
        }
        checkEmpty(javaBean);
    }

    public static final void checkEmpty(Object javaBean, List<String> keywords, boolean checkEmptyStringOrSet) {
        checkEmpty(javaBean, keywords, null, checkEmptyStringOrSet);
    }

    /**
     * 参数验证存在非空即通过
     *
     * @param objs
     */
    public static final void checkExist(String msg, Object... objs) {
        if (CommonUtil.exist(objs)) {
            Arrays.stream(objs).forEach(obj -> {
                if (obj != null) {
                    return;
                }
            });
        }
        throw new RuntimeBusinessException("参数:" + msg + "不能同时为空");
    }

    /**
     * 简化后的空验证 2
     *
     * @param javaBean 验证所有声明的字段（包括父类中的字段)
     */
    public static final <B> void checkEmpty(B javaBean) {
        checkEmpty(javaBean, false);
    }

    public static final <B> void checkEmpty(B javaBean, boolean checkEmptyStringOrSet) {
        checkEmpty(javaBean, null, null, checkEmptyStringOrSet);
    }

    public static final <B> void checkEmpty(B javaBean, List<String> keywords, Set<String> exceptWords, boolean checkEmptyStringOrSet) {
        if (javaBean == null) {
            throw new RuntimeBusinessException("参数不能为空");
        }
        if (CollectionUtils.isEmpty(keywords)) {
            keywords = extractFieldNames(javaBean.getClass());
        }
        if (CollectionUtils.isEmpty(keywords)) {
            return;
        }
        if (CollectionUtils.isEmpty(exceptWords)) {
            exceptWords = new HashSet<>();
        }
        try {
            Class clazz = javaBean.getClass();
            for (String keyWord : keywords) {
                if (exceptWords.contains(keyWord)) {
                    continue;
                }
                PropertyDescriptor pd = new PropertyDescriptor(keyWord, clazz);
                Method getMethod = pd.getReadMethod();//获得keyWord在obj中对应的getter方法
                Object o = getMethod.invoke(javaBean);
                checkEmpty(o, keyWord, checkEmptyStringOrSet);//空验证
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static final <B> void checkEmpty(B javaBean, List<String> keywords, Set<String> exceptWords) {
        checkEmpty(javaBean, keywords, exceptWords, false);
    }
}
