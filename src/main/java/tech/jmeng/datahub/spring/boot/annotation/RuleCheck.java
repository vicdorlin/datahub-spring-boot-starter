package tech.jmeng.datahub.spring.boot.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 参数非空校验
 *
 * @author linx 2019-12-24 12:57
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface RuleCheck {
    /**
     * 是否校验全部 默认开启
     * 注意：校验全部参数，全部字段（{@link RuleCheck#all()}和{@link RuleCheck#argNames()}配置会生效 其余无效）
     *
     * @return
     */
    boolean all() default true;

    /**
     * 针对all配置为true的方法，如果有多个参数
     * 只对argsRule配置为1的参数进行bean全字段非空校验
     * 配置为0的不校验
     * 配置为字符串的直接进行对象非空校验，并按字符串内容进行提示
     * 逗号间隔配置 例如："1,1,username,0,0" 1表示校验bean字段 0和其它数字或者username表示仅检验参数本身
     * 注意：配置长度需要与参数长度保持一致，并且顺序对应
     *
     * @return
     */
    String argNames() default "all";

    /**
     * 校验第几个参数
     *
     * @return
     */
    int index() default 0;

    /**
     * 校验方法中index位置对应的参数总由fields(英文逗号间隔)指定的字段
     * 例如：“id,name,password”
     * 注意：如果配置的fields和{@link RuleCheck#excepts()}存在重叠
     * 重叠部分将被排除
     *
     * @return
     */
    String fields() default "all";

    /**
     * excepts中指定的字段不会进行校验
     * 例如"createTime,modifyTime,del"
     * 注意：如果配置的fields和{@link RuleCheck#excepts()}存在重叠
     * 重叠部分将被排除
     *
     * @return
     */
    String excepts() default "null";

    /**
     * 是否限制空字符串
     *
     * @return
     */
    boolean checkEmptyStringOrSet() default false;
}
