package tech.jmeng.datahub.spring.boot.aspect;

import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import tech.jmeng.datahub.spring.boot.annotation.RuleCheck;
import tech.jmeng.datahub.spring.boot.util.RuleCheckUtil;
import tech.jmeng.datahub.spring.boot.util.StringUtil;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 参数规则校验切面
 *
 * @author linx 2019-12-24 12:59
 */
@Aspect
@Component
@Slf4j
@Order(1)
public class RuleCheckAspect {
    public static final String ALL = "all";
    public static final String NULL = "null";

    @Pointcut("@annotation(tech.jmeng.datahub.spring.boot.annotation.RuleCheck)")
    public void ruleCheck() {
    }

    @Before("ruleCheck()")
    public void ruleCheckBefore(JoinPoint joinPoint) {
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        RuleCheck annotation = signature.getMethod().getAnnotation(RuleCheck.class);
        //得到方法的参数的类型
        Object[] arguments = joinPoint.getArgs();
        boolean checkAll = annotation.all();
        if (checkAll) {
            String argNames = annotation.argNames();
            if (ALL.equals(argNames)) {
                for (Object argument : arguments) {
                    RuleCheckUtil.checkEmpty(argument);
                }
                return;
            }
            String[] split = argNames.split(",");
            if (arguments.length != split.length) {
                throw new RuntimeException("参数长度不匹配");
            }
            for (int i = 0; i < split.length; i++) {
                String s = split[i];
                if (!StringUtil.isNumeric(s)) {
                    RuleCheckUtil.checkEmpty(arguments[i], s, annotation.checkEmptyStringOrSet());
                    return;
                }
                int idx = Integer.parseInt(s);
                if (idx == 1) {
                    RuleCheckUtil.checkEmpty(arguments[i]);
                    return;
                } else {
                    RuleCheckUtil.checkEmpty(arguments[i], arguments[i].getClass().getSimpleName(), annotation.checkEmptyStringOrSet());
                }
            }
            return;
        }
        int checkIndex = annotation.index();
        String fields = annotation.fields();
        String exceptFields = annotation.excepts();
        List<String> checkFieldList = null;
        Set<String> exceptFieldSet = null;
        if (!ALL.equals(fields)) {
            checkFieldList = Arrays.asList(fields.split(","));
        }
        if (!NULL.equals(exceptFields)) {
            exceptFieldSet = new HashSet<>(Arrays.asList(exceptFields.split(",")));
        }
        RuleCheckUtil.checkEmpty(arguments[checkIndex], checkFieldList, exceptFieldSet, annotation.checkEmptyStringOrSet());
    }
}
