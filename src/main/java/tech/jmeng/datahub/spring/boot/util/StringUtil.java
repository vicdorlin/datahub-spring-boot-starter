package tech.jmeng.datahub.spring.boot.util;

import com.alibaba.fastjson.JSON;
import org.apache.commons.codec.binary.Base64;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * String处理工具类
 *
 * @author linx 2019-12-23 0:59
 */
public class StringUtil {
    private static Pattern EMAIL_PATTERN = Pattern.compile("\\w+([-+.]\\w+)*@\\w+([-.]\\w+)*\\.\\w+([-.]\\w+)*");

    /**
     * 手机格式验证
     *
     * @param tel
     * @return
     */
    public static boolean isTel(String tel) {
        return tel.matches("^1[0-9][0-9]\\d{8}$");
    }

    /**
     * 检测邮箱地址是否合法
     *
     * @param email
     * @return true合法 false不合法
     */
    public static boolean isEmail(String email) {
        if (null == email || "".equals(email)) {
            return false;
        }
        Matcher m = EMAIL_PATTERN.matcher(email);
        return m.matches();
    }

    /**
     * 判断字符串是否是纯数字
     *
     * @param str
     * @return
     */
    public static boolean isNumeric(String str) {
        if (str == null || str.length() == 0) {
            return false;
        }
        return str.matches("^[0-9]*$");
    }

    /**
     * 判断字符串为空
     *
     * @param str
     * @return
     */
    public static boolean isEmpty(String str) {
        return (str == null) || (str.trim().length() == 0);
    }

    /**
     * 判断字符串是否不为空
     *
     * @param str
     * @return
     */
    public static boolean isExist(String str) {
        return !isEmpty(str);
    }

    /**
     * encode url
     *
     * @param content
     * @return
     */
    public static String urlEncode(String content) {
        try {
            return URLEncoder.encode(content, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            return "";
        }
    }

    public static String base64Encode(String content) {
        return Base64.encodeBase64String(content.getBytes());
    }

    /**
     * 返回非空字符串
     *
     * @param word
     * @return
     */
    public static String nullToEmpty(String word) {
        return word != null ? word : "";
    }

    /**
     * Mark key msg string.
     *
     * @param msg   the msg
     * @param color the color
     * @param marks the marks
     * @return the string
     */
    public static String markKeyMsg(String msg, String color, String... marks) {
        for (String mark : marks) {
            if (!msg.contains(mark)) {
                continue;
            }
            int markIndex = msg.indexOf(mark);
            int startIndex = markIndex - 1;
            int endIntex = markIndex + mark.length();
            for (; startIndex >= 0 && Character.isDigit(msg.charAt(startIndex)); startIndex--) {
            }
            if (++startIndex == markIndex) {
                continue;
            }
            String prefixMsg = msg.substring(0, startIndex);
            String suffixMsg = msg.substring(endIntex);
            String content = "<font color='" + color + "'>" + msg.substring(startIndex, endIntex) + "</font>";

            msg = prefixMsg + content + suffixMsg;
        }
        return msg;
    }

    /**
     * 可以理解为万用toString
     *
     * @param t
     * @param <T>
     * @return
     */
    public static <T> String toString(T t) {
        if (t == null) {
            return null;
        }
        if (t instanceof String) {
            return (String) t;
        }
        if (t instanceof Date) {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(t);
        }
        if (t instanceof Number || t instanceof Character || t instanceof Boolean) {
            return String.valueOf(t);
        }
        return JSON.toJSONString(t);
    }

    /**
     * List转String
     *
     * @param list
     * @param separator 分隔符
     * @return
     */
    public static String listToString(List<?> list, char separator) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < list.size(); i++) {
            sb.append(list.get(i)).append(separator);
        }
        return sb.toString().substring(0, sb.toString().length() - 1);
    }

    /**
     * 为字符串打上一连串的码
     *
     * @param words  待操作字符串
     * @param mark   替换标识{如：*}
     * @param start  打码起始位置
     * @param length 打码长度
     * @return
     */
    public static String markCenter(String words, String mark, int start, int length) {
        String ex = "(?<=[\\S]{" + start + "})\\S(?=[\\S]{" + (words.length() - start - length) + "})";
        return words.replaceAll(ex, mark);
    }

    /**
     * 为字符串两边打码，仅留中间部分
     *
     * @param words         待操作字符串
     * @param mark          替换标识{如：*}
     * @param markNumBefore 前标识位数
     * @param markNumAfter  后标识位数
     * @return
     */
    public static String markSides(String words, String mark, int markNumBefore, int markNumAfter) {
        return markCenter(markCenter(words, mark, 0, markNumBefore), mark, words.length() - markNumAfter, markNumAfter);
    }

}
