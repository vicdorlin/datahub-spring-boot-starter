package tech.jmeng.datahub.spring.boot.datahub.vo;

import lombok.Getter;
import lombok.Setter;

/**
 * 分页请求参数类
 *
 * @author linx 2019-12-22 0:25
 */
@Setter
@Getter
public class PageRequest<T> extends BaseVo {
    private Integer page = 1;
    private Integer size = 20;
    private T filter;
}
