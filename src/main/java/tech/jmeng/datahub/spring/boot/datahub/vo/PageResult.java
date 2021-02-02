package tech.jmeng.datahub.spring.boot.datahub.vo;

import lombok.Getter;

import java.util.List;

/**
 * 分页返回结果类
 *
 * @author linx 2019-12-22 0:18
 */
@Getter
public class PageResult<T> extends BaseVo {
    private int page;
    private int size;
    private long total;
    private int totalPage;
    private List<T> list;

    public PageResult(int page, int size, long total, List<T> list) {
        this.page = page;
        this.size = size;
        this.total = total;
        this.list = list;
        this.totalPage = (int) (total % size > 0 ? total / size + 1 : total / size);
    }

    public PageResult(PageRequest request, long total, List<T> list) {
        this(request.getPage(), request.getSize(), total, list);
    }
}
