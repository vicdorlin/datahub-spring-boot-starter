package tech.jmeng.datahub.spring.boot.datahub.vo;

import lombok.*;

import java.util.Map;

/**
 * Http请求参数类
 * @author linx 2020-08-05 4:52 下午
 */
@Setter
@Getter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class HttpRequestParam extends BaseVo {
    private String url;
    private Map<String, String> param;
    private Map<String, String> headerMap;
    private String json;
    private boolean checkProxy;
    private String proxyHost;
    private Integer proxyPort;
}
