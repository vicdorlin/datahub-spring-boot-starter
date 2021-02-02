package tech.jmeng.datahub.spring.boot.datahub.vo.request;

import lombok.*;
import tech.jmeng.datahub.spring.boot.datahub.vo.BaseRequestVo;

/**
 * datahub project创建参数类
 *
 * @author linx 2020-09-01 4:36 下午
 */
@NoArgsConstructor
@AllArgsConstructor
@Setter
@Getter
@Builder
public class ProjectParamRequestVo extends BaseRequestVo {
    private String projectName;
    private String comment;
}
