package tech.jmeng.datahub.spring.boot.enums;

import lombok.Getter;

/**
 * @author linx 2019-12-22 1:08
 */
@Getter
public enum ErrorCodeEnum {
    ERC_SUCCESS(100, "success"),
    ERC_ERROR(2000, "位置错误"),
    ERC_DATAHUB_ERROR(5000, "datahub error"),
    ERC_PARAM_ERROR(1000, "参数错误"),
    ERC_OFFSET_RESET(10000,"点位被重置"),
    ;
    private Integer code;
    private String desc;

    private ErrorCodeEnum(Integer code, String desc) {
        this.code = code;
        this.desc = desc;
    }
}
