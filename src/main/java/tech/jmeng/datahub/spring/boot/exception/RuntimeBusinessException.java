package tech.jmeng.datahub.spring.boot.exception;

import lombok.Getter;
import lombok.Setter;
import tech.jmeng.datahub.spring.boot.enums.ErrorCodeEnum;

import static tech.jmeng.datahub.spring.boot.enums.ErrorCodeEnum.ERC_ERROR;

/**
 * @author linx 2020-01-08 20:13
 */
@Setter
@Getter
public class RuntimeBusinessException extends RuntimeException {
    private static final long serialVersionUID = -3723062570829361085L;
    private Integer code;

    private String message;

    public RuntimeBusinessException(Integer code, String message) {
        this.code = code;
        this.message = message;
    }

    public RuntimeBusinessException(ErrorCodeEnum errorCodeEnum) {
        this(errorCodeEnum.getCode(), errorCodeEnum.getDesc());
    }

    public RuntimeBusinessException(String message) {
        this(ERC_ERROR.getCode(), message);
    }

    public RuntimeBusinessException() {
        this(ERC_ERROR);
    }
}