package tech.jmeng.datahub.spring.boot.conf;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import tech.jmeng.datahub.spring.boot.datahub.DataHubOperator;
import tech.jmeng.datahub.spring.boot.prop.DataHubProperties;

/**
 * @author linx 2020-11-30 下午4:16
 */
@Configuration
@EnableConfigurationProperties(DataHubProperties.class)
@ConditionalOnClass(DataHubOperator.class)
@ConditionalOnProperty(prefix = "spring.datahub", value = "enabled", matchIfMissing = true)
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class DataHubAutoConfiguration {
    private final DataHubProperties dataHubProperties;

    @Bean
    @ConditionalOnMissingBean(DataHubOperator.class)
    public DataHubOperator dataHubOperator() {
        return new DataHubOperator(dataHubProperties);
    }

}
