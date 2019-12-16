package com.alibaba.nacossync.extension.holder;

import com.alibaba.nacossync.cache.SystemConfigCache;
import com.alibaba.nacossync.util.StringUtils;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.utils.Utils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.function.Supplier;

/**
 * @author shenyong
 * @date 2019/12/14
 */
@Service
@Slf4j
public class KubernetesServerHolder extends AbstractServerHolder<KubernetesClient> {

    @Override
    KubernetesClient createServer(String clusterId, Supplier<String> serverAddressSupplier, String namespace) throws Exception {
        checkConfig();
        String serverAddress = serverAddressSupplier.get();
        Config base = Config.autoConfigure(null);
        Config config = new ConfigBuilder(base)
                .withMasterUrl(serverAddress)
                .withUsername(SystemConfigCache.systemConfigMap.get(Config.KUBERNETES_AUTH_BASIC_USERNAME_SYSTEM_PROPERTY))
                .withPassword(SystemConfigCache.systemConfigMap.get(Config.KUBERNETES_AUTH_BASIC_PASSWORD_SYSTEM_PROPERTY))
                .withNamespace(namespace)
                .withTrustCerts(true)
                .build();
        return new DefaultKubernetesClient(config);
    }

    private void checkConfig() throws Exception {
        String username = Utils.getSystemPropertyOrEnvVar(Config.KUBERNETES_AUTH_BASIC_USERNAME_SYSTEM_PROPERTY,
                SystemConfigCache.systemConfigMap.get(Config.KUBERNETES_AUTH_BASIC_USERNAME_SYSTEM_PROPERTY));
        String password = Utils.getSystemPropertyOrEnvVar(Config.KUBERNETES_AUTH_BASIC_PASSWORD_SYSTEM_PROPERTY,
                SystemConfigCache.systemConfigMap.get(Config.KUBERNETES_AUTH_BASIC_PASSWORD_SYSTEM_PROPERTY));
        if(StringUtils.isEmpty(username)) {
            log.error("no kubernetes system config [kubernetes.auth.basic.username] find, please config in table system_config with config_key->config_value[kubernetes.auth.basic.username->value].");
            throw new Exception("no kubernetes system config [kubernetes.auth.basic.username] find, please config in table system_config with config_key->config_value[kubernetes.auth.basic.username->value].");
        }
        if(StringUtils.isEmpty(password)) {
            log.error("no kubernetes system config [kubernetes.auth.basic.password] find, please config in table system_config with config_key->config_value[kubernetes.auth.basic.password->value].");
            throw new Exception("no kubernetes system config [kubernetes.auth.basic.password] find, please config in table system_config with config_key->config_value[kubernetes.auth.basic.password->value].");
        }
    }
}
