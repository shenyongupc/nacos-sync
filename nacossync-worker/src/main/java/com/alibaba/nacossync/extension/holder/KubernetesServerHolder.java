package com.alibaba.nacossync.extension.holder;

import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
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

    private static final String HTTPS = "https://";

    @Override
    KubernetesClient createServer(String clusterId, Supplier<String> serverAddressSupplier, String namespace) {
        String serverAddress = serverAddressSupplier.get();
        serverAddress = serverAddress.startsWith(HTTPS) ? serverAddress : HTTPS + serverAddress;
        Config base = Config.autoConfigure(null);
        Config config = new ConfigBuilder(base)
                .withMasterUrl(serverAddress)
                .withUsername("admin")
                .withPassword("admin")
                .withNamespace(namespace)
                .withTrustCerts(true)
                .build();
        return new DefaultKubernetesClient(config);
    }
}
