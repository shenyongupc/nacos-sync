package com.alibaba.nacossync.cache;

import com.alibaba.nacossync.dao.repository.SystemConfigRepository;
import com.alibaba.nacossync.pojo.model.SystemConfigDO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;

/**
 * @author shenyong
 * @date 2019/12/16
 */
@Component
@Slf4j
public class SystemConfigCache {

    public static Map<String, String> systemConfigMap = new HashMap<String, String>();

    private final SystemConfigRepository systemConfigRepository;

    public SystemConfigCache(SystemConfigRepository systemConfigRepository) {
        this.systemConfigRepository = systemConfigRepository;
    }

    @PostConstruct
    public void init() {
        log.info("系统启动中。。。加载systemConfigMap");
        Iterable<SystemConfigDO> systemConfigList = systemConfigRepository.findAll();
        for (SystemConfigDO systemConfig : systemConfigList) {
            systemConfigMap.put(systemConfig.getConfigKey(), systemConfig.getConfigValue());
        }
    }
}
