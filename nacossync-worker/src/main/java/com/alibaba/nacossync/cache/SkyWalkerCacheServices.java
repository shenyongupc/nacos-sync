/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.nacossync.cache;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.nacossync.constant.ClusterTypeEnum;
import com.alibaba.nacossync.dao.ClusterAccessService;
import com.alibaba.nacossync.exception.SkyWalkerException;
import com.alibaba.nacossync.pojo.FinishedTask;
import com.alibaba.nacossync.pojo.model.ClusterDO;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.alibaba.nacossync.util.SkyWalkerUtil;

/**
 * @author NacosSync
 * @version $Id: SkyWalkerCacheServices.java, v 0.1 2018-09-27 上午2:47 NacosSync Exp $$
 */
@Service
public class SkyWalkerCacheServices {

    @Autowired
    private ClusterAccessService clusterAccessService;

    private static Map<String, FinishedTask> finishedTaskMap = new ConcurrentHashMap<>();

    public String getClusterConnectKey(String clusterId) {

        ClusterDO clusterDOS = clusterAccessService.findByClusterId(clusterId);

        List<String> connectKeyList = JSONObject.parseObject(clusterDOS.getConnectKeyList(),
                new TypeReference<List<String>>() {
                });

        if (CollectionUtils.isEmpty(connectKeyList)) {
            throw new SkyWalkerException("getClusterConnectKey empty, clusterId:" + clusterId);
        }

        Random random = new Random();
        return connectKeyList.get(random.nextInt(connectKeyList.size()));
    }

    public ClusterTypeEnum getClusterType(String clusterId) {

        ClusterDO clusterDOS = clusterAccessService.findByClusterId(clusterId);

        return ClusterTypeEnum.valueOf(clusterDOS.getClusterType());
    }

    public void addFinishedTask(TaskDO taskDO) {

        String operationId = SkyWalkerUtil.getOperationId(taskDO);

        FinishedTask finishedTask = new FinishedTask();
        finishedTask.setOperationId(operationId);

        finishedTaskMap.put(operationId, finishedTask);

    }

    public FinishedTask getFinishedTask(TaskDO taskDO) {

        String operationId = SkyWalkerUtil.getOperationId(taskDO);

        if (StringUtils.isEmpty(operationId)) {
            return null;
        }

        return finishedTaskMap.get(operationId);

    }

    public Map<String, FinishedTask> getFinishedTaskMap() {

        return finishedTaskMap;
    }

}
