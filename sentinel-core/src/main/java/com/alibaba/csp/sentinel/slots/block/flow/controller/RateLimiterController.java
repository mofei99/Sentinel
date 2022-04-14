/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.csp.sentinel.slots.block.flow.controller;

import java.util.concurrent.atomic.AtomicLong;

import com.alibaba.csp.sentinel.slots.block.flow.TrafficShapingController;

import com.alibaba.csp.sentinel.util.TimeUtil;
import com.alibaba.csp.sentinel.node.Node;

/**
 * @author jialiang.linjl
 */
public class RateLimiterController implements TrafficShapingController {
    //队列最大等待时间
    private final int maxQueueingTimeMs;
    //限流数
    private final double count;
    //最近通过请求的时间
    private final AtomicLong latestPassedTime = new AtomicLong(-1);

    public RateLimiterController(int timeOut, double count) {
        this.maxQueueingTimeMs = timeOut;
        this.count = count;
    }

    @Override
    public boolean canPass(Node node, int acquireCount) {
        return canPass(node, acquireCount, false);
    }

    @Override
    public boolean canPass(Node node, int acquireCount, boolean prioritized) {
        // 请求令牌小于0，直接通过
        if (acquireCount <= 0) {
            return true;
        }
        // count<=0直接通过
        // Otherwise,the costTime will be max of long and waitTime will overflow in some cases.
        if (count <= 0) {
            return false;
        }

        long currentTime = TimeUtil.currentTimeMillis();
        /**
         * 计算两个请求之间的间隔时间，单位:ms
         * 假设限流为1000QPS/秒，则间隔时间为1ms
         */
        long costTime = Math.round(1.0 * (acquireCount) / count * 1000);

        /**
         * 期望时间：上次请求通过时间+间隔时间
         * 期望通过时间<=currentTime则直接放行
         */
        long expectedTime = costTime + latestPassedTime.get();

        if (expectedTime <= currentTime) {
            // 可能存在并发问题，为了性能，可以接受
            // TODO: 2022/4/11  学习AtomicLong
            latestPassedTime.set(currentTime);
            return true;
        } else {
            // 计算等待时间
            long waitTime = costTime + latestPassedTime.get() - TimeUtil.currentTimeMillis();
            //如果大于最大排队时间，则直接拒绝
            if (waitTime > maxQueueingTimeMs) {
                return false;
            } else {
                //更新最近放行时间
                long oldTime = latestPassedTime.addAndGet(costTime);
                try {
                    waitTime = oldTime - TimeUtil.currentTimeMillis();
                    //二次校验
                    if (waitTime > maxQueueingTimeMs) {
                        latestPassedTime.addAndGet(-costTime);
                        return false;
                    }
                    /**
                     * 等待一段时间再放行
                     * 由于并发问题，此处waitTime可能小于等于0
                     */
                    if (waitTime > 0) {
                        Thread.sleep(waitTime);
                    }
                    return true;
                } catch (InterruptedException e) {
                }
            }
        }
        return false;
    }

}
