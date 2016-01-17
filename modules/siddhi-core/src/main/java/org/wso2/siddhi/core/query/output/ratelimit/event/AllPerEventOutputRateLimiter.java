/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.siddhi.core.query.output.ratelimit.event;

import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.query.output.ratelimit.OutputRateLimiter;


public class AllPerEventOutputRateLimiter extends OutputRateLimiter {

    private final Integer value;
    private String id;

    private volatile int counter = 0;
    private ComplexEventChunk<ComplexEvent> allComplexEventChunk;

    public AllPerEventOutputRateLimiter(String id, Integer value) {
        this.id = id;
        this.value = value;
        allComplexEventChunk = new ComplexEventChunk<ComplexEvent>();
    }

    @Override
    public OutputRateLimiter clone(String key) {
        AllPerEventOutputRateLimiter instance = new AllPerEventOutputRateLimiter(id + key, value);
        instance.setLatencyTracker(latencyTracker);
        return instance;
    }

    @Override
    public void process(ComplexEventChunk complexEventChunk) {
        complexEventChunk.reset();
        while (complexEventChunk.hasNext()) {
            ComplexEvent event = complexEventChunk.next();
            complexEventChunk.remove();
            allComplexEventChunk.add(event);
            counter++;
            if (counter == value) {
                sendEvents();
            }
        }
    }

    @Override
    public void start() {
        //Nothing to start
    }

    @Override
    public void stop() {
        //Nothing to stop
    }

    private void sendEvents() {
        ComplexEventChunk<ComplexEvent> complexEventChunk = new ComplexEventChunk<ComplexEvent>();
        complexEventChunk.add(allComplexEventChunk.getFirst());
        allComplexEventChunk.clear();
        counter = 0;
        sendToCallBacks(complexEventChunk);
    }

    @Override
    public Object[] currentState() {
        return new Object[]{allComplexEventChunk, counter};
    }

    @Override
    public void restoreState(Object[] state) {
        allComplexEventChunk = (ComplexEventChunk<ComplexEvent>) state[0];
        counter = (Integer) state[1];
    }

}
