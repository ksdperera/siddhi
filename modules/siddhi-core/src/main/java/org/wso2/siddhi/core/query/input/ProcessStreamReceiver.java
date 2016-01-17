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
package org.wso2.siddhi.core.query.input;

import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.event.stream.MetaStreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventPool;
import org.wso2.siddhi.core.event.stream.converter.ConversionStreamEventChunk;
import org.wso2.siddhi.core.query.input.stream.state.PreStateProcessor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.stream.StreamJunction;
import org.wso2.siddhi.core.util.statistics.LatencyTracker;

import java.util.ArrayList;
import java.util.List;

public class ProcessStreamReceiver implements StreamJunction.Receiver {

    protected String streamId;
    protected Processor next;
    private ConversionStreamEventChunk streamEventChunk;
    private MetaStreamEvent metaStreamEvent;
    private StreamEventPool streamEventPool;
    protected List<PreStateProcessor> stateProcessors = new ArrayList<PreStateProcessor>();
    protected int stateProcessorsSize;
    protected LatencyTracker latencyTracker;

    public ProcessStreamReceiver(String streamId, LatencyTracker latencyTracker) {
        this.streamId = streamId;
        this.latencyTracker = latencyTracker;
    }

    @Override
    public String getStreamId() {
        return streamId;
    }

    public ProcessStreamReceiver clone(String key) {
        return new ProcessStreamReceiver(streamId + key, latencyTracker);
    }

    private void process() {
        if (latencyTracker != null) {
            try {
                latencyTracker.markIn();
                processAndClear(streamEventChunk);
            } finally {
                latencyTracker.markOut();
            }
        } else {
            processAndClear(streamEventChunk);
        }
    }

    @Override
    public void receive(ComplexEvent complexEvent) {
        streamEventChunk.convertAndAssign(complexEvent);
        process();
    }

    @Override
    public void receive(Event event) {
        streamEventChunk.convertAndAssign(event);
        process();
    }

    @Override
    public void receive(Event[] events) {
        streamEventChunk.convertAndAssign(events);
        process();
    }


    @Override
    public void receive(Event event, boolean endOfBatch) {
        streamEventChunk.convertAndAdd(event);
        if (endOfBatch) {
            process();
        }
    }

    @Override
    public void receive(long timeStamp, Object[] data) {
        streamEventChunk.convertAndAssign(timeStamp, data);
        process();

    }

    protected void processAndClear(ComplexEventChunk<StreamEvent> streamEventChunk) {
        if (stateProcessorsSize != 0) {
            stateProcessors.get(0).updateState();
        }
        next.process(streamEventChunk);
        streamEventChunk.clear();
    }

    public void setMetaStreamEvent(MetaStreamEvent metaStreamEvent) {
        this.metaStreamEvent = metaStreamEvent;
    }

    public boolean toTable() {
        return metaStreamEvent.isTableEvent();
    }

    public void setNext(Processor next) {
        this.next = next;
    }

    public void setStreamEventPool(StreamEventPool streamEventPool) {
        this.streamEventPool = streamEventPool;
    }

    public void init() {
        streamEventChunk = new ConversionStreamEventChunk(metaStreamEvent, streamEventPool);
    }

    public void addStatefulProcessor(PreStateProcessor stateProcessor) {
        stateProcessors.add(stateProcessor);
        stateProcessorsSize = stateProcessors.size();
    }
}
