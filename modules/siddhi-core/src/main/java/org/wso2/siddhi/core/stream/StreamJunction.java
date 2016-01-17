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
package org.wso2.siddhi.core.stream;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.PhasedBackoffWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.apache.log4j.Logger;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.event.EventFactory;
import org.wso2.siddhi.core.stream.input.InputProcessor;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.SiddhiConstants;
import org.wso2.siddhi.core.util.statistics.ThroughputTracker;
import org.wso2.siddhi.core.util.statistics.metrics.SiddhiThroughputMetric;
import org.wso2.siddhi.query.api.annotation.Annotation;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.exception.DuplicateAnnotationException;
import org.wso2.siddhi.query.api.util.AnnotationHelper;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class StreamJunction {

    private static final Logger log = Logger.getLogger(StreamJunction.class);
    private List<Receiver> receivers = new CopyOnWriteArrayList<Receiver>();
    private List<Publisher> publishers = new CopyOnWriteArrayList<Publisher>();
    private ExecutorService executorService;
    private final ExecutionPlanContext executionPlanContext;
    private final StreamDefinition streamDefinition;
    private final int bufferSize;
    private Boolean parallel = null;
    private Disruptor<Event> disruptor;
    private RingBuffer<Event> ringBuffer;
    private ThroughputTracker throughputTracker = null;
    private boolean isTraceEnabled;

    public StreamJunction(StreamDefinition streamDefinition, ExecutorService executorService, int defaultBufferSize,
                          ExecutionPlanContext executionPlanContext) {
        this.streamDefinition = streamDefinition;
        this.bufferSize = defaultBufferSize;
        this.executorService = executorService;
        this.executionPlanContext = executionPlanContext;

        if (executionPlanContext.getStatisticsManager()!=null) {
            String metricName = executionPlanContext.getSiddhiContext().getStatisticsConfiguration().getMatricPrefix() + ".stream." + streamDefinition.getId();
            this.throughputTracker = executionPlanContext
                    .getSiddhiContext()
                    .getStatisticsConfiguration()
                    .getFactory()
                    .createThroughputTracker(metricName, executionPlanContext.getStatisticsManager());
        }

        try {
            Annotation annotation = AnnotationHelper.getAnnotation(SiddhiConstants.ANNOTATION_PARALLEL, streamDefinition.getAnnotations());
            if (annotation != null) {
                parallel = true;
            }
        } catch (DuplicateAnnotationException e) {
            throw new DuplicateAnnotationException(e.getMessage() + " for the same Stream " + streamDefinition.getId());
        }
        isTraceEnabled = log.isTraceEnabled();

    }

    public void sendEvent(ComplexEvent complexEvent) {
        if (isTraceEnabled) {
            log.trace("event is received by streamJunction " + this);
        }

        ComplexEvent complexEventList = complexEvent;
        if (disruptor != null) {
            while (complexEventList != null) {
                if (throughputTracker != null) {
                    throughputTracker.eventIn();
                }

                long sequenceNo = ringBuffer.next();
                try {
                    Event existingEvent = ringBuffer.get(sequenceNo);
                    existingEvent.copyFrom(complexEventList);
                } finally {
                    ringBuffer.publish(sequenceNo);
                }
                complexEventList = complexEventList.getNext();
            }

        } else {
            if (throughputTracker != null) {
                int messageCount = 0;
                while (complexEventList != null) {
                    messageCount++;
                    complexEventList = complexEventList.getNext();
                }
                throughputTracker.eventsIn(messageCount);
            }

            for (Receiver receiver : receivers) {
                receiver.receive(complexEvent);
            }
        }

    }

    public void sendEvent(Event event) {
        if (throughputTracker != null) {
            throughputTracker.eventIn();
        }

        if (isTraceEnabled) {
            log.trace(event + " event is received by streamJunction " + this);
        }
        if (disruptor != null) {
            long sequenceNo = ringBuffer.next();
            try {
                Event existingEvent = ringBuffer.get(sequenceNo);
                existingEvent.copyFrom(event);
            } finally {
                ringBuffer.publish(sequenceNo);
            }
        } else {
            for (Receiver receiver : receivers) {
                receiver.receive(event);
            }
        }

    }

    private void sendEvent(Event[] events) {
        if (throughputTracker != null) {
            throughputTracker.eventsIn(events.length);
        }

        if (isTraceEnabled) {
            log.trace("event is received by streamJunction " + this);
        }
        if (disruptor != null) {
            for (Event event : events) {   //todo optimize for arrays
                long sequenceNo = ringBuffer.next();
                try {
                    Event existingEvent = ringBuffer.get(sequenceNo);
                    existingEvent.copyFrom(event);
                } finally {
                    ringBuffer.publish(sequenceNo);
                }
            }
        } else {
            for (Receiver receiver : receivers) {
                receiver.receive(events);
            }
        }
    }

    private void sendEvent(List<Event> events) {
        if (isTraceEnabled) {
            log.trace("event is received by streamJunction " + this);
        }
        if (disruptor != null) {
            for (Event event : events) {   //todo optimize for arrays
                long sequenceNo = ringBuffer.next();
                try {
                    Event existingEvent = ringBuffer.get(sequenceNo);
                    existingEvent.copyFrom(event);
                } finally {
                    ringBuffer.publish(sequenceNo);
                }
            }
        } else {
            for (Receiver receiver : receivers) {
                receiver.receive(events.toArray(new Event[events.size()]));
            }
        }
    }

    private void sendData(long timeStamp, Object[] data) {
        if (throughputTracker != null) {
            throughputTracker.eventIn();
        }

        if (disruptor != null) {
            long sequenceNo = ringBuffer.next();
            try {
                Event existingEvent = ringBuffer.get(sequenceNo);
                existingEvent.setTimestamp(timeStamp);
                existingEvent.setIsExpired(false);
                System.arraycopy(data, 0, existingEvent.getData(), 0, data.length);
            } finally {
                ringBuffer.publish(sequenceNo);
            }
        } else {
            for (Receiver receiver : receivers) {
                receiver.receive(timeStamp, data);
            }
        }
    }

    /**
     * create and start disruptor based on annotations given in the streamDefinition
     */
    public synchronized void startProcessing() {
        if (!receivers.isEmpty()) {

            if (parallel == null) {
                parallel = executionPlanContext.isParallel();
            }
            if (parallel) {
                for (Constructor constructor : Disruptor.class.getConstructors()) {
                    if (constructor.getParameterTypes().length == 5) {      //if new disruptor classes available
                        ProducerType producerType = ProducerType.SINGLE;

                        if (publishers.size() > 1) {
                            producerType = ProducerType.MULTI;
                        }

                        disruptor = new Disruptor<Event>(new EventFactory(streamDefinition.getAttributeList().size()),
                                bufferSize, executorService, producerType, PhasedBackoffWaitStrategy.withLiteLock(1, 4, TimeUnit.SECONDS));
                        break;
                    }
                }
                if (disruptor == null) {
                    disruptor = new Disruptor<Event>(new EventFactory(streamDefinition.getAttributeList().size()),
                            bufferSize, executorService);
                }
                for (Receiver receiver : receivers) {
                    disruptor.handleEventsWith(new StreamHandler(receiver));
                }

                ringBuffer = disruptor.start();

            } else {
                for (Receiver receiver : receivers) {
                    if (receiver instanceof StreamCallback) {
                        ((StreamCallback) receiver).startProcessing();
                    }
                }
            }

        }
    }

    public synchronized void stopProcessing() {
        if (disruptor != null) {
            disruptor.shutdown();
        } else {
            for (Receiver receiver : receivers) {
                if (receiver instanceof StreamCallback) {
                    ((StreamCallback) receiver).stopProcessing();
                }
            }
        }
    }

    public synchronized Publisher constructPublisher() {
        Publisher publisher = new Publisher();
        publisher.setStreamJunction(this);
        publishers.add(publisher);
        return publisher;
    }

    public synchronized void subscribe(Receiver receiver) {
        //to have reverse order at the sequence/pattern processors
        if (!receivers.contains(receiver)) {
            receivers.add(receiver);
        }
    }

    public String getStreamId() {
        return streamDefinition.getId();
    }

    public StreamDefinition getStreamDefinition() {
        return streamDefinition;
    }

    public interface Receiver {

        public String getStreamId();

        public void receive(ComplexEvent complexEvent);

        public void receive(Event event);

        public void receive(Event event, boolean endOfBatch);

        public void receive(long timeStamp, Object[] data);

        public void receive(Event[] events);
    }

    public class StreamHandler implements EventHandler<Event> {

        private Receiver receiver;

        public StreamHandler(Receiver receiver) {
            this.receiver = receiver;
        }

        public void onEvent(Event event, long sequence, boolean endOfBatch) {
            receiver.receive(event, endOfBatch);
        }

    }

    public class Publisher implements InputProcessor {

        private StreamJunction streamJunction;

        public void setStreamJunction(StreamJunction streamJunction) {
            this.streamJunction = streamJunction;
        }

        public void send(ComplexEvent complexEvent) {
            streamJunction.sendEvent(complexEvent);
        }

        @Override
        public void send(Event event, int streamIndex) {
            streamJunction.sendEvent(event);
        }

        @Override
        public void send(Event[] events, int streamIndex) {
            streamJunction.sendEvent(events);
        }

        @Override
        public void send(List<Event> events, int streamIndex) {
            streamJunction.sendEvent(events);
        }

        @Override
        public void send(long timeStamp, Object[] data, int streamIndex) {
            streamJunction.sendData(timeStamp, data);
        }

        public String getStreamId() {
            return streamJunction.getStreamId();
        }
    }

}
