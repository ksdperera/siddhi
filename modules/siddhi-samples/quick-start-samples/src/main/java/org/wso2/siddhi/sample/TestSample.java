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

package org.wso2.siddhi.sample;

import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;

public class TestSample {

    public static void main(String[] args) throws InterruptedException {

        // Creating Siddhi Manager
        SiddhiManager siddhiManager = new SiddhiManager();


        String executionPlan = "define stream inStream (UID string, externalTimeID long, globalOrder int);\n" +
                "define stream outStream (UID string, externalTimeID long, globalOrder int);\n" +
                "from every ( e1=inStream ) -> e2=inStream[externalTimeID - e1.externalTimeID < 10]\n" +
                "select e2.UID as UID, e1.externalTimeID - 10 as externalTimeID, e2.globalOrder as globalOrder\n" +
                "insert into inStreamCopy1;\n" +
                "from inStream\n" +
                "select UID, externalTimeID, globalOrder\n" +
                "insert into inStreamCopy1;\n" +
                "from inStreamCopy1\n" +
                "select UID, max(externalTimeID) as externalTimeID, globalOrder\n" +
                "group by globalOrder\n" +
                "insert into intStream;\n" +
                "from every e1=intStream, e2=intStream[globalOrder != e1.globalOrder]\n" +
                "select e1.UID as UID, e1.externalTimeID as externalTimeID, e1.globalOrder as globalOrder\n" +
                "insert into intStream2;\n" +
                "from intStream2#window.sort(10, externalTimeID, 'desc', globalOrder, 'desc')\n" +
                "select UID, externalTimeID, globalOrder\n" +
                "insert expired events into outStream;";

        //Generating runtime
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);

        //Adding callback to retrieve output events from stream
        executionPlanRuntime.addCallback("outStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
            }
        });

        //Retrieving InputHandler to push events into Siddhi
        InputHandler sources1 = executionPlanRuntime.getInputHandler("inStream");
        InputHandler sources2 = executionPlanRuntime.getInputHandler("inStream");
        InputHandler sources3 = executionPlanRuntime.getInputHandler("inStream");

        //Starting event processing
        executionPlanRuntime.start();

        //Sending events to Siddhi
        sources1.send(new Object[]{"1A",1001L, 1});
        sources1.send(new Object[]{"1B", 1010L, 2});
        sources2.send(new Object[]{"2A",1005L, 3});
        sources3.send(new Object[]{ "3A",1001L, 4});
        sources1.send(new Object[]{ "1C",1010L, 5});
        sources2.send(new Object[]{ "2B",1011L, 6});
        sources3.send(new Object[]{ "3B",1002L, 7});
        sources3.send(new Object[]{ "3C",1010L, 8});
        sources2.send(new Object[]{ "2C",1020L, 9});
        sources1.send(new Object[]{ "1D",1025L, 10});
        sources2.send(new Object[]{ "2D",1025L, 11});
        sources3.send(new Object[]{ "3D",1025L, 12});
        sources1.send(new Object[]{ "1E",1029L, 13});
        sources2.send(new Object[]{ "2E",1027L, 14});
        sources1.send(new Object[]{ "1F",1040L, 15});
        sources2.send(new Object[]{ "2F",1028L, 16});
        sources3.send(new Object[]{ "3E",1040L, 17});
        Thread.sleep(1000);

        //Shutting down the runtime
        executionPlanRuntime.shutdown();

        //Shutting down Siddhi
        siddhiManager.shutdown();
    }
}
