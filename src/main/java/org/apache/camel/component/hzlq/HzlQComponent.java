/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.camel.component.hzlq;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import org.apache.camel.CamelContext;
import org.apache.camel.Endpoint;
import org.apache.camel.impl.DefaultComponent;
import org.apache.camel.util.ObjectHelper;

import com.hazelcast.core.Hazelcast;

/**
 * HzlQ Component :  implementation of a work queue based on
 * <a href="http://www.hazelcast.com">HazelCast</a> in-memory
 * data grid.
 */
public class HzlQComponent extends DefaultComponent {

	private final transient Map<String, BlockingQueue> queues = new HashMap<String, BlockingQueue>();

	public HzlQComponent() {
		super();
	}

	public HzlQComponent(final CamelContext context) {
		super(context);
	}

	@Override
	protected Endpoint createEndpoint(final String uri, final String remaining, final Map<String, Object> parameters) throws Exception {
		final HzlqConfiguration config = new HzlqConfiguration();
		setProperties(config, parameters);

		if(ObjectHelper.isEmpty(remaining)){
			throw new IllegalArgumentException("Queue name is missing.");
		}

		config.setQueueName(remaining);

		return new HzlQEndpoint(uri, this, createQueue(config,parameters), config);
	}

	public synchronized BlockingQueue createQueue(final HzlqConfiguration config, final Map<String, Object> parameters) {
		final String qName= config.getQueueName();

		if (queues.containsKey(qName)){
			return queues.get(qName);
		}

		final BlockingQueue<Serializable> queue = Hazelcast.getQueue(qName);
		queues.put(qName, queue);
		return queue;
	}

}
