/**
 * Copyright 2011 Ioannis Polyzos
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 */
package org.apache.camel.component.hazelcast;

import java.util.concurrent.BlockingQueue;

import org.apache.camel.Consumer;
import org.apache.camel.Endpoint;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.impl.DefaultEndpoint;
import org.apache.camel.util.ObjectHelper;

/**
 * Hazelcast {@link Endpoint} implementation.
 */
public class HazelcastEndpoint extends DefaultEndpoint {

	private final BlockingQueue queue;
	private final HazelcastConfiguration configuration;

	public HazelcastEndpoint(final String uri, final HazelcastComponent component, final BlockingQueue hzlq, final HazelcastConfiguration configuration) {
		super(uri, component);
		ObjectHelper.notNull(hzlq, "queue");
		this.queue = hzlq;
		this.configuration = configuration;
	}

	public Producer createProducer() throws Exception {
		return new HazelcastProducer(this, getQueue());
	}

	public Consumer createConsumer(final Processor processor) throws Exception {
		return new HazelcastConsumer(this, processor);
	}

	public BlockingQueue getQueue() {
		return queue;
	}

	public HazelcastConfiguration getConfiguration() {
		return configuration;
	}

	public boolean isSingleton() {
		return true;
	}

}
