/*
 * Copyright (c) 2010-2011. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.eventsourcing;

import org.axonframework.domain.AggregateIdentifier;
import org.axonframework.domain.DomainEventMessage;
import org.axonframework.domain.MetaData;
import org.joda.time.DateTime;

/**
 * Snapshot event that captures the entire aggregate. The motivation is that the aggregate contains all relevant state.
 *
 * @param <T> The type of aggregate this snapshot captures
 * @author Allard Buijze
 * @since 0.6
 */
public class AggregateSnapshot<T extends EventSourcedAggregateRoot> implements Snapshot<T> {

    private final T aggregate;
    private final String eventIdentifier;
    private final MetaData metaData;
    private final DateTime timestamp;

    /**
     * Initialize a new AggregateSnapshot for the given <code>aggregate</code>. Note that the aggregate may not contain
     * uncommitted modifications.
     *
     * @param aggregate The aggregate containing the state to capture in the snapshot
     * @throws IllegalArgumentException if the aggregate contains uncommitted modifications
     */
    public AggregateSnapshot(T aggregate) {
        this(aggregate, MetaData.emptyInstance());
    }

    /**
     * Initialize a new AggregateSnapshot for the given <code>aggregate</code> and <code>metaData</code>. Note that
     * the aggregate may not contain uncommitted modifications.
     *
     * @param aggregate The aggregate containing the state to capture in the snapshot
     * @param metaData  The meta data to assign to this snapshot
     * @throws IllegalArgumentException if the aggregate contains uncommitted modifications
     */
    public AggregateSnapshot(T aggregate, MetaData metaData) {
        if (aggregate.getUncommittedEventCount() != 0) {
            throw new IllegalArgumentException("Aggregate may not have uncommitted modifications");
        }
        this.aggregate = aggregate;
        this.timestamp = new DateTime();
        this.metaData = metaData;
        this.eventIdentifier = getAggregateIdentifier().asString() + "@" + getSequenceNumber();
    }

    /**
     * Copy constructor used to created new a new Snapshot instance based on another one. All data, except MetaData is
     * copied.
     *
     * @param snapshot The snapshot message to copy
     * @param metaData The MetaData for the new snapshot
     */
    protected AggregateSnapshot(AggregateSnapshot<T> snapshot, MetaData metaData) {
        this.aggregate = snapshot.getAggregate();
        this.timestamp = snapshot.getTimestamp();
        this.eventIdentifier = snapshot.getEventIdentifier();
        this.metaData = metaData;
    }

    /**
     * Return the aggregate that was captured in this snapshot.
     *
     * @return the aggregate that was captured in this snapshot.
     */
    public T getAggregate() {
        return aggregate;
    }

    @Override
    public Long getSequenceNumber() {
        return aggregate.getVersion();
    }

    @Override
    public AggregateIdentifier getAggregateIdentifier() {
        return aggregate.getIdentifier();
    }

    @Override
    public String getEventIdentifier() {
        return eventIdentifier;
    }

    @Override
    public DateTime getTimestamp() {
        return timestamp;
    }

    @Override
    public MetaData getMetaData() {
        return metaData;
    }

    @Override
    public T getPayload() {
        return aggregate;
    }

    @Override
    public Class getPayloadType() {
        return aggregate.getClass();
    }

    @Override
    public DomainEventMessage<T> withMetaData(MetaData metaData) {
        return new AggregateSnapshot<T>(this, metaData);
    }
}
