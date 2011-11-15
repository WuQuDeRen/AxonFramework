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

package org.axonframework.unitofwork;

import org.axonframework.domain.AggregateRoot;
import org.axonframework.domain.EventMessage;
import org.axonframework.eventhandling.EventBus;

/**
 * This class represents a UnitOfWork in which modifications are made to aggregates. A typical UnitOfWork scope is the
 * execution of a command. A UnitOfWork may be used to prevent individual events from being published before a number
 * of
 * aggregates has been processed. It also allows repositories to manage resources, such as locks, over an entire
 * transaction. Locks, for example, will only be released when the UnitOfWork is either committed or rolled back.
 * <p/>
 * The current UnitOfWork can be obtained using {@link CurrentUnitOfWork#get()}.
 *
 * @author Allard Buijze
 * @since 0.6
 */
public interface UnitOfWork {

    /**
     * Commits the UnitOfWork. All registered aggregates that have not been registered as stored are saved in their
     * respective repositories, buffered events are sent to their respective event bus, and all registered
     * UnitOfWorkListeners are notified.
     * <p/>
     * After the commit (successful or not), the UnitOfWork is unregistered from the CurrentUnitOfWork and has cleaned
     * up all resources it occupied. This effectively means that a rollback should be done if Unit Of Work failed to
     * commit.
     *
     * @throws IllegalStateException if the UnitOfWork wasn't started
     */
    void commit();

    /**
     * Clear the UnitOfWork of any buffered changes. All buffered events and registered aggregates are discarded and
     * registered {@link org.axonframework.unitofwork.UnitOfWorkListener}s are notified.
     * <p/>
     * If the rollback is a result of an exception, consider using {@link #rollback(Throwable)} instead.
     */
    void rollback();

    /**
     * Clear the UnitOfWork of any buffered changes. All buffered events and registered aggregates are discarded and
     * registered {@link org.axonframework.unitofwork.UnitOfWorkListener}s are notified.
     *
     * @param cause The cause of the rollback. May be <code>null</code>.
     * @throws IllegalStateException if the UnitOfWork wasn't started
     */
    void rollback(Throwable cause);

    /**
     * Starts the current unit of work, preparing it for aggregate registration. The UnitOfWork instance is registered
     * with the CurrentUnitOfWork.
     */
    void start();

    /**
     * Indicates whether this UnitOfWork is started. It is started when the {@link #start()} method has been called,
     * and
     * if the UnitOfWork has not been committed or rolled back.
     *
     * @return <code>true</code> if this UnitOfWork is started, <code>false</code> otherwise.
     */
    boolean isStarted();

    /**
     * Register a listener that listens to state changes in this UnitOfWork. This typically allows components to clean
     * up resources, such as locks, when a UnitOfWork is committed or rolled back. If a UnitOfWork is partially
     * committed, only the listeners bound to one of the committed aggregates is notified.
     *
     * @param listener The listener to notify when the UnitOfWork's state changes.
     */
    void registerListener(UnitOfWorkListener listener);

    /**
     * Register an aggregate with this UnitOfWork. These aggregates will be saved (at the latest) when the UnitOfWork
     * is
     * committed. This method returns the instance of the aggregate root that should be used as part of the processing
     * in this Unit Of Work.
     * <p/>
     * If an aggregate of the same type and with the same identifier has already been registered, one of two things may
     * happen, depending on the actual implementation: <ul><li>the instance that has already been registered is
     * returned. In which case the given <code>saveAggregateCallback</code> is ignored.</li><li>an
     * IllegalStateException
     * is thrown to indicate an illegal attempt to register a duplicate</li></ul>.
     *
     * @param aggregateRoot         The aggregate root to register in the UnitOfWork
     * @param eventBus              The event bus on which Events generated by this aggregate must be published
     * @param saveAggregateCallback The callback that is invoked when the UnitOfWork wants to store the registered
     *                              aggregate
     * @param <T>                   the type of aggregate to register
     * @return The actual aggregate instance to use
     *
     * @throws IllegalStateException if this Unit Of Work does not support registrations of aggregates with identical
     *                               type and identifier
     */
    <T extends AggregateRoot> T registerAggregate(T aggregateRoot, EventBus eventBus,
                                                  SaveAggregateCallback<T> saveAggregateCallback);

    /**
     * Request to publish the given <code>event</code> on the given <code>eventBus</code>. The UnitOfWork may either
     * publish immediately, or buffer the events until the UnitOfWork is committed.
     *
     * @param event    The event to be published on the event bus
     * @param eventBus The event bus on which to publish the event
     */
    void publishEvent(EventMessage event, EventBus eventBus);
}
