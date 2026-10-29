/*
 * Copyright (C) 2016 HaiYang Li
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.landawn.abacus.eventbus;

/**
 * A functional interface for implementing event subscribers in the EventBus system.
 * Classes implementing this interface can be registered with an {@link EventBus} to receive events
 * of the specified type.
 *
 * <p>This interface is particularly useful for creating subscribers using lambda expressions
 * or anonymous inner classes. The bus matches events against the type argument {@code E} only when the
 * implementing class reifies it, as an anonymous {@code new Subscriber<Integer>() { ... }}, a named
 * {@code class X implements Subscriber<Integer>}, or a lambda whose target is a sub-interface that fixes it
 * ({@code interface IntSubscriber extends Subscriber<Integer>}) does. A lambda expression or method reference
 * whose target type is {@code Subscriber<E>} itself does not retain {@code E} at runtime, so the bus treats it
 * as accepting any {@code Object}, like an unannotated
 * {@code Subscriber<Object>} or unbound generic implementation: it must be registered with a non-empty event
 * ID (otherwise registration throws {@link IllegalStateException}), and it then receives every event posted
 * with that ID, whatever its type. Delivering an event that is not an {@code E} to such a subscriber fails;
 * the {@code EventBus} catches and logs that failure, so post a single payload type per event ID.</p>
 *
 * <p><b>Usage Examples with lambda expression:</b></p>
 * <pre>{@code
 * EventBus eventBus = EventBus.getDefault();
 *
 * // Lambda subscriber: receives every event posted with "stringEvents"
 * eventBus.register((Subscriber<String>) event -> {
 *     System.out.println("Received: " + event);
 * }, "stringEvents");
 *
 * // Lambda subscriber: receives every event posted with "userEvents"
 * eventBus.register((Subscriber<UserLoginEvent>) event -> {
 *     System.out.println("User logged in: " + event.getUserId());
 * }, "userEvents");
 * }</pre>
 *
 * <p><b>Usage Examples with anonymous inner class:</b></p>
 * <pre>{@code
 * Subscriber<Integer> numberSubscriber = new Subscriber<Integer>() {
 *     @Override
 *     public void on(Integer event) {
 *         System.out.println("Number received: " + event);
 *     }
 * };
 * eventBus.register(numberSubscriber, "numberEvents");
 * }</pre>
 *
 * <p><b>Usage Examples with method reference:</b></p>
 * <pre>{@code
 * class MyHandler {
 *     public void handleString(String message) {
 *         System.out.println("Handling: " + message);
 *     }
 * }
 *
 * MyHandler handler = new MyHandler();
 * eventBus.register((Subscriber<String>) handler::handleString, "messages");
 * }</pre>
 *
 * @param <E> the type of event this subscriber will receive
 * @see EventBus
 * @see Subscribe
 */
@FunctionalInterface
public interface Subscriber<E> {

    /**
     * Called when an event of type {@code E} is posted to the {@link EventBus}.
     * This method is invoked by the {@code EventBus} when a matching event is posted.
     *
     * <p>The method will be called on the thread determined by the
     * {@link com.landawn.abacus.util.ThreadMode} supplied during registration. Without a registration
     * override, an implementation method's {@link Subscribe} annotation supplies the mode;
     * otherwise {@link com.landawn.abacus.util.ThreadMode#DEFAULT} applies.</p>
     *
     * <p>Implementations should handle the event and should avoid throwing unchecked exceptions.
     * Any exceptions thrown will be caught and logged by the {@code EventBus}.</p>
     *
     * @param event the event instance posted to the {@code EventBus}; never {@code null} when the
     *              {@code EventBus} invokes this method, because the bus rejects {@code null} events
     *              (direct callers are not bound by that guarantee)
     */
    void on(E event);
}
