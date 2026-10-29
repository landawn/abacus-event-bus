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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.landawn.abacus.util.ThreadMode;

/**
 * Marks a method as an event subscriber in the {@link EventBus} system.
 * Methods annotated with {@code @Subscribe} will receive events when their containing object
 * is registered with an {@link EventBus}.
 *
 * <p>The annotated method must meet the following requirements:</p>
 * <ul>
 *   <li>Must be {@code public} — a non-{@code public} annotated method is silently ignored rather
 *       than rejected (unless it is also {@code static}, see below).</li>
 *   <li>Must have exactly one parameter representing the event type; otherwise registering the
 *       declaring object throws a {@link RuntimeException}.</li>
 *   <li>Must not be {@code static}; otherwise registering the declaring object throws a
 *       {@link RuntimeException}. This check runs before the visibility check, so a non-{@code public}
 *       {@code static} annotated method is rejected rather than ignored.</li>
 *   <li>Should not throw checked exceptions (any thrown exceptions are caught and logged).</li>
 * </ul>
 *
 * <p><b>Annotation-only advanced features:</b> The {@link #sticky()}, {@link #strictEventType()},
 * {@link #intervalMillis()} (throttling) and {@link #deduplicate()} knobs can be configured
 * <em>only</em> through this annotation. The {@code EventBus.register(...)} methods accept
 * registration-time overrides for the event ID and thread mode only, so a {@link Subscriber}
 * registration whose {@code on(...)} method carries no {@code @Subscribe} annotation (for example a
 * plain lambda or method reference) cannot use any of these four features. Annotate a subscriber method
 * with {@code @Subscribe} to opt into them; a {@code Subscriber} implementation may annotate its own
 * {@code on(...)} method.</p>
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * public class EventHandler {
 *     @Subscribe
 *     public void onStringEvent(String event) {
 *         System.out.println("Received: " + event);
 *     }
 *
 *     @Subscribe(
 *         threadMode = ThreadMode.THREAD_POOL_EXECUTOR,
 *         eventId = "criticalEvents",
 *         sticky = true,
 *         strictEventType = true,
 *         intervalMillis = 1000,
 *         deduplicate = true
 *     )
 *     public void onCriticalEvent(CriticalEvent event) {
 *         // Receives only CriticalEvent instances (not subtypes) posted with the "criticalEvents" ID,
 *         // on the bus executor (a background thread by default), replays matching retained sticky
 *         // events on registration, accepts at most one event per second, and skips an event equal to
 *         // the previously accepted one (both filters also apply to sticky replays).
 *     }
 * }
 * }</pre>
 *
 * @see EventBus
 * @see Subscriber
 * @see ThreadMode
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Subscribe {

    /**
     * Specifies the thread mode for event delivery.
     * This determines on which thread the subscriber method will be called. A non-{@code null} thread mode
     * passed to {@code EventBus.register(...)} replaces this value for every handler of that registration.
     *
     * <p>Available options:</p>
     * <ul>
     *   <li>{@link ThreadMode#DEFAULT} - Events are delivered synchronously on the thread that posts them; a
     *       sticky replay runs on the thread that calls {@code register} (default)</li>
     *   <li>{@link ThreadMode#THREAD_POOL_EXECUTOR} - Events are handed to the bus's executor: a pool of
     *       background threads for the default executor, or whatever thread a custom executor supplied to
     *       {@link EventBus#create(String, java.util.concurrent.Executor)} chooses (a direct executor runs the
     *       callback on the posting thread)</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * @Subscribe(threadMode = ThreadMode.THREAD_POOL_EXECUTOR)
     * public void onHeavyEvent(DataEvent event) {
     *     // Runs on the bus executor (a background thread with the default executor)
     *     performExpensiveOperation(event.getData());
     * }
     * }</pre>
     *
     * @return the thread mode for event delivery
     */
    ThreadMode threadMode() default ThreadMode.DEFAULT;

    /**
     * Controls whether the subscriber accepts only exact event type matches or includes subtypes.
     *
     * <p>When set to {@code true}, only events of the exact parameter type will be delivered.
     * When {@code false} (default), events of the parameter type and all its subtypes will be delivered.
     * The strict comparison uses the event's runtime class, so a strict subscriber whose parameter type is an
     * interface or an abstract class never receives events.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // With strictEventType = false (default)
     * @Subscribe
     * public void onAnimal(Animal event) {
     *     // Receives Animal, Dog, Cat, etc.
     * }
     *
     * // With strictEventType = true
     * @Subscribe(strictEventType = true)
     * public void onAnimalOnly(Animal event) {
     *     // Receives only Animal, not Dog or Cat
     * }
     * }</pre>
     *
     * @return {@code true} to accept only exact type matches, {@code false} to accept subtypes
     */
    boolean strictEventType() default false;

    /**
     * Indicates whether this subscriber should receive previously posted sticky events upon registration.
     *
     * <p>When {@code true}, every retained sticky event matching this subscriber's type and effective event ID
     * is dispatched to this subscriber during registration, in its thread mode. A {@code THREAD_POOL_EXECUTOR}
     * handler's replay is handed to the bus's executor: with the default executor it runs on a background
     * thread, but a caller-supplied direct executor runs it on the registering thread before
     * {@code register} returns. Replayed events pass through the
     * same {@link #intervalMillis()} and {@link #deduplicate()} filters as posted events, so when either is
     * enabled some matching retained events may be suppressed. When several retained sticky events
     * match, the order in which they are replayed is unspecified.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * class ConfigHandler {
     *     @Subscribe(sticky = true)
     *     public void onConfig(AppConfig config) {
     *         // This will be called immediately with the sticky config
     *         applyConfiguration(config);
     *     }
     * }
     *
     * // Post a sticky configuration, then register the annotated subscriber.
     * eventBus.postSticky(new AppConfig("production"));
     * eventBus.register(new ConfigHandler());
     * }</pre>
     *
     * @return {@code true} to receive sticky events upon registration
     */
    boolean sticky() default false;

    /**
     * Specifies an event ID to filter incoming events.
     * Only events posted with the matching event ID will be delivered to this subscriber. A non-empty event ID
     * passed to {@code EventBus.register(...)} replaces this value for every handler of that registration: the
     * handler then receives only events posted with the registration's ID, not those posted with this one.
     *
     * <p>If empty (default) and no registration-level event ID is supplied, the subscriber receives
     * only events posted without an event ID, matched by type.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * @Subscribe(eventId = "userUpdates")
     * public void onUserUpdate(User user) {
     *     // Only receives User events posted with "userUpdates" ID
     * }
     *
     * @Subscribe(eventId = "adminUpdates")
     * public void onAdminUpdate(User user) {
     *     // Only receives User events posted with "adminUpdates" ID
     * }
     * }</pre>
     *
     * @return the event ID to filter on, or an empty string to match events posted without an event ID
     */
    String eventId() default "";

    /**
     * Specifies the minimum time interval (in milliseconds) between accepted delivery attempts.
     * Events posted within this interval will be ignored. The decision is made on the posting thread at
     * the moment the event is posted, for every {@link #threadMode()}, so it follows post spacing rather
     * than executor scheduling; a suppressed asynchronous event is never handed to the executor.
     * Accepted callbacks may overlap if a callback runs longer than the interval; a failed callback still
     * counts as an attempt, while a submission the executor rejects does not, because it was never delivered.
     *
     * <p>This is useful for throttling high-frequency events to prevent overwhelming the subscriber.</p>
     *
     * <p>A value of {@code 0} (the default) disables throttling. Any value <b>{@code <= 0}</b> is
     * likewise treated as "no throttling": the {@code EventBus} delivery guard only throttles when
     * the interval is strictly positive, so a negative interval has the same effect as {@code 0}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * @Subscribe(intervalMillis = 1000)  // keeps at most one event per second
     * public void onSensorData(SensorEvent event) {
     *     updateDisplay(event.getValue());
     * }
     *
     * @Subscribe(intervalMillis = 5000)  // keeps at most one event per 5 seconds
     * public void onLocationUpdate(Location location) {
     *     saveLocationToServer(location);
     * }
     * }</pre>
     *
     * @return the minimum interval between accepted attempts in milliseconds; any value {@code <= 0} disables throttling
     */
    long intervalMillis() default 0;

    /**
     * Controls whether duplicate consecutive events should be ignored.
     *
     * <p>When {@code true}, an event equal to the event of this subscriber's most recently accepted delivery
     * attempt is ignored. Events are compared using their {@code equals()} method. The comparison is made on
     * the posting thread at the moment the event is posted, for every {@link #threadMode()}, so "previous"
     * follows posting order, not the order in which an executor happened to run callbacks; a suppressed
     * asynchronous duplicate is never handed to the executor. An event suppressed by
     * {@link #intervalMillis()} is not accepted, so it never becomes the previous event.</p>
     *
     * <p>The comparison uses the most recently accepted delivery attempt, including one whose callback
     * throws. An asynchronous submission the executor rejects is not one of them: it was never delivered, so
     * it does not become the "previous event". Filtering is atomic, but distinct accepted callbacks may
     * execute concurrently.</p>
     *
     * <p>This is useful for preventing redundant processing of unchanged data.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * @Subscribe(deduplicate = true)
     * public void onTemperature(Temperature temp) {
     *     // If temperature hasn't changed, event is ignored
     *     updateThermostat(temp.getValue());
     * }
     *
     * @Subscribe(deduplicate = true)
     * public void onConfigChange(Config config) {
     *     // Only processes when configuration actually changes
     *     applyNewConfiguration(config);
     * }
     * }</pre>
     *
     * @return {@code true} to ignore duplicate consecutive events
     */
    boolean deduplicate() default false;
}
