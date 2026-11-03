# abacus-event-bus API Index (v1.1.1)
- Build: 6115f183239876262c32c1a276a288ec8a4736eb
- Java: 21
- Generated: 2026-11-03

## Packages
- com.landawn.abacus.eventbus — A lightweight, thread-safe publish-subscribe event system for loosely coupled components.

## com.landawn.abacus.eventbus
### Class EventBus (com.landawn.abacus.eventbus.EventBus)
A publish-subscribe event bus that simplifies communication between components.

**Thread-safety:** thread-safe
**Nullability:** unspecified

#### Public Constructors
- (none)

#### Public Static Methods
##### getDefault(...) -> EventBus
- **Signature:** `@SuppressFBWarnings("MS_EXPOSE_REP") public static EventBus getDefault()`
- **Summary:** Returns the default `EventBus` instance.
- **Contract:**
  - On a standard JVM its worker threads are daemon threads: they never keep the JVM alive, and deliveries still queued when the JVM exits may be abandoned.
  - A shutdown hook, installed when the first default worker is created, gives them up to 120 seconds to drain; no hook is installed if that happens while the JVM is already shutting down or while a security manager denies `RuntimePermission("shutdownHooks")`.
  - When this library is shared by several applications (a container-level library, for example), give each application's bus its own executor through #create(String, Executor) and shut that executor down when the application stops.
  - Conversely, a JVM shutdown hook keeps this library's own class loader reachable until exit once the default executor has created a worker or a caller-supplied executor service has been registered; an application that bundles this library and must be unloadable should deliver only synchronously or, better, use a copy of this library loaded by a shared class loader.
- **Parameters:**
  - (none)
- **Returns:** the default `EventBus` instance
- **Examples:**
  ```java
  EventBus eventBus = EventBus.getDefault();
   eventBus.post("Hello World");
  ```
##### create(...) -> EventBus
- **Signature:** `public static EventBus create()`
- **Summary:** Creates a new `EventBus` instance with a randomly generated identifier.
- **Parameters:**
  - (none)
- **Returns:** a new `EventBus` instance
- **Examples:**
  ```java
  EventBus eventBus = EventBus.create();
  ```
- **Signature:** `public static EventBus create(final String identifier)`
- **Summary:** Creates a new `EventBus` instance with the specified identifier.
- **Parameters:**
  - `identifier` (`String`) — a label for this `EventBus`, returned by #identifier(); it need not be unique and may be `null`
- **Returns:** a new `EventBus` instance with the given identifier
- **Examples:**
  ```java
  EventBus eventBus = EventBus.create("myEventBus");
  ```
- **Signature:** `public static EventBus create(final String identifier, final Executor executor) throws IllegalArgumentException`
- **Summary:** Creates a new `EventBus` instance with the specified identifier and executor.
- **Contract:**
  - The executor is used for asynchronous event delivery when ThreadMode#THREAD_POOL_EXECUTOR is specified.
  - If the supplied executor is an ExecutorService other than the shared default executor or ForkJoinPool#commonPool(), it is registered, by identity, with a single process-wide JVM shutdown hook.
  - That can happen to a decorator over a pool, such as `Executors.newSingleThreadExecutor()`, while the pool behind it is still running queued deliveries; keep a reference to such an executor service, or shut it down yourself, when those deliveries must complete at exit.
- **Parameters:**
  - `identifier` (`String`) — a label for this `EventBus`, returned by #identifier(); it need not be unique and may be `null`
  - `executor` (`Executor`) — the executor to use for asynchronous event delivery; must not be `null`
- **Returns:** a new `EventBus` instance with the given identifier and executor
- **Throws:**
  - `java.lang.IllegalArgumentException` — if `executor` is `null`.
- **Examples:**
  ```java
  ExecutorService executor = Executors.newFixedThreadPool(4);
   EventBus eventBus = EventBus.create("myEventBus", executor);
  ```

#### Public Instance Methods
##### identifier(...) -> String
- **Signature:** `public String identifier()`
- **Summary:** Returns the identifier this `EventBus` was created with.
- **Contract:**
  - Returns `null` if the bus was created with a `null` identifier.
- **Parameters:**
  - (none)
- **Returns:** the identifier of this `EventBus`, possibly `null`
- **Examples:**
  ```java
  EventBus eventBus = EventBus.create("myBus");
   String id = eventBus.identifier();   // returns "myBus"
  ```
##### subscribers(...) -> List<Object>
- **Signature:** `public List<Object> subscribers(final Class<?> eventType) throws IllegalArgumentException`
- **Summary:** Returns the subscribers that have at least one handler with no effective event ID (neither from registration nor from its Subscribe annotation) that would receive an event whose runtime class is `eventType`.
- **Contract:**
  - A handler matches when its parameter type is `eventType` or a supertype of it; a `strictEventType` handler matches only its exact parameter type.
- **Parameters:**
  - `eventType` (`Class<?>`) — the event type to search for
- **Returns:** a snapshot list of subscribers registered for the specified event type
- **Throws:**
  - `java.lang.IllegalArgumentException` — if `eventType` is `null`.
- **Examples:**
  ```java
  List<Object> subscribers = eventBus.subscribers(String.class);
  ```
- **Signature:** `public List<Object> subscribers(final String eventId, final Class<?> eventType) throws IllegalArgumentException`
- **Summary:** Returns the subscribers that have at least one handler whose effective event ID (the registration override, else the Subscribe annotation's) equals `eventId` and that would receive an event whose runtime class is `eventType`.
- **Contract:**
  - Type matching follows #subscribers(Class): a handler matches when its parameter type is `eventType` or a supertype of it (exactly `eventType` for a `strictEventType` handler), a primitive `eventType` is treated as its wrapper class, and a lambda or method reference typed as `Subscriber<E>` itself matches every type.
- **Parameters:**
  - `eventId` (`String`) — the event ID to match, or `null` or empty for subscribers without an event ID
  - `eventType` (`Class<?>`) — the event type to search for
- **Returns:** a snapshot list of subscribers registered for the specified event type and ID
- **Throws:**
  - `java.lang.IllegalArgumentException` — if `eventType` is `null`.
- **Examples:**
  ```java
  List<Object> subscribers = eventBus.subscribers("textEvents", String.class);
  ```
##### allSubscribers(...) -> List<Object>
- **Signature:** `public List<Object> allSubscribers()`
- **Summary:** Returns all currently registered subscribers in this `EventBus`.
- **Parameters:**
  - (none)
- **Returns:** a snapshot list of all currently registered subscribers
- **Examples:**
  ```java
  List<Object> allSubscribers = eventBus.allSubscribers();
  ```
##### countOfSubscribers(...) -> int
- **Signature:** `public int countOfSubscribers()`
- **Summary:** Returns the number of subscribers currently registered with this `EventBus`.
- **Parameters:**
  - (none)
- **Returns:** the count of currently registered subscribers
- **Examples:**
  ```java
  if (eventBus.countOfSubscribers() == 0) {
       // no subscribers registered
   }
  ```
- **See also:** #allSubscribers(), #hasSubscribers(Class)
##### hasSubscribers(...) -> boolean
- **Signature:** `public boolean hasSubscribers(final Class<?> eventType) throws IllegalArgumentException`
- **Summary:** Checks whether any registered subscriber has a handler that accepts events of the given type, regardless of event ID.
- **Contract:**
  - A subscriber matches when one of its handlers' parameter type (that of a @Subscribe method or of its Subscriber#on(Object) implementation) is compatible with `eventType`: for a normal subscriber the parameter type must be assignable from `eventType`; for a `strictEventType` subscriber the two types must be exactly equal.
- **Parameters:**
  - `eventType` (`Class<?>`) — the event class to test for (must not be `null`)
- **Returns:** `true` if at least one registered subscriber accepts `eventType`
- **Throws:**
  - `java.lang.IllegalArgumentException` — if `eventType` is `null`.
- **Examples:**
  ```java
  record OrderPlacedEvent(long orderId) {}
   // Skips building the event when no handler of any event ID accepts it. To test whether a post without an
   // event ID would reach anyone, use !eventBus.subscribers(OrderPlacedEvent.class).isEmpty() instead.
   if (eventBus.hasSubscribers(OrderPlacedEvent.class)) {
       eventBus.post(new OrderPlacedEvent(42L));
   }
  ```
- **See also:** #countOfSubscribers()
##### register(...) -> EventBus
- **Signature:** `public EventBus register(final Object subscriber) throws IllegalArgumentException, RuntimeException, IllegalStateException`
- **Summary:** Registers a subscriber to receive events.
- **Contract:**
  - The subscriber must either implement the Subscriber interface or have methods annotated with Subscribe.
- **Parameters:**
  - `subscriber` (`Object`) — the subscriber to register
- **Returns:** this `EventBus` instance for method chaining
- **Throws:**
  - `java.lang.IllegalArgumentException` — if `subscriber` is `null`, if inherited `@Subscribe` declarations of a handler method conflict, or if no subscriber methods are found in the subscriber class.
  - `java.lang.RuntimeException` — if a `@Subscribe` method of the subscriber's class or a superclass is `static` (whatever its visibility), or if a `public` instance `@Subscribe` method of the class or any supertype does not declare exactly one parameter, or (java.lang.reflect.InaccessibleObjectException) if a handler method cannot be made accessible, for example one in a non-public class whose package is in a named module not opened to this library
  - `java.lang.IllegalStateException` — if the subscriber is identified as a lambda subscriber (an event ID is required for lambda subscribers)
- **Examples:**
  ```java
  eventBus.register(new Subscriber<String>() {
       @Override
       public void on(String event) {
           System.out.println("Received: " + event);
       }
   });
  ```
- **Signature:** `public EventBus register(final Object subscriber, final String eventId) throws IllegalArgumentException, RuntimeException, IllegalStateException`
- **Summary:** Registers a subscriber with an optional event ID override.
- **Contract:**
  - When `eventId` is non-empty, every handler of the subscriber, including handlers whose Subscribe annotation declares a different event ID, receives only events posted with that ID.
  - When `eventId` is `null` or empty, each handler keeps its annotation-level event ID, or no ID if none is declared, as with #register(Object).
- **Parameters:**
  - `subscriber` (`Object`) — the subscriber to register
  - `eventId` (`String`) — the event ID override, or `null` or empty to keep each handler's annotation-level event ID; must be non-empty for lambda-based subscribers
- **Returns:** this `EventBus` instance for method chaining
- **Throws:**
  - `java.lang.IllegalArgumentException` — if `subscriber` is `null`, if inherited `@Subscribe` declarations of a handler method conflict, or if no subscriber methods are found in the subscriber class.
  - `java.lang.RuntimeException` — if a `@Subscribe` method of the subscriber's class or a superclass is `static` (whatever its visibility), or if a `public` instance `@Subscribe` method of the class or any supertype does not declare exactly one parameter, or (java.lang.reflect.InaccessibleObjectException) if a handler method cannot be made accessible, for example one in a non-public class whose package is in a named module not opened to this library
  - `java.lang.IllegalStateException` — if the subscriber is identified as a lambda subscriber and `eventId` is empty or `null`
- **Examples:**
  ```java
  eventBus.register(mySubscriber, "userEvents");
   // This subscriber will only receive events posted with "userEvents" ID
  ```
- **Signature:** `public EventBus register(final Object subscriber, final ThreadMode threadMode) throws IllegalArgumentException, RuntimeException, IllegalStateException`
- **Summary:** Registers a subscriber with a specific thread mode.
- **Parameters:**
  - `subscriber` (`Object`) — the subscriber to register
  - `threadMode` (`ThreadMode`) — the thread mode override for event delivery, or `null` to use the thread mode declared in each subscriber method's Subscribe annotation
- **Returns:** this `EventBus` instance for method chaining
- **Throws:**
  - `java.lang.IllegalArgumentException` — if `subscriber` is `null`, if `threadMode` is not supported, if inherited `@Subscribe` declarations of a handler method conflict, or if no subscriber methods are found in the subscriber class.
  - `java.lang.RuntimeException` — if a `@Subscribe` method of the subscriber's class or a superclass is `static` (whatever its visibility), or if a `public` instance `@Subscribe` method of the class or any supertype does not declare exactly one parameter, or (java.lang.reflect.InaccessibleObjectException) if a handler method cannot be made accessible, for example one in a non-public class whose package is in a named module not opened to this library
  - `java.lang.IllegalStateException` — if the subscriber is identified as a lambda subscriber (an event ID is required for lambda subscribers)
- **Examples:**
  ```java
  eventBus.register(mySubscriber, ThreadMode.THREAD_POOL_EXECUTOR);
   // Events will be delivered on a background thread
  ```
- **Signature:** `public EventBus register(final Object subscriber, final String eventId, final ThreadMode threadMode) throws IllegalArgumentException, RuntimeException, IllegalStateException`
- **Summary:** Registers a subscriber with both event ID and thread mode specifications.
- **Contract:**
  - If the subscriber has sticky event handling enabled, it will immediately receive any matching sticky events; delivery of those sticky events respects each handler's effective ThreadMode (the registration override if given, otherwise the annotation's), so a `THREAD_POOL_EXECUTOR` sticky delivery is handed to this bus's executor: with the default executor it runs on a background thread, but a caller-supplied direct executor (such as `Runnable::run`) runs it on the registering thread before `register` returns.
  - A sticky event removed while this call is replaying is skipped if its turn has not yet come, but one already being replayed is still delivered.
  - Likewise, the replay stops once the subscriber is unregistered or registered again (the new registration replays for itself), even when its own handler does so.
- **Parameters:**
  - `subscriber` (`Object`) — the subscriber to register
  - `eventId` (`String`) — the event ID override; must be non-empty for lambda-based subscribers; `null` or empty preserves each handler's annotation-level event ID (or no ID if none is declared)
  - `threadMode` (`ThreadMode`) — the thread mode override for event delivery, or `null` to use the thread mode declared in each subscriber method's Subscribe annotation
- **Returns:** this `EventBus` instance for method chaining
- **Throws:**
  - `java.lang.IllegalArgumentException` — if `subscriber` is `null`, if `threadMode` is not supported, if inherited `@Subscribe` declarations of a handler method conflict, or if no subscriber methods are found in the subscriber class.
  - `java.lang.RuntimeException` — if a `@Subscribe` method of the subscriber's class or a superclass is `static` (whatever its visibility), or if a `public` instance `@Subscribe` method of the class or any supertype does not declare exactly one parameter, or (java.lang.reflect.InaccessibleObjectException) if a handler method cannot be made accessible, for example one in a non-public class whose package is in a named module not opened to this library
  - `java.lang.IllegalStateException` — if the subscriber is identified as a lambda subscriber and `eventId` is empty or `null`
- **Examples:**
  ```java
  eventBus.register(mySubscriber, "userEvents", ThreadMode.THREAD_POOL_EXECUTOR);
  ```
- **Signature:** `public EventBus register(final Subscriber<?> subscriber, final String eventId) throws IllegalArgumentException, RuntimeException, IllegalStateException`
- **Summary:** Registers a Subscriber interface implementation with a specific event ID.
- **Parameters:**
  - `subscriber` (`Subscriber<?>`) — the `Subscriber` implementation to register
  - `eventId` (`String`) — the event ID to filter events; must be non-empty when the subscriber is identified as a lambda subscriber
- **Returns:** this `EventBus` instance for method chaining
- **Throws:**
  - `java.lang.IllegalArgumentException` — if `subscriber` is `null`, if inherited `@Subscribe` declarations of a handler method conflict, or if no subscriber methods are found.
  - `java.lang.RuntimeException` — if a `@Subscribe` method of the subscriber's class or a superclass is `static` (whatever its visibility), or if a `public` instance `@Subscribe` method of the class or any supertype does not declare exactly one parameter, or (java.lang.reflect.InaccessibleObjectException) if a handler method cannot be made accessible, for example one in a non-public class whose package is in a named module not opened to this library
  - `java.lang.IllegalStateException` — if the subscriber is identified as a lambda subscriber and `eventId` is empty or `null`
- **Examples:**
  ```java
  eventBus.register((Subscriber<String>) event -> {
       System.out.println("Lambda received: " + event);
   }, "stringEvents");
  ```
- **Signature:** `public EventBus register(final Subscriber<?> subscriber, final String eventId, final ThreadMode threadMode) throws IllegalArgumentException, RuntimeException, IllegalStateException`
- **Summary:** Registers a Subscriber interface implementation with both event ID and thread mode.
- **Parameters:**
  - `subscriber` (`Subscriber<?>`) — the `Subscriber` implementation to register
  - `eventId` (`String`) — the event ID to filter events; must be non-empty when the subscriber is identified as a lambda subscriber
  - `threadMode` (`ThreadMode`) — the thread mode override for event delivery, or `null` to use the thread mode declared in each subscriber method's Subscribe annotation
- **Returns:** this `EventBus` instance for method chaining
- **Throws:**
  - `java.lang.IllegalArgumentException` — if `subscriber` is `null`, if `threadMode` is not supported, if inherited `@Subscribe` declarations of a handler method conflict, or if no subscriber methods are found.
  - `java.lang.RuntimeException` — if a `@Subscribe` method of the subscriber's class or a superclass is `static` (whatever its visibility), or if a `public` instance `@Subscribe` method of the class or any supertype does not declare exactly one parameter, or (java.lang.reflect.InaccessibleObjectException) if a handler method cannot be made accessible, for example one in a non-public class whose package is in a named module not opened to this library
  - `java.lang.IllegalStateException` — if the subscriber is identified as a lambda subscriber and `eventId` is empty or `null`
- **Examples:**
  ```java
  eventBus.register((Subscriber<User>) user -> {
       // Process user on background thread
       processUser(user);
   }, "userUpdates", ThreadMode.THREAD_POOL_EXECUTOR);
  ```
##### unregister(...) -> EventBus
- **Signature:** `public EventBus unregister(final Object subscriber)`
- **Summary:** Unregisters a previously registered subscriber.
- **Contract:**
  - This method should be called when a subscriber is no longer needed to prevent memory leaks.
  - If the subscriber was not previously registered, this method does nothing.
- **Parameters:**
  - `subscriber` (`Object`) — the subscriber to unregister
- **Returns:** this `EventBus` instance for method chaining
- **Examples:**
  ```java
  eventBus.unregister(mySubscriber);
   // Future posts will not select mySubscriber; deliveries already queued may still run.
  ```
##### post(...) -> EventBus
- **Signature:** `public EventBus post(final Object event) throws IllegalArgumentException`
- **Summary:** Posts an event to subscribers that have no event ID filter.
- **Parameters:**
  - `event` (`Object`) — the event to post
- **Returns:** this `EventBus` instance for method chaining
- **Throws:**
  - `java.lang.IllegalArgumentException` — if `event` is `null`.
- **Examples:**
  ```java
  eventBus.post("Hello World");
   eventBus.post(new UserLoginEvent(userId));
  ```
- **Signature:** `public EventBus post(final String eventId, final Object event) throws IllegalArgumentException`
- **Summary:** Posts an event with an optional event ID.
- **Contract:**
  - When `eventId` is non-`null` and non-empty, the event is delivered only to handlers whose effective event ID equals it: handlers of subscribers registered with that event ID, and handlers whose Subscribe#eventId() specifies it when their subscriber was registered without an event ID (a registration-time event ID replaces the annotation's).
  - The handler must also accept the event's runtime type (exactly, for a `strictEventType` handler).
  - When `eventId` is `null` or empty, the event is delivered to all subscribers that have no event ID filter (equivalent to calling #post(Object)).
- **Parameters:**
  - `eventId` (`String`) — the event ID for filtering subscribers, or `null` (or empty) to deliver to subscribers without a specific event ID
  - `event` (`Object`) — the event to post
- **Returns:** this `EventBus` instance for method chaining
- **Throws:**
  - `java.lang.IllegalArgumentException` — if `event` is `null`.
- **Examples:**
  ```java
  eventBus.post("userEvents", new UserLoginEvent(userId));
   // Only subscribers associated with "userEvents" will receive this
  ```
##### postSticky(...) -> EventBus
- **Signature:** `public EventBus postSticky(final Object event) throws IllegalArgumentException`
- **Summary:** Posts a sticky event without an event ID.
- **Parameters:**
  - `event` (`Object`) — the sticky event to post
- **Returns:** this `EventBus` instance for method chaining
- **Throws:**
  - `java.lang.IllegalArgumentException` — if `event` is `null`.
- **Examples:**
  ```java
  eventBus.postSticky(new AppConfiguration());
   // Later registered subscribers with sticky=true will receive this event
  ```
- **Signature:** `public EventBus postSticky(final String eventId, final Object event) throws IllegalArgumentException`
- **Summary:** Posts a sticky event with a specific event ID.
- **Contract:**
  - When several retained sticky events match a newly registering sticky subscriber, the order in which they are replayed to it is unspecified.
- **Parameters:**
  - `eventId` (`String`) — the event ID to associate with the sticky event, or `null` (or empty) to retain it without a specific event ID
  - `event` (`Object`) — the sticky event to post
- **Returns:** this `EventBus` instance for method chaining
- **Throws:**
  - `java.lang.IllegalArgumentException` — if `event` is `null`.
- **Examples:**
  ```java
  Configuration config = new Configuration();
   eventBus.postSticky("appConfig", config);
   // Future subscribers for "appConfig" with sticky=true will receive this
  ```
##### removeStickyEvent(...) -> boolean
- **Signature:** `public boolean removeStickyEvent(final Object event)`
- **Summary:** Removes `event` if that exact instance (`==`, not `equals()`) is currently retained without an event ID; subscribers that register after this call returns will no longer receive it (a replay already handing it to a subscriber may still complete).
- **Parameters:**
  - `event` (`Object`) — the sticky event to remove
- **Returns:** `true` if the event was removed, `false` otherwise
- **Examples:**
  ```java
  Configuration config = getConfiguration();
   boolean removed = eventBus.removeStickyEvent(config);
  ```
- **Signature:** `public boolean removeStickyEvent(final String eventId, final Object event)`
- **Summary:** Removes `event` if that exact instance (`==`, not `equals()`) is currently retained under `eventId` (`null` or empty meaning no event ID).
- **Parameters:**
  - `eventId` (`String`) — the event ID the sticky event was posted with, or `null` or empty for no event ID
  - `event` (`Object`) — the sticky event to remove
- **Returns:** `true` if the event was removed, `false` otherwise
- **Examples:**
  ```java
  Configuration config = getConfiguration();
   boolean removed = eventBus.removeStickyEvent("appConfig", config);
  ```
##### removeStickyEvents(...) -> boolean
- **Signature:** `public boolean removeStickyEvents(final Class<?> eventType) throws IllegalArgumentException`
- **Summary:** Removes every sticky event retained without an event ID whose runtime class is `eventType` or a subtype of it, matching #stickyEvents(Class).
- **Parameters:**
  - `eventType` (`Class<?>`) — the type, or a supertype, of the sticky events to remove; a primitive class matches its wrapper type
- **Returns:** `true` if one or more events were removed, `false` otherwise
- **Throws:**
  - `java.lang.IllegalArgumentException` — if `eventType` is `null`.
- **Examples:**
  ```java
  boolean removed = eventBus.removeStickyEvents(UserSession.class);
   // All UserSession (and UserSession subclass) sticky events with no event ID are removed
  ```
- **Signature:** `public boolean removeStickyEvents(final String eventId, final Class<?> eventType) throws IllegalArgumentException`
- **Summary:** Removes every sticky event retained under `eventId` whose runtime class is `eventType` or a subtype of it, matching #stickyEvents(String, Class); `Object.class` removes every sticky event retained under that ID.
- **Parameters:**
  - `eventId` (`String`) — the event ID to match, or `null` or empty for events retained without an event ID
  - `eventType` (`Class<?>`) — the type, or a supertype, of the sticky events to remove; a primitive class matches its wrapper type
- **Returns:** `true` if one or more events were removed, `false` otherwise
- **Throws:**
  - `java.lang.IllegalArgumentException` — if `eventType` is `null`.
- **Examples:**
  ```java
  boolean removed = eventBus.removeStickyEvents("userSessions", UserSession.class);
   // All UserSession (and UserSession subclass) sticky events with the "userSessions" ID are removed
  ```
##### removeAllStickyEvents(...) -> boolean
- **Signature:** `public boolean removeAllStickyEvents()`
- **Summary:** Removes all sticky events from this `EventBus`.
- **Parameters:**
  - (none)
- **Returns:** `true` if one or more sticky events were removed, `false` if there were no sticky events to remove. The return type mirrors #removeStickyEvent(Object) and #removeStickyEvents(Class) for family consistency.
- **Examples:**
  ```java
  boolean removed = eventBus.removeAllStickyEvents();
   // All sticky events are cleared
  ```
##### stickyEvents(...) -> List<T>
- **Signature:** `public <T> List<T> stickyEvents(final Class<T> eventType) throws IllegalArgumentException`
- **Summary:** Returns all sticky events of a specific type that were posted without an event ID.
- **Parameters:**
  - `eventType` (`Class<T>`) — the class type to search for; a primitive class matches its wrapper type
- **Returns:** a list, in unspecified order, of sticky events that can be assigned to the specified type
- **Throws:**
  - `java.lang.IllegalArgumentException` — if `eventType` is `null`.
- **Examples:**
  ```java
  List<Configuration> configs = eventBus.stickyEvents(Configuration.class);
   for (Configuration config : configs) {
       processConfig(config);
   }
  ```
- **Signature:** `public <T> List<T> stickyEvents(final String eventId, final Class<T> eventType) throws IllegalArgumentException`
- **Summary:** Returns all sticky events of a specific type that were posted with the specified event ID.
- **Parameters:**
  - `eventId` (`String`) — the event ID to match, or `null` or empty for events retained without an event ID
  - `eventType` (`Class<T>`) — the class type to search for; a primitive class matches its wrapper type
- **Returns:** a list, in unspecified order, of sticky events matching both the type and event ID
- **Throws:**
  - `java.lang.IllegalArgumentException` — if `eventType` is `null`.
- **Examples:**
  ```java
  List<Configuration> userConfigs = eventBus.stickyEvents("userConfig", Configuration.class);
  ```

### Annotation Subscribe (com.landawn.abacus.eventbus.Subscribe)
Marks a method as an event subscriber in the EventBus system.

**Thread-safety:** unspecified
**Nullability:** unspecified

#### Public Constructors
- (none)

#### Public Static Methods
- (none)

#### Public Instance Methods
##### threadMode(...) -> ThreadMode
- **Signature:** `ThreadMode threadMode() default ThreadMode.DEFAULT`
- **Summary:** Specifies the thread mode for event delivery.
- **Parameters:**
  - (none)
- **Returns:** the thread mode for event delivery
- **Examples:**
  ```java
  @Subscribe(threadMode = ThreadMode.THREAD_POOL_EXECUTOR)
   public void onHeavyEvent(DataEvent event) {
       // Runs on the bus executor (a background thread with the default executor)
       performExpensiveOperation(event.getData());
   }
  ```
##### strictEventType(...) -> boolean
- **Signature:** `boolean strictEventType() default false`
- **Summary:** Controls whether the subscriber accepts only exact event type matches or includes subtypes.
- **Contract:**
  - When set to `true`, only events of the exact parameter type will be delivered.
  - When `false` (default), events of the parameter type and all its subtypes will be delivered.
- **Parameters:**
  - (none)
- **Returns:** `true` to accept only exact type matches, `false` to accept subtypes
- **Examples:**
  ```java
  // With strictEventType = false (default)
   @Subscribe
   public void onAnimal(Animal event) {
       // Receives Animal, Dog, Cat, etc.
   }
  
   // With strictEventType = true
   @Subscribe(strictEventType = true)
   public void onAnimalOnly(Animal event) {
       // Receives only Animal, not Dog or Cat
   }
  ```
##### sticky(...) -> boolean
- **Signature:** `boolean sticky() default false`
- **Summary:** Indicates whether this subscriber should receive previously posted sticky events upon registration.
- **Contract:**
  - When `true`, every retained sticky event matching this subscriber's type and effective event ID is dispatched to this subscriber during registration, in its thread mode.
  - Replayed events pass through the same #intervalMillis() and #deduplicate() filters as posted events, so when either is enabled some matching retained events may be suppressed.
  - When several retained sticky events match, the order in which they are replayed is unspecified.
- **Parameters:**
  - (none)
- **Returns:** `true` to receive sticky events upon registration
- **Examples:**
  ```java
  class ConfigHandler {
       @Subscribe(sticky = true)
       public void onConfig(AppConfig config) {
           // This will be called immediately with the sticky config
           applyConfiguration(config);
       }
   }
  
   // Post a sticky configuration, then register the annotated subscriber.
   eventBus.postSticky(new AppConfig("production"));
   eventBus.register(new ConfigHandler());
  ```
##### eventId(...) -> String
- **Signature:** `String eventId() default ""`
- **Summary:** Specifies an event ID to filter incoming events.
- **Contract:**
  - If empty (default) and no registration-level event ID is supplied, the subscriber receives only events posted without an event ID, matched by type.
- **Parameters:**
  - (none)
- **Returns:** the event ID to filter on, or an empty string to match events posted without an event ID
- **Examples:**
  ```java
  @Subscribe(eventId = "userUpdates")
   public void onUserUpdate(User user) {
       // Only receives User events posted with "userUpdates" ID
   }
  
   @Subscribe(eventId = "adminUpdates")
   public void onAdminUpdate(User user) {
       // Only receives User events posted with "adminUpdates" ID
   }
  ```
##### intervalMillis(...) -> long
- **Signature:** `long intervalMillis() default 0`
- **Summary:** Specifies the minimum time interval (in milliseconds) between accepted delivery attempts.
- **Contract:**
  - Accepted callbacks may overlap if a callback runs longer than the interval; a failed callback still counts as an attempt, while a submission for which `Executor.execute` throws (for example a `RejectedExecutionException` under the default `AbortPolicy`) does not, because it was never delivered - unless its task had already begun the callback by the time the bus handled that failure.
  - Any value `<= 0` is likewise treated as "no throttling": the `EventBus` delivery guard only throttles when the interval is strictly positive, so a negative interval has the same effect as `0`.
- **Parameters:**
  - (none)
- **Returns:** the minimum interval between accepted attempts in milliseconds; any value `<= 0` disables throttling
- **Examples:**
  ```java
  @Subscribe(intervalMillis = 1000)  // keeps at most one event per second
   public void onSensorData(SensorEvent event) {
       updateDisplay(event.getValue());
   }
  
   @Subscribe(intervalMillis = 5000)  // keeps at most one event per 5 seconds
   public void onLocationUpdate(Location location) {
       saveLocationToServer(location);
   }
  ```
##### deduplicate(...) -> boolean
- **Signature:** `boolean deduplicate() default false`
- **Summary:** Controls whether duplicate consecutive events should be ignored.
- **Contract:**
  - When `true`, an event equal to the event of this subscriber's most recently accepted delivery attempt is ignored.
  - If the posted event's `equals()` throws, that event is not delivered to this handler and the previous event is kept, so later events are still compared against it.
- **Parameters:**
  - (none)
- **Returns:** `true` to ignore duplicate consecutive events
- **Examples:**
  ```java
  @Subscribe(deduplicate = true)
   public void onTemperature(Temperature temp) {
       // If temperature hasn't changed, event is ignored
       updateThermostat(temp.getValue());
   }
  
   @Subscribe(deduplicate = true)
   public void onConfigChange(Config config) {
       // Only processes when configuration actually changes
       applyNewConfiguration(config);
   }
  ```

### Interface Subscriber (com.landawn.abacus.eventbus.Subscriber)
A functional interface for implementing event subscribers in the EventBus system.

**Thread-safety:** unspecified
**Nullability:** unspecified

#### Public Constructors
- (none)

#### Public Static Methods
- (none)

#### Public Instance Methods
##### on(...) -> void
- **Signature:** `void on(E event)`
- **Summary:** Called when a matching event is posted to the EventBus; see the class description for how `E` is matched (a lambda typed as `Subscriber<E>` itself receives every event posted with its event ID, whatever its type).
- **Contract:**
  - Implementations should handle the event and should avoid throwing unchecked exceptions.
- **Parameters:**
  - `event` (`E`) — the event instance posted to the `EventBus`; never `null` when the `EventBus` invokes this method, because the bus rejects `null` events (direct callers are not bound by that guarantee)

