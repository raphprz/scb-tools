# SCB tools

I made this tools in pure Java version 11. The only libraries I used are for testing. Both tools are unit tested.

## Cache

This class provides a basic loading cache, using ConcurrentHashMap. In production, I would use the library
Caffeine (https://github.com/ben-manes/caffeine).

For a given cache <K, V>:

- K and V can be null, since the ConcurrentHashMap uses Optional to store the returned value.
- if the value loader raise an exception, the cache will not be modified, and the user can catch the exception if
  needed. I log the key that created the exception, then rethrow.

To implement the cache, I used a ConcurrentHashMap, but avoided the usage of putIfAbsent/computeIfAbsent methods. To
achieve this, I used 2 types of locks:

- A lock object, called mutex
- A lock map (another ConcurrentHashMap), called locks

We use the mutex object to safely create a lock in the lock table. That way, we will have one lock per key. Concurrent
calls to the cache will not be blocked by others, and concurrent calls for the same key will not have cache stampede
issues.

## Scheduler

A simple scheduler. Implemented using a PriorityBlockingQueue to have the ordering by deadline time, and the thread
safety.

Another possible implementation would be to use a TreeSet, and its method floor to get all deadlines below a threshold.
However it does not seem more efficient, since we still have to iterate over the returned view to trigger all consumers.
A Queue seems more suitable for this kind of job.
