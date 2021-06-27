package com.scb.tools.cache;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CacheImplTest {

    AtomicInteger callCounter = new AtomicInteger();

    private String fakeSlowToUpperCase(String str) {
        int numberOfCalls = callCounter.incrementAndGet();
        System.out.printf("Calling fakeSlowMethod on %s for the %s time%n", str, ordinal(numberOfCalls));

        try {
            TimeUnit.SECONDS.sleep(3);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        return str.toUpperCase();
    }

    private String alwaysReturnTest(String str) {
        // useful for testing null input
        int numberOfCalls = callCounter.incrementAndGet();
        System.out.printf("Calling ifNullReturnsSomethingAnyway on %s for the %s time%n", str, ordinal(numberOfCalls));

        return "test";
    }

    private String produceNull(String str) {
        int numberOfCalls = callCounter.incrementAndGet();
        System.out.printf("Calling produceNull on %s for the %s time%n", str, ordinal(numberOfCalls));

        return null;
    }

    private String throwingMethodExceptIfArgumentIsNoThrow(String str) {
        int numberOfCalls = callCounter.incrementAndGet();
        System.out.printf("Calling throwingMethodExceptIfArgumentIsNoThrow on %s for the %s time%n", str, ordinal(numberOfCalls));

        if (str != null && str.equalsIgnoreCase("no_throw"))
            return str;

        throw new IllegalArgumentException("Error!");
    }

    @Test
    public void testCacheAFewValuesButManyCalls() throws InterruptedException, ExecutionException {
        // create an executor to call the cache many times in parallel
        ExecutorService executor = Executors.newFixedThreadPool(24);

        CacheImpl<String, String> cache = new CacheImpl<>(this::fakeSlowToUpperCase);

        // How many times we will call the cache
        int numberOfTestValues = 10000;
        // the upper bound (excluded) to generate test strings
        int randomNumberUpperBound = 5;
        // the prefix of the generated strings that will be passed to fakeSlowMethod
        String testStringPrefix = "test";
        List<Callable<String>> tasks = new ArrayList<>();

        for (int i = 0; i < numberOfTestValues; i++) {
            // This will produce lots of similar strings, to try our caching mechanism
            // if things go well, we should call the loading function passed to the cache
            // exactly randomNumberUpperBound times
            int randomNum = ThreadLocalRandom.current().nextInt(0, randomNumberUpperBound);
            String testValue = testStringPrefix + randomNum;

            tasks.add(() -> cache.get(testValue));
        }

        List<Future<String>> futures = executor.invokeAll(tasks);

        List<String> results = new ArrayList<>();
        for (Future<String> future : Objects.requireNonNull(futures)) {
            String s = future.get();
            results.add(s);
        }

        executor.shutdown();
        try {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
        }

        List<String> expectedStrings = IntStream.range(0, randomNumberUpperBound)
                .mapToObj(i -> testStringPrefix.toUpperCase() + i)
                .collect(Collectors.toList());

        assertThat(results).containsAll(expectedStrings);
        assertThat(results).hasSize(numberOfTestValues);
        assertThat(callCounter.get()).isEqualTo(randomNumberUpperBound);
    }

    @Test
    public void testCacheWithNullKey_ShouldCacheNullKey() {
        CacheImpl<String, String> cache = new CacheImpl<>(this::alwaysReturnTest);

        // No call yet, should be zero
        assertThat(callCounter.get()).isEqualTo(0);
        // first call, should be null
        assertThat(cache.get(null)).isEqualTo("test");
        assertThat(callCounter.get()).isEqualTo(1);

        // second call, counter should be still one
        assertThat(cache.get(null)).isEqualTo("test");
        assertThat(callCounter.get()).isEqualTo(1);
    }

    @Test
    public void testCacheWithProducingNullValueLoader_ShouldCacheNullValue() {
        CacheImpl<String, String> cache = new CacheImpl<>(this::produceNull);

        // No call yet, should be zero
        assertThat(callCounter.get()).isEqualTo(0);
        // first call, should be null
        assertThat(cache.get("test")).isNull();
        assertThat(callCounter.get()).isEqualTo(1);

        // second call, counter should be still one
        assertThat(cache.get("test")).isNull();
        assertThat(callCounter.get()).isEqualTo(1);
    }

    @Test
    public void testCacheWithNullValueLoader_ShouldThrow() {
        assertThatThrownBy(() -> new CacheImpl<>(null)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testCacheWithThrowingValueLoader_ShouldNotCache() {
        CacheImpl<String, String> cache = new CacheImpl<>(this::throwingMethodExceptIfArgumentIsNoThrow);

        String shouldNotThrow = cache.get("no_throw");
        String shouldNotThrow2 = cache.get("no_throw");

        assertThat(callCounter.get()).isEqualTo(1);
        assertThat(shouldNotThrow).isEqualTo("no_throw");
        assertThat(shouldNotThrow).isEqualTo(shouldNotThrow2);

        assertThatThrownBy(() -> cache.get("test")).isInstanceOf(IllegalArgumentException.class);
        assertThat(callCounter.get()).isEqualTo(2);

        // check that the previously cached values are still there
        assertThat(cache.get("no_throw")).isEqualTo("no_throw");
        // call counter should still be 2
        assertThat(callCounter.get()).isEqualTo(2);

        // Retry to cache the throwing value
        assertThatThrownBy(() -> cache.get("test")).isInstanceOf(IllegalArgumentException.class);
        // it should still call the method (and therefore increment the counter)
        assertThat(callCounter.get()).isEqualTo(3);
    }

    // just a helper to print ordinal numbers nicely
    public static String ordinal(int i) {
        String[] suffixes = new String[]{"th", "st", "nd", "rd", "th", "th", "th", "th", "th", "th"};
        switch (i % 100) {
            case 11:
            case 12:
            case 13:
                return i + "th";
            default:
                return i + suffixes[i % 10];

        }
    }

}