"""
Tests for the token bucket rate limiter.

These tests verify that the rate limiter correctly throttles API calls
to respect the configured rate limit.
"""
import time
import threading
import unittest
from project_eden.utils.rate_limiter import TokenBucketRateLimiter, reset_rate_limiter


class TestRateLimiter(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method."""
        reset_rate_limiter()
    
    def test_basic_token_acquisition(self):
        """Test that tokens are acquired correctly once the bucket has filled."""
        # Bucket starts empty, so we wait for enough tokens to accumulate first.
        # At 60/min = 1 token/sec, 5 tokens take ~5 seconds to arrive.
        limiter = TokenBucketRateLimiter(rate_limit_per_min=60)
    
        start = time.time()
        limiter.acquire(5)
        elapsed = time.time() - start
    
        # Should have waited approximately 5 seconds (5 tokens at 1 token/sec)
        self.assertGreaterEqual(elapsed, 4.5)
        self.assertLess(elapsed, 7.0)
    
    def test_rate_limiting(self):
        """Test that rate limiting actually throttles requests."""
        # Use a high rate (600/min = 10 tokens/sec) so filling the bucket is fast.
        limiter = TokenBucketRateLimiter(rate_limit_per_min=600)
    
        # At 10 tokens/sec, fill 20 tokens in 2 s, then drain 20, then wait for 10 more.
        time.sleep(2)                    # fill ~20 tokens
        limiter.acquire(20)              # drain the bucket (near-instant, tokens waiting)
    
        # Next acquisition should wait ~1 second (10 tokens at 10 tokens/sec)
        start = time.time()
        limiter.acquire(10)
        elapsed = time.time() - start
    
        # Should have waited approximately 1 second
        self.assertGreaterEqual(elapsed, 0.8)
        self.assertLess(elapsed, 2.5)
    
    def test_token_refill(self):
        """Test that tokens refill over time."""
        # Use a high rate (600/min = 10 tokens/sec) so the test completes quickly.
        # Bucket starts empty; acquire(5) blocks ~0.5 s, leaving ~5 tokens remaining.
        limiter = TokenBucketRateLimiter(rate_limit_per_min=600)
    
        # Drain below halfway: wait for 5 tokens then consume them.
        limiter.acquire(5)
    
        # Wait 2 seconds for more tokens to accumulate (10 tokens/sec × 2s = ~20 tokens).
        time.sleep(2)
    
        available = limiter.get_available_tokens()
        expected = 20  # ~20 tokens refilled during the 2-second sleep
    
        self.assertGreaterEqual(available, expected - 2)
        self.assertLessEqual(available, expected + 2)
    
    def test_parallel_workers(self):
        """Test that multiple workers can share the rate limiter."""
        # 120 calls/min = 2 tokens/sec. Bucket starts empty.
        # 4 workers × 30 tokens = 120 tokens total → ~60 seconds to fill from empty.
        limiter = TokenBucketRateLimiter(rate_limit_per_min=120)
    
        results = []
        lock = threading.Lock()
    
        def worker(worker_id, num_tokens):
            """Simulate a worker acquiring tokens."""
            start = time.time()
            limiter.acquire(num_tokens)
            elapsed = time.time() - start
            with lock:
                results.append((worker_id, elapsed))
    
        # Start 4 workers, each requesting 30 tokens
        threads = []
        for i in range(4):
            t = threading.Thread(target=worker, args=(i, 30))
            threads.append(t)
            t.start()
    
        # Wait for all workers to complete
        for t in threads:
            t.join()
    
        # All workers should have completed
        self.assertEqual(len(results), 4)
    
        # The last worker finishes after ~60 s (120 tokens at 2 tokens/sec from empty).
        # Allow a generous upper bound for scheduling jitter.
        total_time = max(elapsed for _, elapsed in results)
        self.assertGreaterEqual(total_time, 55)
        self.assertLess(total_time, 75)
    
    def test_max_tokens_cap(self):
        """Test that tokens don't exceed the maximum."""
        limiter = TokenBucketRateLimiter(rate_limit_per_min=60)
        
        # Wait for tokens to refill beyond max
        time.sleep(3)
        
        # Should be capped at max_tokens
        available = limiter.get_available_tokens()
        self.assertLessEqual(available, 60)


if __name__ == "__main__":
    unittest.main()

