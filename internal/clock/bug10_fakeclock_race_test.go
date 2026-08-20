package clock

import (
	"sync"
	"testing"
	"time"
)

func TestFakeClockConcurrentNowAndAdvanceIsRaceFree(t *testing.T) {
	c := NewFakeClock(time.Unix(900, 0))
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 2000; j++ {
				_ = c.Now()
			}
		}()
	}
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				c.Advance(time.Millisecond)
			}
		}()
	}
	wg.Wait()
}
