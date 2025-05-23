package dgcron

import (
	"bytes"
	"context"
	dgctx "github.com/darwinOrg/go-common/context"
	dglock "github.com/darwinOrg/go-dlock"
	dglogger "github.com/darwinOrg/go-logger"
	"github.com/panjf2000/ants"
	"github.com/robfig/cron/v3"
	"github.com/rolandhe/saber/gocc"
	"log"
	"sync"
	"time"
)

type DgCron struct {
	cron   *cron.Cron
	locker dglock.Locker
}

type DgJob func(ctx *dgctx.DgContext)
type DgJobWithCancel func(ctx *dgctx.DgContext, cancel context.CancelFunc)

func NewAndStart(locker dglock.Locker) *DgCron {
	dc := &DgCron{cron: newWithSeconds(), locker: locker}
	dc.cron.Start()

	return dc
}

func (dc *DgCron) Stop() {
	dc.cron.Stop()
}

func (dc *DgCron) AddJob(name string, spec string, job DgJob) {
	log.Printf("add spec job, name: %s, spec: %s", name, spec)
	_, _ = dc.cron.AddFunc(spec, func() {
		ctx := dgctx.SimpleDgContext()
		defer func() {
			if err := recover(); err != nil {
				dglogger.Errorf(ctx, "job panic, name: %s, err: %v", name, err)
			}
		}()
		job(ctx)
	})
}

func (dc *DgCron) AddJobWithTimeout(name string, spec string, timeout time.Duration, job DgJobWithCancel) {
	log.Printf("add spec job with timeout, name: %s, spec: %s, timeout: %d", name, spec, timeout)
	_, _ = dc.cron.AddFunc(spec, func() {
		ctx, cancel := dgctx.WithTimeout(context.Background(), timeout)
		defer cancel()
		defer func() {
			if err := recover(); err != nil {
				dglogger.Errorf(ctx, "job panic, name: %s, err: %v", name, err)
			}
		}()
		job(ctx, cancel)
	})
}

func (dc *DgCron) AddJobWithLock(name string, spec string, lockMilli int64, job DgJob) {
	if dc.locker == nil {
		dc.AddJob(name, spec, job)
		return
	}

	log.Printf("add spec job, name: %s, spec: %s, lockMilli: %d", name, spec, lockMilli)
	_, _ = dc.cron.AddFunc(spec, func() {
		ctx := dgctx.SimpleDgContext()
		if dc.locker.DoLock(ctx, name, lockMilli) {
			defer dc.locker.Unlock(ctx, name)
			defer func() {
				if err := recover(); err != nil {
					dglogger.Errorf(ctx, "job panic, name: %s, err: %v", name, err)
				}
			}()
			job(ctx)
		}
	})
}

func (dc *DgCron) AddJobWithLockAndTimeout(name string, spec string, lockMilli int64, timeout time.Duration, job DgJobWithCancel) {
	if dc.locker == nil {
		dc.AddJobWithTimeout(name, spec, timeout, job)
		return
	}

	log.Printf("add spec job, name: %s, spec: %s, lockMilli: %d, timeout: %d", name, spec, lockMilli, timeout)
	_, _ = dc.cron.AddFunc(spec, func() {
		ctx, cancel := dgctx.WithTimeout(context.Background(), timeout)
		defer cancel()
		if dc.locker.DoLock(ctx, name, lockMilli) {
			defer dc.locker.Unlock(ctx, name)
			defer func() {
				if err := recover(); err != nil {
					dglogger.Errorf(ctx, "job panic, name: %s, err: %v", name, err)
				}
			}()
			job(ctx, cancel)
		}
	})
}

func AddFixDurationJob(name string, duration time.Duration, job DgJob) {
	log.Printf("add fix duration job, name: %s, duration: %s", name, duration)

	go func() {
		for {
			go func() {
				ctx := dgctx.SimpleDgContext()
				defer func() {
					if err := recover(); err != nil {
						dglogger.Errorf(ctx, "job panic, name: %s, err: %v", name, err)
					}
				}()
				job(ctx)
			}()

			time.Sleep(duration)
		}
	}()
}

func AddFixDurationJobWithTimeout(name string, duration time.Duration, timeout time.Duration, job DgJobWithCancel) {
	log.Printf("add fix duration job, name: %s, duration: %s, timeout: %d", name, duration, timeout)

	go func() {
		for {
			go func() {
				ctx, cancel := dgctx.WithTimeout(context.Background(), timeout)
				defer cancel()
				defer func() {
					if err := recover(); err != nil {
						dglogger.Errorf(ctx, "job with timeout panic, name: %s, err: %v", name, err)
					}
				}()
				job(ctx, cancel)
			}()

			time.Sleep(duration)
		}
	}()
}

func AddFixDelayJob(name string, delay time.Duration, job DgJob) {
	log.Printf("add fix delay job, name: %s, delay: %s", name, delay)

	go func() {
		for {
			func() {
				ctx := dgctx.SimpleDgContext()
				defer func() {
					if err := recover(); err != nil {
						dglogger.Errorf(ctx, "job panic, name: %s, err: %v", name, err)
					}
				}()
				job(ctx)
			}()

			time.Sleep(delay)
		}
	}()
}

func AddFixDelayJobWithTimeout(name string, delay time.Duration, timeout time.Duration, job DgJobWithCancel) {
	log.Printf("add fix delay job, name: %s, delay: %s, timeout: %d", name, delay, timeout)

	go func() {
		for {
			func() {
				ctx, cancel := dgctx.WithTimeout(context.Background(), timeout)
				defer cancel()
				defer func() {
					if err := recover(); err != nil {
						dglogger.Errorf(ctx, "job with timeout panic, name: %s, err: %v", name, err)
					}
				}()
				job(ctx, cancel)
			}()

			time.Sleep(delay)
		}
	}()
}

func RunSemaphoreJob(ctx *dgctx.DgContext, name string, semaphore gocc.Semaphore, acquireTimeout time.Duration, job DgJob) bool {
	if !semaphore.AcquireTimeout(acquireTimeout) {
		return false
	}

	go func() {
		defer semaphore.Release()
		dglogger.Infof(ctx, "run semaphore job, name: %s", name)
		defer func() {
			if err := recover(); err != nil {
				dglogger.Errorf(ctx, "job panic, name: %s, err: %v", name, err)
			}
		}()
		job(ctx)
	}()

	return true
}

func RunSemaphoreJobWithTimeout(ctx *dgctx.DgContext, name string, semaphore gocc.Semaphore, acquireTimeout time.Duration, jobTimeout time.Duration, job DgJobWithCancel) bool {
	if !semaphore.AcquireTimeout(acquireTimeout) {
		return false
	}

	go func() {
		defer semaphore.Release()
		dglogger.Infof(ctx, "run semaphore job, name: %s", name)
		cancel := ctx.WithTimeout(context.Background(), jobTimeout)
		defer cancel()
		defer func() {
			if err := recover(); err != nil {
				dglogger.Errorf(ctx, "job with timeout panic, name: %s, err: %v", name, err)
			}
		}()
		job(ctx, cancel)
	}()

	return true
}

func RunParallelMap[T any, R any](slice []T, poolSize int, iteratee func(item T, index int) R) []R {
	p, _ := ants.NewPool(poolSize)
	defer p.Release()
	result := make([]R, len(slice))

	var wg sync.WaitGroup
	wg.Add(len(slice))

	for i, item := range slice {
		_ = p.Submit(func() {
			res := iteratee(item, i)
			result[i] = res
			wg.Done()
		})
	}

	wg.Wait()
	return result
}

// newWithSeconds returns a Cron with the seconds field enabled.
func newWithSeconds() *cron.Cron {
	var buf syncWriter
	logger := newBufLogger(&buf)
	var secondParser = cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.DowOptional | cron.Descriptor)
	return cron.New(cron.WithParser(&secondParser), cron.WithChain(cron.Recover(logger), cron.DelayIfStillRunning(logger)))
}

func newBufLogger(sw *syncWriter) cron.Logger {
	return cron.PrintfLogger(log.New(sw, "", log.LstdFlags))
}

type syncWriter struct {
	wr bytes.Buffer
	m  sync.Mutex
}

func (sw *syncWriter) Write(data []byte) (n int, err error) {
	sw.m.Lock()
	n, err = sw.wr.Write(data)
	sw.m.Unlock()
	return
}

func (sw *syncWriter) String() string {
	sw.m.Lock()
	defer sw.m.Unlock()
	return sw.wr.String()
}
