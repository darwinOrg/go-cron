package dgcron

import (
	"context"
	"log"
	"sync"
	"time"

	dgctx "github.com/darwinOrg/go-common/context"
	dgerr "github.com/darwinOrg/go-common/enums/error"
	dglogger "github.com/darwinOrg/go-logger"
	"github.com/panjf2000/ants/v2"
	"github.com/rolandhe/saber/gocc"
)

type RecoverProcessor func(ctx *dgctx.DgContext, name, errorMessage string)

var recoverProcessors []RecoverProcessor

func RegisterRecoverProcessor(processor RecoverProcessor) {
	recoverProcessors = append(recoverProcessors, processor)
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
						processRecover(ctx, name, err)
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
						processRecover(ctx, name, err)
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
						processRecover(ctx, name, err)
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
						processRecover(ctx, name, err)
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
				processRecover(ctx, name, err)
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
				processRecover(ctx, name, err)
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
			defer wg.Done()
			result[i] = iteratee(item, i)
		})
	}

	wg.Wait()
	return result
}

func processRecover(ctx *dgctx.DgContext, name string, err any) {
	if len(recoverProcessors) == 0 {
		return
	}

	var errorMessage string
	switch err.(type) {
	case string:
		errorMessage = err.(string)
	case error:
		errorMessage = err.(error).Error()
	default:
		errorMessage = dgerr.SYSTEM_ERROR.Message
	}

	for _, processor := range recoverProcessors {
		processor(ctx, name, errorMessage)
	}
}
