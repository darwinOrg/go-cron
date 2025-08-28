package dgcron

import (
	"fmt"
	"testing"
	"time"

	dgctx "github.com/darwinOrg/go-common/context"
	dglock "github.com/darwinOrg/go-dlock"
	redisdk "github.com/darwinOrg/go-redis"
	"github.com/rolandhe/saber/gocc"
)

func TestAddJob(t *testing.T) {
	cron := NewAndStart(nil)
	defer cron.Stop()
	cron.AddJob("TestJob", "*/1 * * * * ?", func(ctx *dgctx.DgContext) {
		fmt.Println(time.Now().UnixMilli())
	})

	time.Sleep(5 * time.Second)
}

func TestAddJobWithLock(t *testing.T) {
	redisdk.InitClient("localhost:6379")
	cron := NewAndStart(dglock.NewRedisLocker(true))
	defer cron.Stop()
	cron.AddJobWithLock("TestJob", "*/1 * * * * ?", 5000, func(ctx *dgctx.DgContext) {
		fmt.Println(time.Now().UnixMilli())
	})

	time.Sleep(10 * time.Second)
}

func TestAddFixDurationJob(t *testing.T) {
	AddFixDurationJob("TestJob", time.Second, func(ctx *dgctx.DgContext) {
		fmt.Println(time.Now().UnixMilli())
	})

	time.Sleep(5 * time.Second)
}

func TestAddFixDelayJob(t *testing.T) {
	AddFixDelayJob("TestJob", time.Second, func(ctx *dgctx.DgContext) {
		fmt.Println(time.Now().UnixMilli())
	})

	time.Sleep(5 * time.Second)
}

func TestRunSemaphoreJob(t *testing.T) {
	semaphore := gocc.NewDefaultSemaphore(3)

	for i := 0; i < 10; i++ {
		ctx := dgctx.SimpleDgContext()
		_ = RunSemaphoreJob(ctx, "TestJob", semaphore, time.Second, func(ctx *dgctx.DgContext) {
			fmt.Println(time.Now().UnixMilli())
		})
	}

	time.Sleep(10 * time.Second)
}
