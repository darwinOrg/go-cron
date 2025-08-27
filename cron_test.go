package dgcron

import (
	"fmt"
	"testing"
	"time"

	dgctx "github.com/darwinOrg/go-common/context"
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
