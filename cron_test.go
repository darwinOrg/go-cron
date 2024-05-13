package dgcron

import (
	"fmt"
	dgctx "github.com/darwinOrg/go-common/context"
	dglock "github.com/darwinOrg/go-dlock"
	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/rolandhe/daog"
	"github.com/rolandhe/saber/gocc"
	"log"
	"testing"
	"time"
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
	conf := &daog.DbConf{
		DbUrl:    "root:12345678@tcp(localhost:3306)/whale?charset=utf8mb4&loc=Local&interpolateParams=true&parseTime=true&timeout=1s&readTimeout=2s&writeTimeout=2s",
		Size:     200,
		Life:     50,
		IdleCons: 1200,
		IdleTime: 3600,
		LogSQL:   true,
	}
	db, err := daog.NewDatasource(conf)
	if err != nil {
		log.Fatalln(err)
	}

	cron := NewAndStart(dglock.NewDbLocker(db))
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
		ctx := &dgctx.DgContext{TraceId: uuid.NewString()}
		_ = RunSemaphoreJob(ctx, "TestJob", semaphore, time.Second, func(ctx *dgctx.DgContext) {
			fmt.Println(time.Now().UnixMilli())
		})
	}

	time.Sleep(10 * time.Second)
}
