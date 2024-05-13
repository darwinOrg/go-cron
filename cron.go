package dgcron

import (
	"bytes"
	dgctx "github.com/darwinOrg/go-common/context"
	dglock "github.com/darwinOrg/go-dlock"
	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
	"log"
	"sync"
	"time"
)

type DgCron struct {
	cron   *cron.Cron
	locker dglock.Locker
}

type DgJob func(ctx *dgctx.DgContext)

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
		job(&dgctx.DgContext{TraceId: uuid.NewString(), UserId: 0})
	})
}

func (dc *DgCron) AddJobWithLock(name string, spec string, lockMilli int64, job DgJob) {
	if dc.locker == nil {
		dc.AddJob(name, spec, job)
		return
	}

	log.Printf("add spec job, name: %s, spec: %s, lockMilli: %d", name, spec, lockMilli)
	_, _ = dc.cron.AddFunc(spec, func() {
		ctx := &dgctx.DgContext{TraceId: uuid.NewString()}
		if dc.locker.DoLock(ctx, name, lockMilli) {
			defer dc.locker.Unlock(ctx, name)
			job(ctx)
		}
	})
}

func (dc *DgCron) AddFixDurationJob(name string, duration time.Duration, job DgJob) {
	log.Printf("add fix duration job, name: %s, duration: %s", name, duration)
	ticker := time.NewTicker(duration)

	go func() {
		for {
			select {
			case <-ticker.C:
				job(&dgctx.DgContext{TraceId: uuid.NewString()})
			}
		}
	}()
}

func (dc *DgCron) AddFixDelayJob(name string, delay time.Duration, job DgJob) {
	log.Printf("add fix delay job, name: %s, delay: %s", name, delay)

	go func() {
		for {
			time.AfterFunc(delay, func() {
				job(&dgctx.DgContext{TraceId: uuid.NewString()})
			})
		}
	}()
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
