package testutils

import (
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/lytics/cloudstorage"
	"github.com/lytics/cloudstorage/logging"
)

const (
	NOLOGGING = -1
	FATAL     = 0
	ERROR     = 1
	WARN      = 2
	INFO      = 3
	DEBUG     = 4
)

var localconfig = &cloudstorage.CloudStoreContext{
	LogggingContext: "unittest",
	TokenSource:     cloudstorage.LocalFileSource,
	LocalFS:         "/tmp/mockcloud",
	TmpDir:          "/tmp/localcache",
}

var gcsIntconfig = &cloudstorage.CloudStoreContext{
	LogggingContext: "integration-test",
	TokenSource:     cloudstorage.GCEDefaultOAuthToken,
	Project:         "lyticsstaging",
	Bucket:          "cloudstore-tests",
	TmpDir:          "/tmp/localcache",
}

func CreateStore(t *testing.T) cloudstorage.Store {

	cloudstorage.LogConstructor = func(prefix string) logging.Logger {
		return logging.NewStdLogger(true, logging.DEBUG, prefix)
		//return testutils.NewStdLogger(t, prefix)
	}

	var config *cloudstorage.CloudStoreContext
	if os.Getenv("TESTINT") == "" {
		//os.RemoveAll("/tmp/mockcloud")
		//os.RemoveAll("/tmp/localcache")
		config = localconfig
	} else {
		config = gcsIntconfig
	}
	store, err := cloudstorage.NewStore(config)
	AssertEq(t, nil, err, "error.")

	return store
}

func Clearstore(t *testing.T, store cloudstorage.Store) {
	q := cloudstorage.Query{"", nil}
	q.Sorted()
	objs, err := store.List(q)
	AssertEq(t, nil, err, "error.")
	for _, o := range objs {
		t.Logf("clearstore(): deleting %v", o.Name())
		store.Delete(o.Name())
	}

	if os.Getenv("TESTINT") != "" {
		//GCS is lazy about deletes...
		time.Sleep(15 * time.Second)
	}
}

func NewStdLogger(t *testing.T, prefix string) logging.Logger {
	return &testlogger{t, DEBUG, prefix}
}

type testlogger struct {
	t         *testing.T
	LogLevel  int
	LogPrefix string
}

func (l *testlogger) Debug(v ...interface{}) {
	l.logP(DEBUG, v...)
}

func (l *testlogger) Debugf(format string, v ...interface{}) {
	l.logPf(DEBUG, format, v...)
}

func (l *testlogger) Info(v ...interface{}) {
	l.logP(INFO, v...)
}

func (l *testlogger) Infof(format string, v ...interface{}) {
	l.logPf(INFO, format, v...)
}

func (l *testlogger) Warn(v ...interface{}) {
	l.logP(WARN, v...)
}

func (l *testlogger) Warnf(format string, v ...interface{}) {
	l.logPf(WARN, format, v...)
}

func (l *testlogger) Error(v ...interface{}) {
	l.logP(ERROR, v...)
}

func (l *testlogger) Errorf(format string, v ...interface{}) {
	l.logPf(ERROR, format, v...)
}

func (l *testlogger) logP(logLvl int, v ...interface{}) {
	if l.LogLevel >= logLvl && l.t != nil {
		l.t.Log(
			l.LogPrefix + fmt.Sprint(v...))
	}
}

func (l *testlogger) logPf(logLvl int, format string, v ...interface{}) {
	if l.LogLevel >= logLvl && l.t != nil {
		l.t.Log(
			l.LogPrefix + fmt.Sprintf(format, v...))
	}
}

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
// ~ Test Utilizes
// ~
func AssertEq(t *testing.T, exp interface{}, got interface{}, v ...interface{}) {
	if reflect.DeepEqual(exp, got) {
		return
	}

	//gv := reflect.ValueOf(got)
	//ev := reflect.ValueOf(exp)

	t.Logf("caller   : %v", logging.Whoami(1))
	if len(v) == 0 {
		////////////////////
		t.Error("fatal")
	} else if len(v) == 1 {
		t.Errorf("fatal    : %s", fmt.Sprintf("%v", v[0]))
	} else {
		v2 := v[1:]
		format, ok := v[0].(string)
		if ok {
			t.Errorf("fatal    : %s", fmt.Sprintf(format, v2...))
		} else {
			t.Errorf("???format=%T??? : msg:%v", v[0], v2)
		}
	}

	t.Logf("exp      :\n[%v]", exp)
	t.Logf("got      :\n[%v]", got)
	/*
		if gv.Type() != ev.Type() {
			t.Logf("T != T   : %v != %v", gv.Type(), ev.Type())
		}

		g, e := gv.Interface(), ev.Interface()
		if g != e {
			t.Errorf("Hex: %q != %q", e, g)
		}
	*/
	t.FailNow()
}

func AssertT(t *testing.T, eval bool, format string, v ...interface{}) {
	if eval {
		return
	}

	if len(v) == 0 {
		t.Fatalf(format)
	} else {
		t.Fatalf(format, v...)
	}
}
