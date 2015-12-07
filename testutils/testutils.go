package testutils

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/lytics/cloudstorage/logging"
)

type testlogger struct {
	t            *testing.T
	LogLevel     int
	LogLvlPrefix map[int]string
	LogPrefix    string
	LogPostfix   string
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
			l.LogPrefix + l.LogLvlPrefix[logLvl] + fmt.Sprint(v...) + l.LogPostfix)
	}
}

func (l *testlogger) logPf(logLvl int, format string, v ...interface{}) {
	if l.LogLevel >= logLvl && l.t != nil {
		l.t.Log(
			l.LogPrefix + l.LogLvlPrefix[logLvl] + fmt.Sprintf(format, v...) + l.LogPostfix)
	}
}

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
// ~ Test Utilizes
// ~
func AssertEq(t *testing.T, exp interface{}, got interface{}, v ...interface{}) {
	if reflect.DeepEqual(exp, got) {
		return
	}

	gv := reflect.ValueOf(got)
	ev := reflect.ValueOf(exp)

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
	if gv.Type() != ev.Type() {
		t.Logf("T != T   : %v != %v", gv.Type(), ev.Type())
	}

	g, e := gv.Interface(), ev.Interface()
	if g != e {
		t.Errorf("Hex: %q != %q", e, g)
	}

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
