package logging

import (
	"fmt"
	"log"
	"os"
)

//A logging interface
type Logger interface {
	Debug(v ...interface{})
	Debugf(format string, v ...interface{})

	Info(v ...interface{})
	Infof(format string, v ...interface{})

	Warn(v ...interface{})
	Warnf(format string, v ...interface{})

	Error(v ...interface{})
	Errorf(format string, v ...interface{})
}

func NewStdLogger(usercolor bool, loglvl int, prefix string) Logger {
	logPrefix := map[int]string{
		ERROR: "[ERROR] ",
		WARN:  "[WARN] ",
		INFO:  "[INFO] ",
		DEBUG: "[DEBUG] ",
	}
	postfix := ""

	if usercolor {
		logColor := map[int]string{
			ERROR: "\033[0m\033[31m",
			WARN:  "\033[0m\033[33m",
			INFO:  "\033[0m\033[35m",
			DEBUG: "\033[0m\033[34m",
		}

		for lvl, color := range logColor {
			logPrefix[lvl] = color + logPrefix[lvl]
		}

		postfix = "\033[0m"
	}

	l := &stdlogger{
		logger:       log.New(os.Stderr, "", log.LstdFlags|log.Lshortfile|log.Lmicroseconds),
		LogLevel:     loglvl,
		LogLvlPrefix: logPrefix,
		LogPrefix:    prefix,
		LogPostfix:   postfix,
	}

	return l
}

type stdlogger struct {
	logger       *log.Logger
	LogLevel     int
	LogLvlPrefix map[int]string
	LogPrefix    string
	LogPostfix   string
}

func (l *stdlogger) Debug(v ...interface{}) {
	l.logP(DEBUG, v...)
}

func (l *stdlogger) Debugf(format string, v ...interface{}) {
	l.logPf(DEBUG, format, v...)
}

func (l *stdlogger) Info(v ...interface{}) {
	l.logP(INFO, v...)
}

func (l *stdlogger) Infof(format string, v ...interface{}) {
	l.logPf(INFO, format, v...)
}

func (l *stdlogger) Warn(v ...interface{}) {
	l.logP(WARN, v...)
}

func (l *stdlogger) Warnf(format string, v ...interface{}) {
	l.logPf(WARN, format, v...)
}

func (l *stdlogger) Error(v ...interface{}) {
	l.logP(ERROR, v...)
}

func (l *stdlogger) Errorf(format string, v ...interface{}) {
	l.logPf(ERROR, format, v...)
}

func (l *stdlogger) logP(logLvl int, v ...interface{}) {
	if l.LogLevel >= logLvl && l.logger != nil {
		l.logger.Output(3,
			l.LogPrefix+l.LogLvlPrefix[logLvl]+fmt.Sprint(v...)+l.LogPostfix)
	}
}

func (l *stdlogger) logPf(logLvl int, format string, v ...interface{}) {
	if l.LogLevel >= logLvl && l.logger != nil {
		l.logger.Output(3,
			l.LogPrefix+l.LogLvlPrefix[logLvl]+fmt.Sprintf(format, v...)+l.LogPostfix)
	}
}
