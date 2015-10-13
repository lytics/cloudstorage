package logging

import (
	"fmt"
	"path"
	"runtime"
)

const (
	NOLOGGING = -1
	FATAL     = 0
	ERROR     = 1
	WARN      = 2
	INFO      = 3
	DEBUG     = 4
)

func Whoami(skip int) string {
	pc, _, ln, ok := runtime.Caller(skip + 1)
	//pc, file, ln, ok := runtime.Caller(skip+1)
	if !ok {
		return "unknown"
	}
	funcPc := runtime.FuncForPC(pc)
	if funcPc == nil {
		return "unnamed"
	}

	pathname := funcPc.Name()
	name := path.Base(pathname)
	return fmt.Sprintf("%v:%v", name, ln)
}
