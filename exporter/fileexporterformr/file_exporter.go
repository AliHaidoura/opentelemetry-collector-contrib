// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fileexporterformr // Package fileexporterformr import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/fileexporterformr"

import (
	"context"
	"io"
	"os"
	"sync"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// Marshaler configuration used for marhsaling Protobuf to JSON.
var tracesMarshaler = ptrace.NewJSONMarshaler()

// fileExporter is the implementation of file exporter that writes telemetry data to a file
// in Protobuf-JSON format.
type fileExporter struct {
	path  string
	file  io.WriteCloser
	mutex sync.Mutex
}

type traceId = [16]byte

type filePerRunExporter struct {
	instrumentationName string
	filePerRunMap       map[traceId]*fileExporter
}

func (e *filePerRunExporter) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (e *filePerRunExporter) ConsumeTraces(_ context.Context, td ptrace.Traces) error {

	for i := 0; i < td.ResourceSpans().Len(); i++ {
		td.ResourceSpans().At(i).ScopeSpans().RemoveIf(func(spans ptrace.ScopeSpans) bool {
			return spans.Scope().Name() != e.instrumentationName
		})
	}
	td.ResourceSpans().RemoveIf(func(rs ptrace.ResourceSpans) bool {
		return rs.ScopeSpans().Len() == 0
	})
	tracesByTraceId := splitByTrace(td.ResourceSpans())
	for key, val := range tracesByTraceId {
		if _, ok := e.filePerRunMap[key]; !ok {
			fe := &fileExporter{path: "run_trace_" + string(key[:]) + ".json"}
			var err error
			fe.file, err = os.OpenFile(fe.path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
			if err != nil {
				return nil
			}
		}
		err := e.filePerRunMap[key].ConsumeTraces(val)
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *fileExporter) ConsumeTraces(td ptrace.Traces) error {
	buf, err := tracesMarshaler.MarshalTraces(td)
	if err != nil {
		return err
	}
	// Ensure only one write operation happens at a time.
	e.mutex.Lock()
	defer e.mutex.Unlock()
	if _, err := e.file.Write(buf); err != nil {
		return err
	}
	if _, err := io.WriteString(e.file, "\n"); err != nil {
		return err
	}
	return nil
}

func (e *filePerRunExporter) Start(context.Context, component.Host) error {
	return nil
}

// Shutdown stops the exporter and is invoked during shutdown.
func (e *filePerRunExporter) Shutdown(context.Context) error {
	for _, v := range e.filePerRunMap {
		if e := v.file.Close(); e != nil {
			return e
		}
	}
	return nil
}
