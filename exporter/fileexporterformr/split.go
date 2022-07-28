package fileexporterformr

import (
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func splitByTrace(rss ptrace.ResourceSpansSlice) map[traceId]ptrace.Traces {
	// for each span in the resource spans, we group them into batches of rs/traceID.

	batches := map[traceId]ptrace.ResourceSpansSlice{}

	for k := 0; k < rss.Len(); k++ {
		rs := rss.At(k)
		for i := 0; i < rs.ScopeSpans().Len(); i++ {

			ss := rs.ScopeSpans().At(i)
			for j := 0; j < ss.Spans().Len(); j++ {
				span := ss.Spans().At(j)
				key := span.TraceID().Bytes()

				// for the first traceID in the ILS, initialize the map entry
				// and add the singleTraceBatch to the result list

				newRS := ptrace.NewResourceSpans()
				// currently, the ResourceSpans implementation has only a Resource and an ILS. We'll copy the Resource
				// and set our own ILS
				rs.Resource().CopyTo(newRS.Resource())
				newRS.SetSchemaUrl(rs.SchemaUrl())

				newSS := ptrace.NewScopeSpans()
				// currently, the ILS implementation has only an InstrumentationLibrary and spans. We'll copy the library
				// and set our own spans
				ss.Spans().MoveAndAppendTo(newSS.Spans())
				ss.Scope().MoveTo(newSS.Scope())
				newSS.SetSchemaUrl(ss.SchemaUrl())
				newSS.MoveTo(newRS.ScopeSpans().AppendEmpty())
				if _, found := batches[key]; !found {
					batches[key] = ptrace.NewResourceSpansSlice()
				}
				newRS.MoveTo(batches[key].AppendEmpty())
			}
		}
	}

	result := map[traceId]ptrace.Traces{}
	for key, rss := range batches {
		trace := ptrace.NewTraces()
		rss.MoveAndAppendTo(trace.ResourceSpans())
		result[key] = trace
	}
	return result
}
