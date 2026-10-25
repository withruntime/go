package withruntime

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestUsageExportSendsTheRangeAndSaysWhatIsFinal(t *testing.T) {
	t.Parallel()
	var query string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/usage/export" {
			http.NotFound(w, r)
			return
		}
		query = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"since":"2026-09-01T00:00:00Z","until":"2026-10-01T00:00:00Z","nextCursor":null,
			"finalThrough":"2026-09-20T10:00:00.123456Z","csv":"header\n",
			"data":[{"intervalId":"i","resourceId":"r","name":null,"kind":"sandbox","meter":"runtime","fundedBy":"credit",
			"startsAt":"a","endsAt":"b","settledAt":"c","billedMilliseconds":"60000","chargedMicros":"9007199254740993",
			"quotedRates":[{"component":"vcpu","unit":"vcpu_second","rateMicros":"14"}],"measurementsJson":null,"evidence":"measured","final":true}]}`))
	}))
	defer server.Close()
	c, err := New(WithAPIKey(testKey), WithBaseURL(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	page, err := c.UsageExport(context.Background(), UsageExportOptions{Since: "2026-09-01T00:00:00Z", Until: "2026-10-01T00:00:00Z", Cursor: "next+/=", Limit: 500})
	if err != nil {
		t.Fatal(err)
	}
	if want := "cursor=next%2B%2F%3D&limit=500&since=2026-09-01T00%3A00%3A00Z&until=2026-10-01T00%3A00%3A00Z"; query != want {
		t.Fatalf("query %q, want %q", query, want)
	}
	if page.FinalThrough == nil || *page.FinalThrough != "2026-09-20T10:00:00.123456Z" || page.NextCursor != nil {
		t.Fatalf("finalThrough or nextCursor lost: %+v", page)
	}
	if len(page.Data) != 1 || !page.Data[0].Final || page.Data[0].ChargedMicros != "9007199254740993" || page.Data[0].QuotedRates[0].RateMicros != "14" {
		t.Fatalf("row decoded wrong: %+v", page.Data)
	}
}
