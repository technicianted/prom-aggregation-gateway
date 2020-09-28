package main

import (
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/pmezard/go-difflib/difflib"
)

const (
	in1 = `
# HELP gauge A gauge
# TYPE gauge gauge
gauge 42
# HELP counter A counter
# TYPE counter counter
counter 31
# HELP histogram A histogram
# TYPE histogram histogram
histogram_bucket{le="1"} 0
histogram_bucket{le="2"} 0
histogram_bucket{le="3"} 3
histogram_bucket{le="4"} 4
histogram_bucket{le="5"} 4
histogram_bucket{le="6"} 4
histogram_bucket{le="7"} 4
histogram_bucket{le="8"} 4
histogram_bucket{le="9"} 4
histogram_bucket{le="10"} 4
histogram_bucket{le="+Inf"} 4
histogram_sum{} 2.5
histogram_count{} 1
`
	in2 = `
# HELP gauge A gauge
# TYPE gauge gauge
gauge 57
# HELP counter A counter
# TYPE counter counter
counter 29
# HELP histogram A histogram
# TYPE histogram histogram
histogram_bucket{le="1"} 0
histogram_bucket{le="2"} 0
histogram_bucket{le="3"} 0
histogram_bucket{le="4"} 4
histogram_bucket{le="5"} 5
histogram_bucket{le="6"} 5
histogram_bucket{le="7"} 5
histogram_bucket{le="8"} 5
histogram_bucket{le="9"} 5
histogram_bucket{le="10"} 5
histogram_bucket{le="+Inf"} 5
histogram_sum 4.5
histogram_count 1
`
	want = `# HELP counter A counter
# TYPE counter counter
counter 60
# HELP gauge A gauge
# TYPE gauge gauge
gauge 99
# HELP histogram A histogram
# TYPE histogram histogram
histogram_bucket{le="1"} 0
histogram_bucket{le="2"} 0
histogram_bucket{le="3"} 3
histogram_bucket{le="4"} 8
histogram_bucket{le="5"} 9
histogram_bucket{le="6"} 9
histogram_bucket{le="7"} 9
histogram_bucket{le="8"} 9
histogram_bucket{le="9"} 9
histogram_bucket{le="10"} 9
histogram_bucket{le="+Inf"} 9
histogram_sum 7
histogram_count 2
`
	// sample output when in1 and n2 go through pruning
	want2 = `# HELP counter A counter
# TYPE counter counter
counter 60
# HELP histogram A histogram
# TYPE histogram histogram
histogram_bucket{le="1"} 0
histogram_bucket{le="2"} 0
histogram_bucket{le="3"} 3
histogram_bucket{le="4"} 8
histogram_bucket{le="5"} 9
histogram_bucket{le="6"} 9
histogram_bucket{le="7"} 9
histogram_bucket{le="8"} 9
histogram_bucket{le="9"} 9
histogram_bucket{le="10"} 9
histogram_bucket{le="+Inf"} 9
histogram_sum 7
histogram_count 2
`

	multilabel1 = `# HELP counter A counter
# TYPE counter counter
counter{a="a",b="b"} 1
`
	multilabel2 = `# HELP counter A counter
# TYPE counter counter
counter{a="a",b="b"} 2
`
	multilabelResult = `# HELP counter A counter
# TYPE counter counter
counter{a="a",b="b"} 3
`
	labelFields1 = `# HELP ui_page_render_errors A counter
# TYPE ui_page_render_errors counter
ui_page_render_errors{path="/org/:orgId"} 1
ui_page_render_errors{path="/prom/:orgId"} 1
`
	labelFields2 = `# HELP ui_page_render_errors A counter
# TYPE ui_page_render_errors counter
ui_page_render_errors{path="/prom/:orgId"} 1
`
	labelFieldResult = `# HELP ui_page_render_errors A counter
# TYPE ui_page_render_errors counter
ui_page_render_errors{path="/org/:orgId"} 1
ui_page_render_errors{path="/prom/:orgId"} 2
`
	gaugeInput = `
# HELP ui_external_lib_loaded A gauge with entries in un-sorted order
# TYPE ui_external_lib_loaded gauge
ui_external_lib_loaded{name="ga",loaded="true"} 1
ui_external_lib_loaded{name="Intercom",loaded="true"} 1
ui_external_lib_loaded{name="mixpanel",loaded="true"} 1
`
	gaugeOutput = `# HELP ui_external_lib_loaded A gauge with entries in un-sorted order
# TYPE ui_external_lib_loaded gauge
ui_external_lib_loaded{loaded="true",name="Intercom"} 2
ui_external_lib_loaded{loaded="true",name="ga"} 2
ui_external_lib_loaded{loaded="true",name="mixpanel"} 2
`

	gaugeOutput2 = `# HELP ui_external_lib_loaded A gauge with entries in un-sorted order
# TYPE ui_external_lib_loaded gauge
ui_external_lib_loaded{loaded="true",name="Intercom"} 1
ui_external_lib_loaded{loaded="true",name="ga"} 1
ui_external_lib_loaded{loaded="true",name="mixpanel"} 1
`

	duplicateLabels = `
# HELP ui_external_lib_loaded Test with duplicate values
# TYPE ui_external_lib_loaded gauge
ui_external_lib_loaded{name="Munchkin",loaded="true"} 15171
ui_external_lib_loaded{name="Munchkin",loaded="true"} 1
`
	duplicateError = `Duplicate labels: {__name__="ui_external_lib_loaded", loaded="true", name="Munchkin"}`

	reorderedLabels1 = `# HELP counter A counter
# TYPE counter counter
counter{a="a",b="b"} 1
`
	reorderedLabels2 = `# HELP counter A counter
# TYPE counter counter
counter{b="b",a="a"} 2
`
	reorderedLabelsResult = `# HELP counter A counter
# TYPE counter counter
counter{a="a",b="b"} 3
`
)

func TestAggate(t *testing.T) {
	for i, c := range []struct {
		byJob         bool
		a             string
		aJob          string
		b             string
		bJob          string
		byJobDuration time.Duration
		waitDuration  time.Duration
		want          string
		err1          error
		err2          error
	}{
		{false, gaugeInput, "j1", gaugeInput, "j1", 1 * time.Minute, 0, gaugeOutput, nil, nil},
		// enable byJob, same input jobs will overwrite
		{true, gaugeInput, "j1", gaugeInput, "j1", 1 * time.Minute, 0, gaugeOutput2, nil, nil},
		// enable byJob, different jobs will aggregate to same output
		{true, gaugeInput, "j1", gaugeInput, "j2", 1 * time.Minute, 0, gaugeOutput, nil, nil},
		// enable byJob, enable pruning, all gauges expired
		{true, in1, "j1", in2, "j2", 5 * time.Millisecond, 8 * time.Millisecond, want2, nil, nil},
		// enable pruning, no gauges expired
		{false, in1, "j1", in2, "j1", 10 * time.Millisecond, 7 * time.Millisecond, want, nil, nil},

		{false, in1, "j1", in2, "j1", 1 * time.Minute, 0, want, nil, nil},
		{false, multilabel1, "j1", multilabel2, "j1", 1 * time.Minute, 0, multilabelResult, nil, nil},
		{false, labelFields1, "j1", labelFields2, "j1", 1 * time.Minute, 0, labelFieldResult, nil, nil},
		{false, duplicateLabels, "j1", "", "j1", 1 * time.Minute, 0, "", fmt.Errorf("%s", duplicateError), nil},
		{false, reorderedLabels1, "j1", reorderedLabels2, "j1", 1 * time.Minute, 0, reorderedLabelsResult, nil, nil},
	} {
		fmt.Printf("case: %d\n", i)

		a := newAggate(c.byJob, c.byJobDuration)

		if err := a.parseAndMerge(c.aJob, strings.NewReader(c.a)); err != nil {
			if c.err1 == nil {
				t.Fatalf("Unexpected error: %s", err)
			} else if c.err1.Error() != err.Error() {
				t.Fatalf("Expected %s, got %s", c.err1, err)
			}
		}

		time.Sleep(c.waitDuration)

		if err := a.parseAndMerge(c.bJob, strings.NewReader(c.b)); err != c.err2 {
			t.Fatalf("Expected %s, got %s", c.err2, err)
		}

		time.Sleep(c.waitDuration)

		r := httptest.NewRequest("GET", "http://example.com/foo", nil)
		w := httptest.NewRecorder()
		a.handler(w, r)

		if have := w.Body.String(); have != c.want {
			text, _ := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
				A:        difflib.SplitLines(c.want),
				B:        difflib.SplitLines(have),
				FromFile: "want",
				ToFile:   "have",
				Context:  3,
			})
			t.Fatalf("case: %d: %s", i, text)
		}
	}
}
