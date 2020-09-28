package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

func lablesLessThan(a, b []*dto.LabelPair) bool {
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		if *a[i].Name != *b[j].Name {
			return *a[i].Name < *b[j].Name
		}
		if *a[i].Value != *b[j].Value {
			return *a[i].Value < *b[j].Value
		}
		i++
		j++
	}
	return len(a) < len(b)
}

type byLabel []*dto.Metric

func (a byLabel) Len() int           { return len(a) }
func (a byLabel) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byLabel) Less(i, j int) bool { return lablesLessThan(a[i].Label, a[j].Label) }

// Sort a slice of LabelPairs by name
type byName []*dto.LabelPair

func (a byName) Len() int           { return len(a) }
func (a byName) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byName) Less(i, j int) bool { return a[i].GetName() < a[j].GetName() }

func uint64ptr(a uint64) *uint64 {
	return &a
}

func float64ptr(a float64) *float64 {
	return &a
}

func mergeBuckets(a, b []*dto.Bucket) []*dto.Bucket {
	output := []*dto.Bucket{}
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		if *a[i].UpperBound < *b[j].UpperBound {
			output = append(output, a[i])
			i++
		} else if *a[i].UpperBound > *b[j].UpperBound {
			output = append(output, b[j])
			j++
		} else {
			output = append(output, &dto.Bucket{
				CumulativeCount: uint64ptr(*a[i].CumulativeCount + *b[j].CumulativeCount),
				UpperBound:      a[i].UpperBound,
			})
			i++
			j++
		}
	}
	for ; i < len(a); i++ {
		output = append(output, a[i])
	}
	for ; j < len(b); j++ {
		output = append(output, b[j])
	}
	return output
}

func mergeMetric(ty dto.MetricType, a, b *dto.Metric) *dto.Metric {
	switch ty {
	case dto.MetricType_COUNTER:
		return &dto.Metric{
			Label: a.Label,
			Counter: &dto.Counter{
				Value: float64ptr(*a.Counter.Value + *b.Counter.Value),
			},
		}

	case dto.MetricType_GAUGE:
		// No very meaninful way for us to merge gauges.  We'll sum them
		// and clear out any gauges on scrape, as a best approximation, but
		// this relies on client pushing with the same interval as we scrape.
		return &dto.Metric{
			Label: a.Label,
			Gauge: &dto.Gauge{
				Value: float64ptr(*a.Gauge.Value + *b.Gauge.Value),
			},
		}

	case dto.MetricType_HISTOGRAM:
		return &dto.Metric{
			Label: a.Label,
			Histogram: &dto.Histogram{
				SampleCount: uint64ptr(*a.Histogram.SampleCount + *b.Histogram.SampleCount),
				SampleSum:   float64ptr(*a.Histogram.SampleSum + *b.Histogram.SampleSum),
				Bucket:      mergeBuckets(a.Histogram.Bucket, b.Histogram.Bucket),
			},
		}

	case dto.MetricType_UNTYPED:
		return &dto.Metric{
			Label: a.Label,
			Untyped: &dto.Untyped{
				Value: float64ptr(*a.Untyped.Value + *b.Untyped.Value),
			},
		}

	case dto.MetricType_SUMMARY:
		// No way of merging summaries, abort.
		return nil
	}

	return nil
}

func mergeFamily(a, b *dto.MetricFamily, overwrite bool) (*dto.MetricFamily, error) {
	if *a.Type != *b.Type {
		return nil, fmt.Errorf("Cannot merge metric '%s': type %s != %s",
			*a.Name, a.Type.String(), b.Type.String())
	}

	output := &dto.MetricFamily{
		Name: a.Name,
		Help: a.Help,
		Type: a.Type,
	}

	i, j := 0, 0
	for i < len(a.Metric) && j < len(b.Metric) {
		if lablesLessThan(a.Metric[i].Label, b.Metric[j].Label) {
			output.Metric = append(output.Metric, a.Metric[i])
			i++
		} else if lablesLessThan(b.Metric[j].Label, a.Metric[i].Label) {
			output.Metric = append(output.Metric, b.Metric[j])
			j++
		} else {
			if overwrite {
				output.Metric = append(output.Metric, b.Metric[j])
			} else {
				merged := mergeMetric(*a.Type, a.Metric[i], b.Metric[j])
				if merged != nil {
					output.Metric = append(output.Metric, merged)
				}
			}
			i++
			j++
		}
	}
	for ; i < len(a.Metric); i++ {
		output.Metric = append(output.Metric, a.Metric[i])
	}
	for ; j < len(b.Metric); j++ {
		output.Metric = append(output.Metric, b.Metric[j])
	}
	return output, nil
}

type aggate struct {
	byJob            bool
	jobPruneDuration time.Duration
	familiesLock     sync.RWMutex
	familiesByJob    map[string]*jobFamily
}

type jobFamily struct {
	lastUpdate time.Time
	families   map[string]*dto.MetricFamily
}

func newAggate(byJob bool, jobPruneDuration time.Duration) *aggate {
	return &aggate{
		byJob:            byJob,
		jobPruneDuration: jobPruneDuration,
		familiesByJob:    map[string]*jobFamily{},
	}
}

func validateFamily(f *dto.MetricFamily) error {
	// Map of fingerprints we've seen before in this family
	fingerprints := make(map[model.Fingerprint]struct{}, len(f.Metric))
	for _, m := range f.Metric {
		// Turn protobuf LabelSet into Prometheus model LabelSet
		lset := make(model.LabelSet, len(m.Label)+1)
		for _, p := range m.Label {
			lset[model.LabelName(p.GetName())] = model.LabelValue(p.GetValue())
		}
		lset[model.MetricNameLabel] = model.LabelValue(f.GetName())
		if err := lset.Validate(); err != nil {
			return err
		}
		fingerprint := lset.Fingerprint()
		if _, found := fingerprints[fingerprint]; found {
			return fmt.Errorf("Duplicate labels: %v", lset)
		}
		fingerprints[fingerprint] = struct{}{}
	}
	return nil
}

func (a *aggate) parseAndMerge(job string, r io.Reader) error {
	var parser expfmt.TextParser
	inFamilies, err := parser.TextToMetricFamilies(r)
	if err != nil {
		return err
	}

	a.familiesLock.Lock()
	defer a.familiesLock.Unlock()
	for name, family := range inFamilies {
		// Sort labels in case source sends them inconsistently
		for _, m := range family.Metric {
			sort.Sort(byName(m.Label))
		}

		if err := validateFamily(family); err != nil {
			return err
		}

		// family must be sorted for the merge
		sort.Sort(byLabel(family.Metric))

		if _, ok := a.familiesByJob[job]; !ok {
			a.familiesByJob[job] = &jobFamily{
				families: make(map[string]*dto.MetricFamily),
			}
		}
		a.familiesByJob[job].lastUpdate = time.Now()
		existingFamily, ok := a.familiesByJob[job].families[name]
		if !ok {
			a.familiesByJob[job].families[name] = family
			continue
		}

		merged, err := mergeFamily(existingFamily, family, a.byJob)
		if err != nil {
			return err
		}

		a.familiesByJob[job].families[name] = merged
	}

	return nil
}

// prune performs pruning of expired metrics.
// the only case is to reset gauges.
func (a *aggate) prune(job *jobFamily) {
	prunedFamilies := make([]string, 0)
	for name, family := range job.families {
		if *family.Type == dto.MetricType_GAUGE {
			prunedFamilies = append(prunedFamilies, name)
		}
	}
	for _, name := range prunedFamilies {
		delete(job.families, name)
	}
}

func (a *aggate) handler(w http.ResponseWriter, r *http.Request) {
	contentType := expfmt.Negotiate(r.Header)
	w.Header().Set("Content-Type", string(contentType))
	enc := expfmt.NewEncoder(w, contentType)

	a.familiesLock.RLock()
	defer a.familiesLock.RUnlock()

	mergedFamilies := make(map[string]*dto.MetricFamily)
	for jobName, job := range a.familiesByJob {
		if time.Since(job.lastUpdate) > a.jobPruneDuration {
			fmt.Printf("PRUNING %v: %v, %v, %v\n", jobName, job.lastUpdate, a.jobPruneDuration, time.Since(job.lastUpdate))
			a.prune(job)
		}

		for name, family := range job.families {
			existingFamily, ok := mergedFamilies[name]
			if !ok {
				mergedFamilies[name] = family
				continue
			}

			merged, err := mergeFamily(existingFamily, family, false)
			if err != nil {
				http.Error(w, "An error has occurred during metrics merge:\n\n"+err.Error(), http.StatusInternalServerError)
				return
			}

			mergedFamilies[name] = merged
		}
	}

	metricNames := []string{}
	for name := range mergedFamilies {
		metricNames = append(metricNames, name)
	}
	sort.Sort(sort.StringSlice(metricNames))

	for _, name := range metricNames {
		if err := enc.Encode(mergedFamilies[name]); err != nil {
			http.Error(w, "An error has occurred during metrics encoding:\n\n"+err.Error(), http.StatusInternalServerError)
			return
		}
	}
}

func main() {
	listen := flag.String("listen", ":80", "Address and port to listen on.")
	cors := flag.String("cors", "*", "The 'Access-Control-Allow-Origin' value to be returned.")
	pushPath := flag.String("push-path", "/metrics/", "HTTP path to accept pushed metrics.")
	byJob := flag.Bool("by-job", false, "Aggregate metrics by job")
	jobPruneDuration := flag.Duration("job-prune-duration", 90*time.Second, "Duration after which job metric families are pruned if it had not been updated")
	flag.Parse()

	a := newAggate(*byJob, *jobPruneDuration)
	http.HandleFunc("/metrics", a.handler)
	http.HandleFunc(*pushPath, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", *cors)

		job := ""
		if *byJob {
			p, err := filepath.Rel(*pushPath, r.URL.Path)
			if err != nil {
				http.Error(w, fmt.Sprintf("failed to parse URL path: %v", err.Error()), http.StatusBadRequest)
				return
			}
			parts := strings.Split(p, "/")
			if len(parts) < 2 {
				http.Error(w, fmt.Sprintf("path is too short, expecting > 2"), http.StatusBadRequest)
				return
			}
			if parts[0] != "job" {
				http.Error(w, fmt.Sprintf("first path components != job: %v", parts[0]), http.StatusBadRequest)
				return
			}
			job = parts[1]
		}

		if err := a.parseAndMerge(job, r.Body); err != nil {
			log.Println(err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	})
	log.Fatal(http.ListenAndServe(*listen, nil))
}
