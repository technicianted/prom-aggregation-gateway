# Prometheus Aggregation Gateway

Prometheus Aggregation Gateway is a aggregating push gateway for Prometheus.  As opposed to the official [Prometheus Pushgateway](https://github.com/prometheus/pushgateway), this service aggregates the sample values it receives.

* Counters where all labels match are added up.
* Histograms are added up; if bucket boundaries are mismatched then the result has the union of all buckets and counts are given to the lowest bucket that fits.
* Gauges are also added up (but this may not make any sense)
* Summaries are discarded.

## Understanding jobs

In order to support the scenario where a single source may push its metrics multiple times, a change is made to allow the aggregation server to distinguish sources by job. This way, pushes from the same `job` value are treated as updates and are overwritten. However when scraping is performed, all metrics from all jobs are then aggregated.

The only challenge is with gauges. When a source disappears and is no longer pushing metrics, gauge aggregation for its values becomes irrelevant and will not give proper results. To work around this, a timeout can be set on a job that if it has not been updated during that timeout, its gauges will be reset.

The next section will explain how to use job aggregation.

## How to use

Send metrics in [Prometheus format](https://prometheus.io/docs/instrumenting/exposition_formats/) to `/metrics/`

E.g. if you have the program running locally:

```bash
echo 'http_requests_total{method="post",code="200"} 1027' | curl --data-binary @- http://localhost/metrics/
```

Now you can push your metrics using your favorite Prometheus client.

E.g. in Python using [prometheus/client_python](https://github.com/prometheus/client_python):

```python
from prometheus_client import CollectorRegistry, Counter, push_to_gateway
registry = CollectorRegistry()
counter = Counter('some_counter', "A counter", registry=registry)
counter.inc()
push_to_gateway('localhost', job='my_job_name', registry=registry)
```

Then have your Prometheus scrape metrics at `/metrics`.

To enable the job awareness (disabled by default):
* Enable by-job option in command line `-by-job`.
* Send metrics with `/metrics/job/<job>` URL path, where `job` is a unique identifier of the source.
* To set gauge metrics reset expiry for dead jobs you can use `-job-prune-duration` to set the timeout duration.

## Ready-built images

Available on DockerHub `weaveworks/prom-aggregation-gateway`

## Comparison to [Prometheus Pushgateway](https://github.com/prometheus/pushgateway)

According to https://prometheus.io/docs/practices/pushing/:

> The Pushgateway never forgets series pushed to it and will expose them to Prometheus forever...
>
> The latter point is especially relevant when multiple instances of a job differentiate their metrics in the Pushgateway via an instance label or similar.

This restriction makes the Prometheus pushgateway inappropriate for the usecase of accepting metrics from a client-side web app, so we created this one to aggregate counters from multiple senders.

Prom-aggregation-gateway presents a similar API, but does not attempt to be a drop-in replacement.

## JS Client Library

See https://github.com/weaveworks/promjs/ for a JS client library for Prometheus that can be used from within a web app.

## <a name="help"></a>Getting Help

If you have any questions about, feedback for or problems with `prom-aggregation-gateway`:

- Invite yourself to the <a href="https://slack.weave.works/" target="_blank">Weave Users Slack</a>.
- Ask a question on the [#general](https://weave-community.slack.com/messages/general/) slack channel.
- [File an issue](https://github.com/weaveworks/prom-aggregation-gateway/issues/new).

Your feedback is always welcome!
