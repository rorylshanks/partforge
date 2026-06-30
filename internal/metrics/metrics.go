package metrics

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/PostHog/partforge/internal/manifest"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

type PartStats struct {
	Count uint64
	Rows  uint64
	Bytes uint64
}

type QueryProgress struct {
	ReadRows     uint64
	ReadBytes    uint64
	WrittenRows  uint64
	WrittenBytes uint64
}

type StageProgress struct {
	Stage                   string
	StageElapsed            time.Duration
	TotalElapsed            time.Duration
	CompletedStageDurations map[string]time.Duration
}

type Recorder interface {
	ForgeStarted(manifest.Manifest)
	ForgeCompleted(manifest.Manifest)
	ForgeFailed(manifest.Manifest)
	ObserveProgress(manifest.Manifest, QueryProgress, QueryProgress)
	ClearCurrentProgress(manifest.Manifest)
	SetActivePartStats(string, manifest.Manifest, PartStats)
	ObserveStageProgress(manifest.Manifest, StageProgress)
	ClearStageProgress(manifest.Manifest)
	CompactionStarted(jobID, outputPartID string, inputArtifacts uint64, inputStats PartStats)
	CompactionCompleted(jobID, outputPartID string, inputStats, outputStats PartStats)
	CompactionFailed(jobID, outputPartID string)
	CompactionNoReduction(jobID, outputPartID string, inputStats, outputStats PartStats)
	SetCompactPartStats(role, jobID, outputPartID string, stats PartStats)
}

type Noop struct{}

func (Noop) ForgeStarted(manifest.Manifest)                                  {}
func (Noop) ForgeCompleted(manifest.Manifest)                                {}
func (Noop) ForgeFailed(manifest.Manifest)                                   {}
func (Noop) ObserveProgress(manifest.Manifest, QueryProgress, QueryProgress) {}
func (Noop) ClearCurrentProgress(manifest.Manifest)                          {}
func (Noop) SetActivePartStats(string, manifest.Manifest, PartStats)         {}
func (Noop) ObserveStageProgress(manifest.Manifest, StageProgress)           {}
func (Noop) ClearStageProgress(manifest.Manifest)                            {}
func (Noop) CompactionStarted(string, string, uint64, PartStats)             {}
func (Noop) CompactionCompleted(string, string, PartStats, PartStats)        {}
func (Noop) CompactionFailed(string, string)                                 {}
func (Noop) CompactionNoReduction(string, string, PartStats, PartStats)      {}
func (Noop) SetCompactPartStats(string, string, string, PartStats)           {}

type Prometheus struct {
	registry                *prometheus.Registry
	clickHouseScrapeTimeout time.Duration
	httpClient              *http.Client
	targetMu                sync.RWMutex
	clickHouseTarget        string
	stageMu                 sync.Mutex
	currentStageByPart      map[string]string
	forgeStartedAt          map[string]time.Time
	compactionStartedAt     map[string]time.Time

	forgesStarted   *prometheus.CounterVec
	forgesCompleted *prometheus.CounterVec
	forgesFailed    *prometheus.CounterVec
	forgeDuration   *prometheus.HistogramVec

	rowsReadTotal     *prometheus.CounterVec
	bytesReadTotal    *prometheus.CounterVec
	rowsWrittenTotal  *prometheus.CounterVec
	bytesWrittenTotal *prometheus.CounterVec

	currentReadRows     *prometheus.GaugeVec
	currentReadBytes    *prometheus.GaugeVec
	currentWrittenRows  *prometheus.GaugeVec
	currentWrittenBytes *prometheus.GaugeVec

	activePartCount *prometheus.GaugeVec
	activePartRows  *prometheus.GaugeVec
	activePartBytes *prometheus.GaugeVec

	currentStage          *prometheus.GaugeVec
	currentStageElapsed   *prometheus.GaugeVec
	rewriteTotalElapsed   *prometheus.GaugeVec
	completedStageSeconds *prometheus.GaugeVec

	compactionsStarted     *prometheus.CounterVec
	compactionsCompleted   *prometheus.CounterVec
	compactionsFailed      *prometheus.CounterVec
	compactionsNoReduction *prometheus.CounterVec
	compactionDuration     *prometheus.HistogramVec
	compactInputArtifacts  *prometheus.GaugeVec
	compactPartCount       *prometheus.GaugeVec
	compactPartRows        *prometheus.GaugeVec
	compactPartBytes       *prometheus.GaugeVec

	clickHouseMetricsUp             prometheus.Gauge
	clickHouseMetricsScrapes        prometheus.Counter
	clickHouseMetricsScrapeFailures prometheus.Counter
	clickHouseMetricsScrapeDuration prometheus.Histogram
}

const defaultClickHouseScrapeTimeout = 5 * time.Second

func NewPrometheus() *Prometheus {
	p := &Prometheus{
		registry:                prometheus.NewRegistry(),
		clickHouseScrapeTimeout: defaultClickHouseScrapeTimeout,
		httpClient:              &http.Client{},
		currentStageByPart:      map[string]string{},
		forgeStartedAt:          map[string]time.Time{},
		compactionStartedAt:     map[string]time.Time{},
		forgesStarted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "partforge_forges_started_total",
			Help: "Total source part forge attempts started.",
		}, []string{"job_id", "source_table", "destination_table"}),
		forgesCompleted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "partforge_forges_completed_total",
			Help: "Total source part forge attempts completed successfully.",
		}, []string{"job_id", "source_table", "destination_table"}),
		forgesFailed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "partforge_forges_failed_total",
			Help: "Total source part forge attempts that failed.",
		}, []string{"job_id", "source_table", "destination_table"}),
		forgeDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "partforge_forge_duration_seconds",
			Help:    "Wall clock duration of source part forge attempts.",
			Buckets: prometheus.ExponentialBuckets(1, 2, 16),
		}, []string{"job_id", "source_table", "destination_table", "result"}),
		rowsReadTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "partforge_rows_read_total",
			Help: "Total rows read by ClickHouse INSERT SELECT rewrite queries, updated live from system.processes.",
		}, []string{"job_id", "source_table", "destination_table"}),
		bytesReadTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "partforge_bytes_read_total",
			Help: "Total uncompressed bytes read by ClickHouse INSERT SELECT rewrite queries, updated live from system.processes.",
		}, []string{"job_id", "source_table", "destination_table"}),
		rowsWrittenTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "partforge_rows_written_total",
			Help: "Total rows written by ClickHouse INSERT SELECT rewrite queries, updated live from system.processes.",
		}, []string{"job_id", "source_table", "destination_table"}),
		bytesWrittenTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "partforge_bytes_written_total",
			Help: "Total bytes written by ClickHouse INSERT SELECT rewrite queries, updated live from system.processes.",
		}, []string{"job_id", "source_table", "destination_table"}),
		currentReadRows: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "partforge_current_read_rows",
			Help: "Current read rows for the active source part rewrite query.",
		}, []string{"job_id", "part_id"}),
		currentReadBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "partforge_current_read_bytes",
			Help: "Current read bytes for the active source part rewrite query.",
		}, []string{"job_id", "part_id"}),
		currentWrittenRows: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "partforge_current_written_rows",
			Help: "Current written rows for the active source part rewrite query.",
		}, []string{"job_id", "part_id"}),
		currentWrittenBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "partforge_current_written_bytes",
			Help: "Current written bytes for the active source part rewrite query.",
		}, []string{"job_id", "part_id"}),
		activePartCount: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "partforge_active_part_count",
			Help: "Active part count measured while source or destination parts are attached in the worker.",
		}, []string{"role", "job_id", "part_id"}),
		activePartRows: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "partforge_active_part_rows",
			Help: "Active part rows measured while source or destination parts are attached in the worker.",
		}, []string{"role", "job_id", "part_id"}),
		activePartBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "partforge_active_part_bytes",
			Help: "Active part bytes_on_disk measured while source or destination parts are attached in the worker.",
		}, []string{"role", "job_id", "part_id"}),
		currentStage: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "partforge_rewrite_current_stage",
			Help: "Current rewrite stage for the active source part. The current stage label is set to 1.",
		}, []string{"job_id", "part_id", "stage"}),
		currentStageElapsed: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "partforge_rewrite_current_stage_elapsed_seconds",
			Help: "Elapsed wall clock time in the current rewrite stage.",
		}, []string{"job_id", "part_id", "stage"}),
		rewriteTotalElapsed: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "partforge_rewrite_total_elapsed_seconds",
			Help: "Total elapsed wall clock time for the active source part rewrite.",
		}, []string{"job_id", "part_id"}),
		completedStageSeconds: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "partforge_rewrite_completed_stage_duration_seconds",
			Help: "Completed rewrite stage wall clock duration.",
		}, []string{"job_id", "part_id", "stage"}),
		compactionsStarted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "partforge_compactions_started_total",
			Help: "Total compact-ready batch compaction attempts started.",
		}, []string{"job_id"}),
		compactionsCompleted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "partforge_compactions_completed_total",
			Help: "Total compact-ready batch compaction attempts completed with reduced output parts.",
		}, []string{"job_id"}),
		compactionsFailed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "partforge_compactions_failed_total",
			Help: "Total compact-ready batch compaction attempts that failed.",
		}, []string{"job_id"}),
		compactionsNoReduction: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "partforge_compactions_no_reduction_total",
			Help: "Total compact-ready batch compaction attempts that completed without reducing active part count.",
		}, []string{"job_id"}),
		compactionDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "partforge_compaction_duration_seconds",
			Help:    "Wall clock duration of compact-ready batch compaction attempts.",
			Buckets: prometheus.ExponentialBuckets(1, 2, 16),
		}, []string{"job_id", "result"}),
		compactInputArtifacts: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "partforge_compact_input_artifacts",
			Help: "Input artifacts attached for a compact-ready batch compaction attempt.",
		}, []string{"job_id", "output_part_id"}),
		compactPartCount: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "partforge_compact_part_count",
			Help: "Compact batch active ClickHouse part count measured while compaction is running.",
		}, []string{"role", "job_id", "output_part_id"}),
		compactPartRows: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "partforge_compact_part_rows",
			Help: "Compact batch active ClickHouse rows measured while compaction is running.",
		}, []string{"role", "job_id", "output_part_id"}),
		compactPartBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "partforge_compact_part_bytes",
			Help: "Compact batch active ClickHouse bytes_on_disk measured while compaction is running.",
		}, []string{"role", "job_id", "output_part_id"}),
		clickHouseMetricsUp: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "partforge_clickhouse_metrics_up",
			Help: "Whether the active local ClickHouse Prometheus endpoint was scraped successfully by this metrics endpoint.",
		}),
		clickHouseMetricsScrapes: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "partforge_clickhouse_metrics_scrapes_total",
			Help: "Total attempts by this metrics endpoint to scrape the active local ClickHouse Prometheus endpoint.",
		}),
		clickHouseMetricsScrapeFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "partforge_clickhouse_metrics_scrape_failures_total",
			Help: "Total failures scraping the active local ClickHouse Prometheus endpoint.",
		}),
		clickHouseMetricsScrapeDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "partforge_clickhouse_metrics_scrape_duration_seconds",
			Help:    "Wall clock duration of scrapes from this metrics endpoint to the active local ClickHouse Prometheus endpoint.",
			Buckets: prometheus.DefBuckets,
		}),
	}
	p.registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		p.forgesStarted,
		p.forgesCompleted,
		p.forgesFailed,
		p.forgeDuration,
		p.rowsReadTotal,
		p.bytesReadTotal,
		p.rowsWrittenTotal,
		p.bytesWrittenTotal,
		p.currentReadRows,
		p.currentReadBytes,
		p.currentWrittenRows,
		p.currentWrittenBytes,
		p.activePartCount,
		p.activePartRows,
		p.activePartBytes,
		p.currentStage,
		p.currentStageElapsed,
		p.rewriteTotalElapsed,
		p.completedStageSeconds,
		p.compactionsStarted,
		p.compactionsCompleted,
		p.compactionsFailed,
		p.compactionsNoReduction,
		p.compactionDuration,
		p.compactInputArtifacts,
		p.compactPartCount,
		p.compactPartRows,
		p.compactPartBytes,
		p.clickHouseMetricsUp,
		p.clickHouseMetricsScrapes,
		p.clickHouseMetricsScrapeFailures,
		p.clickHouseMetricsScrapeDuration,
	)
	return p
}

func (p *Prometheus) Handler() http.Handler {
	return http.HandlerFunc(p.serveHTTP)
}

func (p *Prometheus) SetClickHouseTarget(target string) {
	p.targetMu.Lock()
	defer p.targetMu.Unlock()
	p.clickHouseTarget = strings.TrimSpace(target)
}

func (p *Prometheus) ClearClickHouseTarget(target string) {
	p.targetMu.Lock()
	defer p.targetMu.Unlock()
	target = strings.TrimSpace(target)
	if target == "" || p.clickHouseTarget == target {
		p.clickHouseTarget = ""
	}
}

func (p *Prometheus) SetClickHouseScrapeTimeout(timeout time.Duration) error {
	if timeout <= 0 {
		return fmt.Errorf("clickhouse metrics scrape timeout must be greater than zero, got %s", timeout)
	}
	p.clickHouseScrapeTimeout = timeout
	return nil
}

func (p *Prometheus) serveHTTP(w http.ResponseWriter, r *http.Request) {
	clickHouseFamilies, active, err := p.scrapeClickHouse(r.Context())
	if err != nil {
		p.clickHouseMetricsUp.Set(0)
		p.clickHouseMetricsScrapeFailures.Inc()
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	if active {
		p.clickHouseMetricsUp.Set(1)
	} else {
		p.clickHouseMetricsUp.Set(0)
	}

	appFamilies, err := p.registry.Gather()
	if err != nil {
		http.Error(w, fmt.Sprintf("gather partforge metrics: %v", err), http.StatusInternalServerError)
		return
	}
	families, err := mergeMetricFamilies(appFamilies, clickHouseFamilies)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	body, err := encodeMetricFamilies(families)
	if err != nil {
		http.Error(w, fmt.Sprintf("encode metrics: %v", err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", string(expfmt.NewFormat(expfmt.TypeTextPlain)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

func (p *Prometheus) scrapeClickHouse(ctx context.Context) (map[string]*dto.MetricFamily, bool, error) {
	target := p.activeClickHouseTarget()
	if target == "" {
		return nil, false, nil
	}
	p.clickHouseMetricsScrapes.Inc()
	startedAt := time.Now()
	defer func() {
		p.clickHouseMetricsScrapeDuration.Observe(time.Since(startedAt).Seconds())
	}()

	ctx, cancel := context.WithTimeout(ctx, p.clickHouseScrapeTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, true, fmt.Errorf("build ClickHouse Prometheus scrape request: %w", err)
	}
	req.Header.Set("Accept", string(expfmt.NewFormat(expfmt.TypeTextPlain)))
	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, true, fmt.Errorf("scrape ClickHouse Prometheus metrics from %s: %w", target, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, true, fmt.Errorf("scrape ClickHouse Prometheus metrics from %s failed with status %d: %s", target, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	parser := expfmt.NewTextParser(model.LegacyValidation)
	families, err := parser.TextToMetricFamilies(resp.Body)
	if err != nil {
		return nil, true, fmt.Errorf("parse ClickHouse Prometheus metrics from %s: %w", target, err)
	}
	return families, true, nil
}

func (p *Prometheus) activeClickHouseTarget() string {
	p.targetMu.RLock()
	defer p.targetMu.RUnlock()
	return p.clickHouseTarget
}

func mergeMetricFamilies(appFamilies []*dto.MetricFamily, clickHouseFamilies map[string]*dto.MetricFamily) ([]*dto.MetricFamily, error) {
	merged := make(map[string]*dto.MetricFamily, len(appFamilies)+len(clickHouseFamilies))
	for _, family := range appFamilies {
		if family == nil || family.Name == nil || *family.Name == "" {
			return nil, fmt.Errorf("partforge metrics contained an unnamed metric family")
		}
		name := family.GetName()
		if _, exists := merged[name]; exists {
			return nil, fmt.Errorf("duplicate metric family %q in partforge metrics", name)
		}
		merged[name] = family
	}
	for name, family := range clickHouseFamilies {
		if family == nil || name == "" {
			return nil, fmt.Errorf("ClickHouse metrics contained an unnamed metric family")
		}
		if _, exists := merged[name]; exists {
			return nil, fmt.Errorf("duplicate metric family %q from ClickHouse metrics", name)
		}
		merged[name] = family
	}

	names := make([]string, 0, len(merged))
	for name := range merged {
		names = append(names, name)
	}
	sort.Strings(names)

	out := make([]*dto.MetricFamily, 0, len(names))
	for _, name := range names {
		out = append(out, merged[name])
	}
	return out, nil
}

func encodeMetricFamilies(families []*dto.MetricFamily) ([]byte, error) {
	var body bytes.Buffer
	encoder := expfmt.NewEncoder(&body, expfmt.NewFormat(expfmt.TypeTextPlain))
	for _, family := range families {
		if err := encoder.Encode(family); err != nil {
			return nil, err
		}
	}
	return body.Bytes(), nil
}

func (p *Prometheus) ForgeStarted(m manifest.Manifest) {
	p.forgesStarted.With(labels(m)).Inc()
	p.stageMu.Lock()
	p.forgeStartedAt[manifestKey(m)] = time.Now()
	p.stageMu.Unlock()
}

func (p *Prometheus) ForgeCompleted(m manifest.Manifest) {
	p.forgesCompleted.With(labels(m)).Inc()
	p.observeForgeDuration(m, "completed")
}

func (p *Prometheus) ForgeFailed(m manifest.Manifest) {
	p.forgesFailed.With(labels(m)).Inc()
	p.observeForgeDuration(m, "failed")
}

func (p *Prometheus) ObserveProgress(m manifest.Manifest, prev QueryProgress, current QueryProgress) {
	l := labels(m)
	p.rowsReadTotal.With(l).Add(delta(prev.ReadRows, current.ReadRows))
	p.bytesReadTotal.With(l).Add(delta(prev.ReadBytes, current.ReadBytes))
	p.rowsWrittenTotal.With(l).Add(delta(prev.WrittenRows, current.WrittenRows))
	p.bytesWrittenTotal.With(l).Add(delta(prev.WrittenBytes, current.WrittenBytes))

	currentLabels := prometheus.Labels{"job_id": m.JobID, "part_id": m.PartID}
	p.currentReadRows.With(currentLabels).Set(float64(current.ReadRows))
	p.currentReadBytes.With(currentLabels).Set(float64(current.ReadBytes))
	p.currentWrittenRows.With(currentLabels).Set(float64(current.WrittenRows))
	p.currentWrittenBytes.With(currentLabels).Set(float64(current.WrittenBytes))
}

func (p *Prometheus) ClearCurrentProgress(m manifest.Manifest) {
	currentLabels := prometheus.Labels{"job_id": m.JobID, "part_id": m.PartID}
	p.currentReadRows.With(currentLabels).Set(0)
	p.currentReadBytes.With(currentLabels).Set(0)
	p.currentWrittenRows.With(currentLabels).Set(0)
	p.currentWrittenBytes.With(currentLabels).Set(0)
}

func (p *Prometheus) SetActivePartStats(role string, m manifest.Manifest, stats PartStats) {
	l := prometheus.Labels{"role": role, "job_id": m.JobID, "part_id": m.PartID}
	p.activePartCount.With(l).Set(float64(stats.Count))
	p.activePartRows.With(l).Set(float64(stats.Rows))
	p.activePartBytes.With(l).Set(float64(stats.Bytes))
}

func (p *Prometheus) ObserveStageProgress(m manifest.Manifest, progress StageProgress) {
	stage := strings.TrimSpace(progress.Stage)
	if stage == "" {
		return
	}
	key := manifestKey(m)
	p.stageMu.Lock()
	if previous := p.currentStageByPart[key]; previous != "" && previous != stage {
		p.currentStage.DeleteLabelValues(m.JobID, m.PartID, previous)
		p.currentStageElapsed.DeleteLabelValues(m.JobID, m.PartID, previous)
	}
	p.currentStageByPart[key] = stage
	p.stageMu.Unlock()

	p.currentStage.WithLabelValues(m.JobID, m.PartID, stage).Set(1)
	p.currentStageElapsed.WithLabelValues(m.JobID, m.PartID, stage).Set(progress.StageElapsed.Seconds())
	p.rewriteTotalElapsed.WithLabelValues(m.JobID, m.PartID).Set(progress.TotalElapsed.Seconds())
	for completedStage, duration := range progress.CompletedStageDurations {
		if strings.TrimSpace(completedStage) == "" {
			continue
		}
		p.completedStageSeconds.WithLabelValues(m.JobID, m.PartID, completedStage).Set(duration.Seconds())
	}
}

func (p *Prometheus) ClearStageProgress(m manifest.Manifest) {
	key := manifestKey(m)
	p.stageMu.Lock()
	stage := p.currentStageByPart[key]
	delete(p.currentStageByPart, key)
	p.stageMu.Unlock()
	if stage != "" {
		p.currentStage.DeleteLabelValues(m.JobID, m.PartID, stage)
		p.currentStageElapsed.DeleteLabelValues(m.JobID, m.PartID, stage)
	}
	p.rewriteTotalElapsed.DeleteLabelValues(m.JobID, m.PartID)
}

func (p *Prometheus) CompactionStarted(jobID, outputPartID string, inputArtifacts uint64, inputStats PartStats) {
	p.compactionsStarted.WithLabelValues(jobID).Inc()
	p.compactInputArtifacts.WithLabelValues(jobID, outputPartID).Set(float64(inputArtifacts))
	p.SetCompactPartStats("input", jobID, outputPartID, inputStats)

	p.stageMu.Lock()
	p.compactionStartedAt[compactKey(jobID, outputPartID)] = time.Now()
	p.stageMu.Unlock()
}

func (p *Prometheus) CompactionCompleted(jobID, outputPartID string, inputStats, outputStats PartStats) {
	p.compactionsCompleted.WithLabelValues(jobID).Inc()
	p.SetCompactPartStats("input", jobID, outputPartID, inputStats)
	p.SetCompactPartStats("output", jobID, outputPartID, outputStats)
	p.observeCompactionDuration(jobID, outputPartID, "completed")
}

func (p *Prometheus) CompactionFailed(jobID, outputPartID string) {
	p.compactionsFailed.WithLabelValues(jobID).Inc()
	p.observeCompactionDuration(jobID, outputPartID, "failed")
}

func (p *Prometheus) CompactionNoReduction(jobID, outputPartID string, inputStats, outputStats PartStats) {
	p.compactionsNoReduction.WithLabelValues(jobID).Inc()
	p.SetCompactPartStats("input", jobID, outputPartID, inputStats)
	p.SetCompactPartStats("output", jobID, outputPartID, outputStats)
	p.observeCompactionDuration(jobID, outputPartID, "no_reduction")
}

func (p *Prometheus) SetCompactPartStats(role, jobID, outputPartID string, stats PartStats) {
	p.compactPartCount.WithLabelValues(role, jobID, outputPartID).Set(float64(stats.Count))
	p.compactPartRows.WithLabelValues(role, jobID, outputPartID).Set(float64(stats.Rows))
	p.compactPartBytes.WithLabelValues(role, jobID, outputPartID).Set(float64(stats.Bytes))
}

func (p *Prometheus) observeForgeDuration(m manifest.Manifest, result string) {
	var startedAt time.Time
	key := manifestKey(m)
	p.stageMu.Lock()
	startedAt = p.forgeStartedAt[key]
	delete(p.forgeStartedAt, key)
	p.stageMu.Unlock()
	if startedAt.IsZero() {
		return
	}
	p.forgeDuration.With(labelsWithResult(m, result)).Observe(time.Since(startedAt).Seconds())
}

func (p *Prometheus) observeCompactionDuration(jobID, outputPartID, result string) {
	key := compactKey(jobID, outputPartID)
	p.stageMu.Lock()
	startedAt := p.compactionStartedAt[key]
	delete(p.compactionStartedAt, key)
	p.stageMu.Unlock()
	if startedAt.IsZero() {
		return
	}
	p.compactionDuration.WithLabelValues(jobID, result).Observe(time.Since(startedAt).Seconds())
}

func StartServer(ctx context.Context, addr, path string, handler http.Handler) (*http.Server, error) {
	if path == "" {
		path = "/metrics"
	}
	mux := http.NewServeMux()
	mux.Handle(path, handler)
	server := &http.Server{Handler: mux}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	go func() {
		<-ctx.Done()
		if err := server.Shutdown(context.Background()); err != nil {
			slog.Warn("metrics server shutdown failed", "error", err)
		}
	}()
	go func() {
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			slog.Error("metrics server failed", "error", err)
		}
	}()
	slog.Info("metrics server listening", "addr", addr, "path", path)
	return server, nil
}

func labels(m manifest.Manifest) prometheus.Labels {
	return prometheus.Labels{
		"job_id":            m.JobID,
		"source_table":      m.Source.Database + "." + m.Source.Table,
		"destination_table": m.Dest.Database + "." + m.Dest.Table,
	}
}

func labelsWithResult(m manifest.Manifest, result string) prometheus.Labels {
	l := labels(m)
	l["result"] = result
	return l
}

func manifestKey(m manifest.Manifest) string {
	return m.JobID + "\xff" + m.PartID
}

func compactKey(jobID, outputPartID string) string {
	return jobID + "\xff" + outputPartID
}

func delta(prev, current uint64) float64 {
	if current <= prev {
		return 0
	}
	return float64(current - prev)
}
