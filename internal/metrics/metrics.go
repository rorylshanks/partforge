package metrics

import (
	"context"
	"log/slog"
	"net"
	"net/http"

	"github.com/partforge/partforge/internal/manifest"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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

type Recorder interface {
	ForgeStarted(manifest.Manifest)
	ForgeCompleted(manifest.Manifest)
	ForgeFailed(manifest.Manifest)
	ObserveProgress(manifest.Manifest, QueryProgress, QueryProgress)
	ClearCurrentProgress(manifest.Manifest)
	SetActivePartStats(string, manifest.Manifest, PartStats)
}

type Noop struct{}

func (Noop) ForgeStarted(manifest.Manifest)                                  {}
func (Noop) ForgeCompleted(manifest.Manifest)                                {}
func (Noop) ForgeFailed(manifest.Manifest)                                   {}
func (Noop) ObserveProgress(manifest.Manifest, QueryProgress, QueryProgress) {}
func (Noop) ClearCurrentProgress(manifest.Manifest)                          {}
func (Noop) SetActivePartStats(string, manifest.Manifest, PartStats)         {}

type Prometheus struct {
	registry *prometheus.Registry

	forgesStarted   *prometheus.CounterVec
	forgesCompleted *prometheus.CounterVec
	forgesFailed    *prometheus.CounterVec

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
}

func NewPrometheus() *Prometheus {
	p := &Prometheus{
		registry: prometheus.NewRegistry(),
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
	}
	p.registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		p.forgesStarted,
		p.forgesCompleted,
		p.forgesFailed,
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
	)
	return p
}

func (p *Prometheus) Handler() http.Handler {
	return promhttp.HandlerFor(p.registry, promhttp.HandlerOpts{})
}

func (p *Prometheus) ForgeStarted(m manifest.Manifest) {
	p.forgesStarted.With(labels(m)).Inc()
}

func (p *Prometheus) ForgeCompleted(m manifest.Manifest) {
	p.forgesCompleted.With(labels(m)).Inc()
}

func (p *Prometheus) ForgeFailed(m manifest.Manifest) {
	p.forgesFailed.With(labels(m)).Inc()
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

func delta(prev, current uint64) float64 {
	if current <= prev {
		return 0
	}
	return float64(current - prev)
}
