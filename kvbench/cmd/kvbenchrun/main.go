package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type runConfig struct {
	bench              string
	benchtime          string
	count              int
	outDir             string
	keepData           bool
	iostat             bool
	gomaxprocs         int
	entries            int
	valueSize          int
	maxRSS             string
	maxData            string
	sampleRate         time.Duration
	reuseLargeLoadData string

	minweightWALSize          string
	minweightMaxImmutableWALs int
	minweightTargetSSTSize    string
}

type resourceSample struct {
	TimeUnixMillis        int64    `json:"time_unix_millis"`
	ElapsedMillis         int64    `json:"elapsed_millis"`
	DataBytes             int64    `json:"data_bytes"`
	DataBytesPerSecond    float64  `json:"data_bytes_per_second"`
	ProcessCPUPercent     *float64 `json:"process_cpu_percent,omitempty"`
	ProcessRSSBytes       *int64   `json:"process_rss_bytes,omitempty"`
	ProcessAnonymousBytes *int64   `json:"process_anonymous_bytes,omitempty"`
	ProcessSampleError    string   `json:"process_sample_error,omitempty"`
	DirectorySampleError  string   `json:"directory_sample_error,omitempty"`
}

type resourceResult struct {
	samples                 []resourceSample
	limitExceeded           string
	limitExceededValueBytes int64
}

type resourceSummary struct {
	Command             []string         `json:"command"`
	Goos                string           `json:"goos"`
	Goarch              string           `json:"goarch"`
	NumCPU              int              `json:"num_cpu"`
	GOMAXPROCS          int              `json:"gomaxprocs,omitempty"`
	LargeEntries        int              `json:"large_entries,omitempty"`
	LargeValueSize      int              `json:"large_value_size,omitempty"`
	MinweightWALSize    int64            `json:"minweight_wal_size,omitempty"`
	MinweightMaxImmWALs int              `json:"minweight_max_immutable_wals,omitempty"`
	MinweightTargetSST  int64            `json:"minweight_target_sst_size,omitempty"`
	StartedAt           time.Time        `json:"started_at"`
	FinishedAt          time.Time        `json:"finished_at"`
	WallSeconds         float64          `json:"wall_seconds"`
	UserCPUSeconds      float64          `json:"user_cpu_seconds"`
	SystemCPUSeconds    float64          `json:"system_cpu_seconds"`
	AverageCPUPercent   float64          `json:"average_cpu_percent"`
	MaxRSSBytes         int64            `json:"max_rss_bytes"`
	PeakSampleRSSBytes  int64            `json:"peak_sample_rss_bytes,omitempty"`
	PeakAnonymousBytes  int64            `json:"peak_anonymous_bytes,omitempty"`
	MemoryLimitBytes    int64            `json:"memory_limit_bytes,omitempty"`
	DataLimitBytes      int64            `json:"data_limit_bytes,omitempty"`
	LimitExceeded       string           `json:"limit_exceeded,omitempty"`
	LimitExceededBytes  int64            `json:"limit_exceeded_value_bytes,omitempty"`
	BlockInputOps       int64            `json:"block_input_ops"`
	BlockOutputOps      int64            `json:"block_output_ops"`
	IostatEnabled       bool             `json:"iostat_enabled"`
	IostatOutput        string           `json:"iostat_output,omitempty"`
	IostatError         string           `json:"iostat_error,omitempty"`
	IostatSampleCount   int              `json:"iostat_sample_count"`
	IostatMBpsAvg       float64          `json:"iostat_mbps_avg"`
	IostatMBpsMax       float64          `json:"iostat_mbps_max"`
	IostatReadWrite     bool             `json:"iostat_read_write_available"`
	IostatReadMBpsAvg   float64          `json:"iostat_read_mbps_avg"`
	IostatReadMBpsMax   float64          `json:"iostat_read_mbps_max"`
	IostatWriteMBpsAvg  float64          `json:"iostat_write_mbps_avg"`
	IostatWriteMBpsMax  float64          `json:"iostat_write_mbps_max"`
	PeakDataBytes       int64            `json:"peak_data_bytes"`
	FinalDataBytes      int64            `json:"final_data_bytes"`
	MaxDataBytesPerSec  float64          `json:"max_data_bytes_per_second"`
	SampleCount         int              `json:"sample_count"`
	ProcessSampler      string           `json:"process_sampler"`
	ProcessSamplerError string           `json:"process_sampler_error,omitempty"`
	BenchmarkError      string           `json:"benchmark_error,omitempty"`
	BenchmarkOutput     string           `json:"benchmark_output"`
	DataDir             string           `json:"data_dir"`
	KeepData            bool             `json:"keep_data"`
	ReuseLargeLoadData  string           `json:"reuse_large_load_data,omitempty"`
	Samples             []resourceSample `json:"-"`
}

type resourceLimits struct {
	memoryBytes int64
	dataBytes   int64
}

type iostatRun struct {
	enabled bool
	path    string
	cmd     *exec.Cmd
	output  bytes.Buffer
	err     error
}

type iostatSummary struct {
	enabled      bool
	outputPath   string
	err          string
	sampleCount  int
	mbpsAvg      float64
	mbpsMax      float64
	readWrite    bool
	readMBpsAvg  float64
	readMBpsMax  float64
	writeMBpsAvg float64
	writeMBpsMax float64
}

type iostatSample struct {
	mbps      float64
	readWrite bool
	readMBps  float64
	writeMBps float64
}

func (s iostatSample) add(other iostatSample) iostatSample {
	return iostatSample{
		mbps:      s.mbps + other.mbps,
		readWrite: s.readWrite || other.readWrite,
		readMBps:  s.readMBps + other.readMBps,
		writeMBps: s.writeMBps + other.writeMBps,
	}
}

func main() {
	cfg := parseFlags()
	if err := run(cfg); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func parseFlags() runConfig {
	var cfg runConfig
	flag.StringVar(&cfg.bench, "bench", ".", "benchmark regexp")
	flag.StringVar(&cfg.benchtime, "benchtime", "3s", "benchmark benchtime")
	flag.IntVar(&cfg.count, "count", 3, "benchmark count")
	flag.StringVar(&cfg.outDir, "out", filepath.Join("results", time.Now().Format("20060102-150405")), "output directory")
	flag.BoolVar(&cfg.keepData, "keep-data", false, "keep benchmark data directories after the run")
	flag.BoolVar(&cfg.iostat, "iostat", true, "record device-level iostat samples")
	flag.IntVar(&cfg.gomaxprocs, "gomaxprocs", 0, "set child GOMAXPROCS and -test.cpu")
	flag.IntVar(&cfg.entries, "entries", 0, "set KVBENCH_LARGE_ENTRIES for large benchmarks")
	flag.IntVar(&cfg.valueSize, "value-size", 0, "set KVBENCH_LARGE_VALUE_SIZE for large benchmarks")
	flag.StringVar(&cfg.maxRSS, "max-rss", "0", "soft child anonymous-memory limit when supported; RSS is report-only")
	flag.StringVar(&cfg.maxData, "max-data", "0", "soft benchmark data directory limit, for example 100GiB")
	flag.DurationVar(&cfg.sampleRate, "sample-rate", time.Second, "resource sample interval")
	flag.StringVar(&cfg.reuseLargeLoadData, "reuse-large-load-data", "", "reuse data/ from a kept BenchmarkLargeLoad run for BenchmarkLargeGet or BenchmarkLargeScan")
	flag.StringVar(&cfg.minweightWALSize, "minweight-wal-size", "0", "set minweight WALSize, for example 256MiB")
	flag.IntVar(&cfg.minweightMaxImmutableWALs, "minweight-max-immutable-wals", 0, "set minweight MaxImmutableWALNum")
	flag.StringVar(&cfg.minweightTargetSSTSize, "minweight-target-sst-size", "0", "set minweight TargetSSTSize, for example 512MiB")
	flag.Parse()
	return cfg
}

func run(cfg runConfig) error {
	if cfg.sampleRate <= 0 {
		return fmt.Errorf("sample-rate must be positive")
	}
	memoryLimit, err := parseByteSize(cfg.maxRSS)
	if err != nil {
		return fmt.Errorf("max-rss: %w", err)
	}
	dataLimit, err := parseByteSize(cfg.maxData)
	if err != nil {
		return fmt.Errorf("max-data: %w", err)
	}
	minweightWALSize, err := parseByteSize(cfg.minweightWALSize)
	if err != nil {
		return fmt.Errorf("minweight-wal-size: %w", err)
	}
	minweightTargetSSTSize, err := parseByteSize(cfg.minweightTargetSSTSize)
	if err != nil {
		return fmt.Errorf("minweight-target-sst-size: %w", err)
	}
	if err := os.MkdirAll(cfg.outDir, 0o755); err != nil {
		return err
	}
	dataDir := filepath.Join(cfg.outDir, "data")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return err
	}
	if !cfg.keepData {
		defer func() {
			_ = os.RemoveAll(dataDir)
		}()
	}
	sampleDataDir := dataDir
	if cfg.reuseLargeLoadData != "" {
		sampleDataDir = cfg.reuseLargeLoadData
	}

	binaryPath := filepath.Join(cfg.outDir, "kvbench.test")
	if err := buildBenchmarkBinary(binaryPath); err != nil {
		return err
	}

	benchOutputPath := filepath.Join(cfg.outDir, "bench.txt")
	args := []string{
		"-test.run=^$",
		"-test.bench=" + cfg.bench,
		"-test.benchmem",
		"-test.benchtime=" + cfg.benchtime,
		"-test.count=" + strconv.Itoa(cfg.count),
	}
	if cfg.gomaxprocs > 0 {
		args = append(args, "-test.cpu="+strconv.Itoa(cfg.gomaxprocs))
	}
	cmd := exec.Command(binaryPath, args...)
	cmd.Env = append(os.Environ(),
		"KVBENCH_DATA_DIR="+dataDir,
		// The runner owns cleanup so resource_summary final_data_bytes can
		// describe the completed benchmark directory, not a mid-run deletion.
		"KVBENCH_KEEP_DATA=1",
	)
	if cfg.gomaxprocs > 0 {
		cmd.Env = append(cmd.Env, "GOMAXPROCS="+strconv.Itoa(cfg.gomaxprocs))
	}
	if cfg.entries > 0 {
		cmd.Env = append(cmd.Env, "KVBENCH_LARGE_ENTRIES="+strconv.Itoa(cfg.entries))
	}
	if cfg.valueSize > 0 {
		cmd.Env = append(cmd.Env, "KVBENCH_LARGE_VALUE_SIZE="+strconv.Itoa(cfg.valueSize))
	}
	if minweightWALSize > 0 {
		cmd.Env = append(cmd.Env, "KVBENCH_MINWEIGHT_WAL_SIZE="+strconv.FormatInt(minweightWALSize, 10))
	}
	if cfg.minweightMaxImmutableWALs > 0 {
		cmd.Env = append(cmd.Env, "KVBENCH_MINWEIGHT_MAX_IMMUTABLE_WALS="+strconv.Itoa(cfg.minweightMaxImmutableWALs))
	}
	if minweightTargetSSTSize > 0 {
		cmd.Env = append(cmd.Env, "KVBENCH_MINWEIGHT_TARGET_SST_SIZE="+strconv.FormatInt(minweightTargetSSTSize, 10))
	}
	if cfg.reuseLargeLoadData != "" {
		cmd.Env = append(cmd.Env, "KVBENCH_REUSE_LARGE_LOAD_DATA_DIR="+cfg.reuseLargeLoadData)
	}
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output

	startedAt := time.Now()
	if err := cmd.Start(); err != nil {
		return err
	}
	iostatSampler := startIostat(filepath.Join(cfg.outDir, "iostat.txt"), cfg.sampleRate, cfg.iostat)
	stopSampling := make(chan struct{})
	resourcesCh := make(chan resourceResult, 1)
	go func() {
		resourcesCh <- sampleResources(cmd.Process.Pid, sampleDataDir, cfg.sampleRate, resourceLimits{
			memoryBytes: memoryLimit,
			dataBytes:   dataLimit,
		}, stopSampling)
	}()
	waitErr := cmd.Wait()
	finishedAt := time.Now()
	close(stopSampling)
	resources := <-resourcesCh
	iostatSummary := iostatSampler.stop()
	finalSample := resourceSample{
		TimeUnixMillis: finishedAt.UnixMilli(),
		ElapsedMillis:  finishedAt.Sub(startedAt).Milliseconds(),
	}
	bytes, err := directorySize(sampleDataDir)
	if err != nil {
		finalSample.DirectorySampleError = err.Error()
	} else {
		finalSample.DataBytes = bytes
		for i := len(resources.samples) - 1; i >= 0; i-- {
			previous := resources.samples[i]
			if previous.DirectorySampleError != "" {
				continue
			}
			elapsedMillis := finalSample.ElapsedMillis - previous.ElapsedMillis
			if elapsedMillis > 0 {
				finalSample.DataBytesPerSecond = float64(bytes-previous.DataBytes) / (float64(elapsedMillis) / 1000)
			}
			break
		}
	}
	resources.samples = append(resources.samples, finalSample)
	if dataLimit > 0 && finalSample.DirectorySampleError == "" && finalSample.DataBytes > dataLimit && resources.limitExceeded == "" {
		resources.limitExceeded = "data"
		resources.limitExceededValueBytes = finalSample.DataBytes
	}

	if writeErr := os.WriteFile(benchOutputPath, output.Bytes(), 0o644); writeErr != nil {
		return writeErr
	}

	summary := makeSummary(cmd, args, startedAt, finishedAt, sampleDataDir, cfg, memoryLimit, dataLimit, minweightWALSize, minweightTargetSSTSize, benchOutputPath, resources, iostatSummary)
	if waitErr != nil {
		summary.BenchmarkError = strings.TrimSpace(output.String())
		if summary.BenchmarkError == "" {
			summary.BenchmarkError = waitErr.Error()
		}
	}
	if err := writeSamples(filepath.Join(cfg.outDir, "resource_samples.csv"), resources.samples); err != nil {
		return err
	}
	if err := writeSummary(filepath.Join(cfg.outDir, "resource_summary.json"), summary); err != nil {
		return err
	}
	if waitErr != nil {
		return waitErr
	}
	return nil
}

func buildBenchmarkBinary(binaryPath string) error {
	cmd := exec.Command("go", "test", "-c", "-o", binaryPath, ".")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func parseByteSize(value string) (int64, error) {
	value = strings.TrimSpace(value)
	if value == "" || value == "0" {
		return 0, nil
	}
	units := []struct {
		suffix string
		scale  int64
	}{
		{"tib", 1 << 40},
		{"gib", 1 << 30},
		{"mib", 1 << 20},
		{"kib", 1 << 10},
		{"tb", 1_000_000_000_000},
		{"gb", 1_000_000_000},
		{"mb", 1_000_000},
		{"kb", 1_000},
		{"b", 1},
	}
	lower := strings.ToLower(value)
	scale := int64(1)
	number := value
	for _, unit := range units {
		if strings.HasSuffix(lower, unit.suffix) {
			scale = unit.scale
			number = strings.TrimSpace(value[:len(value)-len(unit.suffix)])
			break
		}
	}
	parsed, err := strconv.ParseFloat(number, 64)
	if err != nil {
		return 0, err
	}
	if parsed < 0 {
		return 0, fmt.Errorf("negative byte size %q", value)
	}
	return int64(parsed * float64(scale)), nil
}

func startIostat(path string, interval time.Duration, enabled bool) *iostatRun {
	run := &iostatRun{
		enabled: enabled,
		path:    path,
	}
	if !enabled {
		return run
	}
	args := iostatArgs(interval)
	run.cmd = exec.Command("iostat", args...)
	run.cmd.Stdout = &run.output
	run.cmd.Stderr = &run.output
	run.err = run.cmd.Start()
	return run
}

func iostatArgs(interval time.Duration) []string {
	seconds := int((interval + time.Second - 1) / time.Second)
	if seconds < 1 {
		seconds = 1
	}
	if runtime.GOOS == "darwin" {
		return []string{"-d", "-w", strconv.Itoa(seconds)}
	}
	return []string{"-d", "-m", strconv.Itoa(seconds)}
}

func (r *iostatRun) stop() iostatSummary {
	summary := iostatSummary{
		enabled:    r.enabled,
		outputPath: r.path,
	}
	if !r.enabled {
		return summary
	}
	if r.err != nil {
		summary.err = r.err.Error()
		_ = os.WriteFile(r.path, []byte(summary.err+"\n"), 0o644)
		return summary
	}
	_ = r.cmd.Process.Kill()
	waitErr := r.cmd.Wait()
	raw := r.output.String()
	if err := os.WriteFile(r.path, []byte(raw), 0o644); err != nil && waitErr == nil {
		waitErr = err
	}

	summary = summarizeIostat(raw)
	summary.enabled = true
	summary.outputPath = r.path
	if summary.sampleCount == 0 && waitErr != nil {
		summary.err = strings.TrimSpace(raw)
		if summary.err == "" {
			summary.err = waitErr.Error()
		}
	}
	return summary
}

func summarizeIostat(raw string) iostatSummary {
	var samples []iostatSample
	var header []string
	var bucket iostatSample
	bucketHasSample := false
	flushBucket := func() {
		if bucketHasSample {
			samples = append(samples, bucket)
			bucket = iostatSample{}
			bucketHasSample = false
		}
	}
	for _, line := range strings.Split(raw, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		if isIostatHeader(fields) {
			flushBucket()
			header = fields
			continue
		}
		sample, ok := parseIostatSample(header, fields)
		if ok {
			if isDarwinIostatHeader(header) {
				samples = append(samples, sample)
			} else {
				bucket = bucket.add(sample)
				bucketHasSample = true
			}
		}
	}
	flushBucket()
	if len(samples) > 1 {
		// The first iostat bucket is commonly since-boot state, not benchmark-window IO.
		samples = samples[1:]
	}
	return summarizeIostatSamples(samples)
}

func isIostatHeader(fields []string) bool {
	for _, field := range fields {
		switch field {
		case "MB/s", "MB_read/s", "MB_wrtn/s":
			return true
		}
	}
	return false
}

func parseIostatSample(header, fields []string) (iostatSample, bool) {
	if len(header) == 0 {
		return iostatSample{}, false
	}
	if isDarwinIostatHeader(header) {
		return parseDarwinIostatSample(header, fields)
	}
	return parseLinuxIostatSample(header, fields)
}

func isDarwinIostatHeader(header []string) bool {
	hasKBT := false
	hasTPS := false
	hasMBPS := false
	for _, field := range header {
		switch field {
		case "KB/t":
			hasKBT = true
		case "tps":
			hasTPS = true
		case "MB/s":
			hasMBPS = true
		}
	}
	return hasKBT && hasTPS && hasMBPS
}

func parseDarwinIostatSample(header, fields []string) (iostatSample, bool) {
	deviceCount := 0
	for _, field := range header {
		if field == "MB/s" {
			deviceCount++
		}
	}
	if deviceCount == 0 || len(fields) < deviceCount*3 {
		return iostatSample{}, false
	}
	var total float64
	for i := 0; i < deviceCount; i++ {
		mbps, err := strconv.ParseFloat(fields[i*3+2], 64)
		if err != nil {
			return iostatSample{}, false
		}
		total += mbps
	}
	return iostatSample{mbps: total}, true
}

func parseLinuxIostatSample(header, fields []string) (iostatSample, bool) {
	if len(fields) < len(header) {
		return iostatSample{}, false
	}
	readIndex := indexOf(header, "MB_read/s")
	writeIndex := indexOf(header, "MB_wrtn/s")
	if readIndex < 0 || writeIndex < 0 {
		return iostatSample{}, false
	}
	readMBps, err := strconv.ParseFloat(fields[readIndex], 64)
	if err != nil {
		return iostatSample{}, false
	}
	writeMBps, err := strconv.ParseFloat(fields[writeIndex], 64)
	if err != nil {
		return iostatSample{}, false
	}
	return iostatSample{
		mbps:      readMBps + writeMBps,
		readWrite: true,
		readMBps:  readMBps,
		writeMBps: writeMBps,
	}, true
}

func indexOf(fields []string, target string) int {
	for i, field := range fields {
		if field == target {
			return i
		}
	}
	return -1
}

func summarizeIostatSamples(samples []iostatSample) iostatSummary {
	summary := iostatSummary{
		sampleCount: len(samples),
	}
	if len(samples) == 0 {
		return summary
	}
	for _, sample := range samples {
		summary.mbpsAvg += sample.mbps
		summary.readMBpsAvg += sample.readMBps
		summary.writeMBpsAvg += sample.writeMBps
		if sample.readWrite {
			summary.readWrite = true
		}
		if sample.mbps > summary.mbpsMax {
			summary.mbpsMax = sample.mbps
		}
		if sample.readMBps > summary.readMBpsMax {
			summary.readMBpsMax = sample.readMBps
		}
		if sample.writeMBps > summary.writeMBpsMax {
			summary.writeMBpsMax = sample.writeMBps
		}
	}
	n := float64(len(samples))
	summary.mbpsAvg /= n
	summary.readMBpsAvg /= n
	summary.writeMBpsAvg /= n
	return summary
}

func sampleResources(pid int, dataDir string, interval time.Duration, limits resourceLimits, stop <-chan struct{}) resourceResult {
	start := time.Now()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	result := resourceResult{}
	var lastBytes int64
	var lastTime time.Time
	psAvailable := true
	for {
		now := time.Now()
		sample := resourceSample{
			TimeUnixMillis: now.UnixMilli(),
			ElapsedMillis:  now.Sub(start).Milliseconds(),
		}
		bytes, err := directorySize(dataDir)
		if err != nil {
			sample.DirectorySampleError = err.Error()
		} else {
			sample.DataBytes = bytes
			if !lastTime.IsZero() {
				elapsed := now.Sub(lastTime).Seconds()
				if elapsed > 0 {
					sample.DataBytesPerSecond = float64(bytes-lastBytes) / elapsed
				}
			}
			lastBytes = bytes
			lastTime = now
		}
		if psAvailable {
			process, err := sampleProcess(pid)
			if err != nil {
				sample.ProcessSampleError = err.Error()
				psAvailable = false
			} else {
				sample.ProcessCPUPercent = &process.cpuPercent
				sample.ProcessRSSBytes = &process.rssBytes
				sample.ProcessAnonymousBytes = process.anonymousBytes
			}
		}
		result.samples = append(result.samples, sample)
		if limits.dataBytes > 0 && sample.DirectorySampleError == "" && sample.DataBytes > limits.dataBytes {
			result.limitExceeded = "data"
			result.limitExceededValueBytes = sample.DataBytes
			killProcess(pid)
			return result
		}
		if limits.memoryBytes > 0 && sample.ProcessAnonymousBytes != nil && *sample.ProcessAnonymousBytes > limits.memoryBytes {
			result.limitExceeded = "memory"
			result.limitExceededValueBytes = *sample.ProcessAnonymousBytes
			killProcess(pid)
			return result
		}

		select {
		case <-stop:
			return result
		case <-ticker.C:
		}
	}
}

func killProcess(pid int) {
	process, err := os.FindProcess(pid)
	if err == nil {
		_ = process.Kill()
	}
}

type processSample struct {
	cpuPercent     float64
	rssBytes       int64
	anonymousBytes *int64
}

func sampleProcess(pid int) (processSample, error) {
	cmd := exec.Command("ps", "-p", strconv.Itoa(pid), "-o", "%cpu=,rss=")
	output, err := cmd.Output()
	if err != nil {
		return processSample{}, err
	}
	fields := strings.Fields(string(output))
	if len(fields) < 2 {
		return processSample{}, fmt.Errorf("unexpected ps output %q", strings.TrimSpace(string(output)))
	}
	cpu, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return processSample{}, err
	}
	rssKB, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return processSample{}, err
	}
	sample := processSample{
		cpuPercent: cpu,
		rssBytes:   rssKB * 1024,
	}
	if runtime.GOOS == "linux" {
		anonymousBytes, err := sampleLinuxAnonymousMemory(pid)
		if err != nil {
			return processSample{}, err
		}
		sample.anonymousBytes = &anonymousBytes
	}
	return sample, nil
}

func sampleLinuxAnonymousMemory(pid int) (int64, error) {
	data, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "smaps_rollup"))
	if err != nil {
		return 0, err
	}
	return parseLinuxAnonymousMemory(data)
}

func parseLinuxAnonymousMemory(data []byte) (int64, error) {
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 2 && fields[0] == "Anonymous:" {
			kb, err := strconv.ParseInt(fields[1], 10, 64)
			if err != nil {
				return 0, err
			}
			return kb * 1024, nil
		}
	}
	return 0, fmt.Errorf("Anonymous not found in smaps_rollup")
}

func directorySize(root string) (int64, error) {
	var total int64
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		total += info.Size()
		return nil
	})
	return total, err
}

func makeSummary(cmd *exec.Cmd, args []string, startedAt, finishedAt time.Time, dataDir string, cfg runConfig, memoryLimit, dataLimit, minweightWALSize, minweightTargetSSTSize int64, benchOutputPath string, resources resourceResult, iostat iostatSummary) resourceSummary {
	summary := resourceSummary{
		Command:             append([]string{cmd.Path}, args...),
		Goos:                runtime.GOOS,
		Goarch:              runtime.GOARCH,
		NumCPU:              runtime.NumCPU(),
		GOMAXPROCS:          cfg.gomaxprocs,
		LargeEntries:        cfg.entries,
		LargeValueSize:      cfg.valueSize,
		MinweightWALSize:    minweightWALSize,
		MinweightMaxImmWALs: cfg.minweightMaxImmutableWALs,
		MinweightTargetSST:  minweightTargetSSTSize,
		StartedAt:           startedAt,
		FinishedAt:          finishedAt,
		WallSeconds:         finishedAt.Sub(startedAt).Seconds(),
		SampleCount:         len(resources.samples),
		ProcessSampler:      "ps",
		BenchmarkOutput:     benchOutputPath,
		DataDir:             dataDir,
		KeepData:            cfg.keepData,
		ReuseLargeLoadData:  cfg.reuseLargeLoadData,
		Samples:             resources.samples,
		MemoryLimitBytes:    memoryLimit,
		DataLimitBytes:      dataLimit,
		LimitExceeded:       resources.limitExceeded,
		LimitExceededBytes:  resources.limitExceededValueBytes,
		IostatEnabled:       iostat.enabled,
		IostatOutput:        iostat.outputPath,
		IostatError:         iostat.err,
		IostatSampleCount:   iostat.sampleCount,
		IostatMBpsAvg:       iostat.mbpsAvg,
		IostatMBpsMax:       iostat.mbpsMax,
		IostatReadWrite:     iostat.readWrite,
		IostatReadMBpsAvg:   iostat.readMBpsAvg,
		IostatReadMBpsMax:   iostat.readMBpsMax,
		IostatWriteMBpsAvg:  iostat.writeMBpsAvg,
		IostatWriteMBpsMax:  iostat.writeMBpsMax,
	}
	samples := resources.samples
	if len(samples) != 0 {
		summary.PeakDataBytes = samples[0].DataBytes
		summary.FinalDataBytes = samples[len(samples)-1].DataBytes
		for _, sample := range samples {
			if sample.DataBytes > summary.PeakDataBytes {
				summary.PeakDataBytes = sample.DataBytes
			}
			if sample.DataBytesPerSecond > summary.MaxDataBytesPerSec {
				summary.MaxDataBytesPerSec = sample.DataBytesPerSecond
			}
			if sample.ProcessRSSBytes != nil && *sample.ProcessRSSBytes > summary.PeakSampleRSSBytes {
				summary.PeakSampleRSSBytes = *sample.ProcessRSSBytes
			}
			if sample.ProcessAnonymousBytes != nil && *sample.ProcessAnonymousBytes > summary.PeakAnonymousBytes {
				summary.PeakAnonymousBytes = *sample.ProcessAnonymousBytes
			}
			if summary.ProcessSamplerError == "" && sample.ProcessSampleError != "" {
				summary.ProcessSamplerError = sample.ProcessSampleError
			}
		}
	}
	if cmd.ProcessState != nil {
		summary.UserCPUSeconds = cmd.ProcessState.UserTime().Seconds()
		summary.SystemCPUSeconds = cmd.ProcessState.SystemTime().Seconds()
		if summary.WallSeconds > 0 {
			summary.AverageCPUPercent = 100 * (summary.UserCPUSeconds + summary.SystemCPUSeconds) / summary.WallSeconds
		}
		if usage, ok := cmd.ProcessState.SysUsage().(*syscall.Rusage); ok {
			summary.MaxRSSBytes = normalizeMaxRSS(usage.Maxrss)
			summary.BlockInputOps = usage.Inblock
			summary.BlockOutputOps = usage.Oublock
		}
	}
	return summary
}

func normalizeMaxRSS(maxRSS int64) int64 {
	if runtime.GOOS == "linux" {
		return maxRSS * 1024
	}
	return maxRSS
}

func writeSamples(path string, samples []resourceSample) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() {
		_ = file.Close()
	}()
	writer := csv.NewWriter(file)
	if err := writer.Write([]string{
		"time_unix_millis",
		"elapsed_millis",
		"data_bytes",
		"data_bytes_per_second",
		"process_cpu_percent",
		"process_rss_bytes",
		"process_anonymous_bytes",
		"process_sample_error",
		"directory_sample_error",
	}); err != nil {
		return err
	}
	for _, sample := range samples {
		cpu := ""
		if sample.ProcessCPUPercent != nil {
			cpu = strconv.FormatFloat(*sample.ProcessCPUPercent, 'f', 2, 64)
		}
		rss := ""
		if sample.ProcessRSSBytes != nil {
			rss = strconv.FormatInt(*sample.ProcessRSSBytes, 10)
		}
		anonymous := ""
		if sample.ProcessAnonymousBytes != nil {
			anonymous = strconv.FormatInt(*sample.ProcessAnonymousBytes, 10)
		}
		if err := writer.Write([]string{
			strconv.FormatInt(sample.TimeUnixMillis, 10),
			strconv.FormatInt(sample.ElapsedMillis, 10),
			strconv.FormatInt(sample.DataBytes, 10),
			strconv.FormatFloat(sample.DataBytesPerSecond, 'f', 2, 64),
			cpu,
			rss,
			anonymous,
			sample.ProcessSampleError,
			sample.DirectorySampleError,
		}); err != nil {
			return err
		}
	}
	writer.Flush()
	return writer.Error()
}

func writeSummary(path string, summary resourceSummary) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() {
		_ = file.Close()
	}()
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(summary)
}
