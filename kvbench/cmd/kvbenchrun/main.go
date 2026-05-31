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
	bench      string
	benchtime  string
	count      int
	outDir     string
	keepData   bool
	iostat     bool
	sampleRate time.Duration
}

type resourceSample struct {
	TimeUnixMillis       int64    `json:"time_unix_millis"`
	ElapsedMillis        int64    `json:"elapsed_millis"`
	DataBytes            int64    `json:"data_bytes"`
	DataBytesPerSecond   float64  `json:"data_bytes_per_second"`
	ProcessCPUPercent    *float64 `json:"process_cpu_percent,omitempty"`
	ProcessRSSBytes      *int64   `json:"process_rss_bytes,omitempty"`
	ProcessSampleError   string   `json:"process_sample_error,omitempty"`
	DirectorySampleError string   `json:"directory_sample_error,omitempty"`
}

type resourceSummary struct {
	Command             []string         `json:"command"`
	Goos                string           `json:"goos"`
	Goarch              string           `json:"goarch"`
	NumCPU              int              `json:"num_cpu"`
	StartedAt           time.Time        `json:"started_at"`
	FinishedAt          time.Time        `json:"finished_at"`
	WallSeconds         float64          `json:"wall_seconds"`
	UserCPUSeconds      float64          `json:"user_cpu_seconds"`
	SystemCPUSeconds    float64          `json:"system_cpu_seconds"`
	AverageCPUPercent   float64          `json:"average_cpu_percent"`
	MaxRSSBytes         int64            `json:"max_rss_bytes"`
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
	Samples             []resourceSample `json:"-"`
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
	flag.DurationVar(&cfg.sampleRate, "sample-rate", time.Second, "resource sample interval")
	flag.Parse()
	return cfg
}

func run(cfg runConfig) error {
	if cfg.sampleRate <= 0 {
		return fmt.Errorf("sample-rate must be positive")
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
	cmd := exec.Command(binaryPath, args...)
	cmd.Env = append(os.Environ(),
		"KVBENCH_DATA_DIR="+dataDir,
	)
	if cfg.keepData {
		cmd.Env = append(cmd.Env, "KVBENCH_KEEP_DATA=1")
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
	samplesCh := make(chan []resourceSample, 1)
	go func() {
		samplesCh <- sampleResources(cmd.Process.Pid, dataDir, cfg.sampleRate, stopSampling)
	}()
	err := cmd.Wait()
	close(stopSampling)
	samples := <-samplesCh
	iostatSummary := iostatSampler.stop()
	finishedAt := time.Now()

	if writeErr := os.WriteFile(benchOutputPath, output.Bytes(), 0o644); writeErr != nil {
		return writeErr
	}

	summary := makeSummary(cmd, args, startedAt, finishedAt, dataDir, cfg.keepData, benchOutputPath, samples, iostatSummary)
	if err != nil {
		summary.BenchmarkError = strings.TrimSpace(output.String())
		if summary.BenchmarkError == "" {
			summary.BenchmarkError = err.Error()
		}
	}
	if err := writeSamples(filepath.Join(cfg.outDir, "resource_samples.csv"), samples); err != nil {
		return err
	}
	if err := writeSummary(filepath.Join(cfg.outDir, "resource_summary.json"), summary); err != nil {
		return err
	}
	if err != nil {
		return err
	}
	return nil
}

func buildBenchmarkBinary(binaryPath string) error {
	cmd := exec.Command("go", "test", "-c", "-o", binaryPath, ".")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
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
	for _, line := range strings.Split(raw, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		if isIostatHeader(fields) {
			header = fields
			continue
		}
		sample, ok := parseIostatSample(header, fields)
		if ok {
			samples = append(samples, sample)
		}
	}
	if len(samples) > 1 {
		// The first iostat row is commonly since-boot state, not benchmark-window IO.
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

func sampleResources(pid int, dataDir string, interval time.Duration, stop <-chan struct{}) []resourceSample {
	start := time.Now()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var samples []resourceSample
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
			cpu, rss, err := sampleProcess(pid)
			if err != nil {
				sample.ProcessSampleError = err.Error()
				psAvailable = false
			} else {
				sample.ProcessCPUPercent = &cpu
				sample.ProcessRSSBytes = &rss
			}
		}
		samples = append(samples, sample)

		select {
		case <-stop:
			return samples
		case <-ticker.C:
		}
	}
}

func sampleProcess(pid int) (float64, int64, error) {
	cmd := exec.Command("ps", "-p", strconv.Itoa(pid), "-o", "%cpu=,rss=")
	output, err := cmd.Output()
	if err != nil {
		return 0, 0, err
	}
	fields := strings.Fields(string(output))
	if len(fields) < 2 {
		return 0, 0, fmt.Errorf("unexpected ps output %q", strings.TrimSpace(string(output)))
	}
	cpu, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return 0, 0, err
	}
	rssKB, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	return cpu, rssKB * 1024, nil
}

func directorySize(root string) (int64, error) {
	var total int64
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		total += info.Size()
		return nil
	})
	return total, err
}

func makeSummary(cmd *exec.Cmd, args []string, startedAt, finishedAt time.Time, dataDir string, keepData bool, benchOutputPath string, samples []resourceSample, iostat iostatSummary) resourceSummary {
	summary := resourceSummary{
		Command:            append([]string{cmd.Path}, args...),
		Goos:               runtime.GOOS,
		Goarch:             runtime.GOARCH,
		NumCPU:             runtime.NumCPU(),
		StartedAt:          startedAt,
		FinishedAt:         finishedAt,
		WallSeconds:        finishedAt.Sub(startedAt).Seconds(),
		SampleCount:        len(samples),
		ProcessSampler:     "ps",
		BenchmarkOutput:    benchOutputPath,
		DataDir:            dataDir,
		KeepData:           keepData,
		Samples:            samples,
		IostatEnabled:      iostat.enabled,
		IostatOutput:       iostat.outputPath,
		IostatError:        iostat.err,
		IostatSampleCount:  iostat.sampleCount,
		IostatMBpsAvg:      iostat.mbpsAvg,
		IostatMBpsMax:      iostat.mbpsMax,
		IostatReadWrite:    iostat.readWrite,
		IostatReadMBpsAvg:  iostat.readMBpsAvg,
		IostatReadMBpsMax:  iostat.readMBpsMax,
		IostatWriteMBpsAvg: iostat.writeMBpsAvg,
		IostatWriteMBpsMax: iostat.writeMBpsMax,
	}
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
		if err := writer.Write([]string{
			strconv.FormatInt(sample.TimeUnixMillis, 10),
			strconv.FormatInt(sample.ElapsedMillis, 10),
			strconv.FormatInt(sample.DataBytes, 10),
			strconv.FormatFloat(sample.DataBytesPerSecond, 'f', 2, 64),
			cpu,
			rss,
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
