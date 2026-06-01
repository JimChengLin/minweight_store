package main

import "testing"

func TestSummarizeDarwinIostat(t *testing.T) {
	raw := `              disk0
    KB/t  tps  MB/s
   27.61  121  3.25
  130.88 1250 159.83
`
	summary := summarizeIostat(raw)
	if summary.sampleCount != 1 {
		t.Fatalf("sample count = %d, want 1", summary.sampleCount)
	}
	if summary.mbpsAvg != 159.83 || summary.mbpsMax != 159.83 {
		t.Fatalf("mbps avg/max = %.2f/%.2f, want 159.83/159.83", summary.mbpsAvg, summary.mbpsMax)
	}
	if summary.readWrite {
		t.Fatal("darwin iostat should not report read/write split")
	}
}

func TestSummarizeLinuxIostat(t *testing.T) {
	raw := `Device             tps    MB_read/s    MB_wrtn/s    MB_dscd/s    MB_read    MB_wrtn    MB_dscd
vda               1.00         2.00         3.00         0.00          2          3          0
Device             tps    MB_read/s    MB_wrtn/s    MB_dscd/s    MB_read    MB_wrtn    MB_dscd
vda               1.00         4.00         5.00         0.00          4          5          0
`
	summary := summarizeIostat(raw)
	if summary.sampleCount != 1 {
		t.Fatalf("sample count = %d, want 1", summary.sampleCount)
	}
	if summary.mbpsAvg != 9 || summary.readMBpsAvg != 4 || summary.writeMBpsAvg != 5 {
		t.Fatalf("mbps avg/read/write = %.2f/%.2f/%.2f, want 9/4/5", summary.mbpsAvg, summary.readMBpsAvg, summary.writeMBpsAvg)
	}
	if !summary.readWrite {
		t.Fatal("linux iostat should report read/write split")
	}
}

func TestSummarizeLinuxIostatMultiDevice(t *testing.T) {
	raw := `Device             tps    MB_read/s    MB_wrtn/s    MB_dscd/s    MB_read    MB_wrtn    MB_dscd
vda               1.00       100.00       200.00         0.00        100        200          0
vdb               1.00       300.00       400.00         0.00        300        400          0
Device             tps    MB_read/s    MB_wrtn/s    MB_dscd/s    MB_read    MB_wrtn    MB_dscd
vda               1.00         1.00         2.00         0.00          1          2          0
vdb               1.00         3.00         4.00         0.00          3          4          0
Device             tps    MB_read/s    MB_wrtn/s    MB_dscd/s    MB_read    MB_wrtn    MB_dscd
vda               1.00         5.00         6.00         0.00          5          6          0
vdb               1.00         7.00         8.00         0.00          7          8          0
`
	summary := summarizeIostat(raw)
	if summary.sampleCount != 2 {
		t.Fatalf("sample count = %d, want 2", summary.sampleCount)
	}
	if summary.readMBpsAvg != 8 || summary.writeMBpsAvg != 10 {
		t.Fatalf("read/write avg = %.2f/%.2f, want 8/10", summary.readMBpsAvg, summary.writeMBpsAvg)
	}
	if summary.mbpsAvg != 18 || summary.mbpsMax != 26 {
		t.Fatalf("mbps avg/max = %.2f/%.2f, want 18/26", summary.mbpsAvg, summary.mbpsMax)
	}
}

func TestParseByteSize(t *testing.T) {
	tests := []struct {
		value string
		want  int64
	}{
		{value: "0", want: 0},
		{value: "1024", want: 1024},
		{value: "10GiB", want: 10 << 30},
		{value: "100GB", want: 100_000_000_000},
		{value: "1.5MiB", want: 1572864},
	}
	for _, test := range tests {
		got, err := parseByteSize(test.value)
		if err != nil {
			t.Fatalf("parseByteSize(%q): %v", test.value, err)
		}
		if got != test.want {
			t.Fatalf("parseByteSize(%q) = %d, want %d", test.value, got, test.want)
		}
	}
}

func TestParseLinuxAnonymousMemory(t *testing.T) {
	data := []byte(`Rss:                1024 kB
Shared_Clean:        512 kB
Anonymous:           123 kB
`)
	got, err := parseLinuxAnonymousMemory(data)
	if err != nil {
		t.Fatal(err)
	}
	if got != 123*1024 {
		t.Fatalf("anonymous bytes = %d, want %d", got, 123*1024)
	}
}
