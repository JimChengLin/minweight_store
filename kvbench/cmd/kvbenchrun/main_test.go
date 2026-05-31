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
