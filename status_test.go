package warppool_test

import (
	"testing"

	"github.com/mzzsfy/warp-pool"
)

// Given 全部状态与越界值 When 转字符串 Then 返回对应状态名或 Unknown
func TestStatus_String_MapsStatesAndUnknown(t *testing.T) {
	cases := []struct {
		status warppool.Status
		want   string
	}{
		{warppool.StatusProbing, "Probing"},
		{warppool.StatusNormal, "Normal"},
		{warppool.StatusDraining, "Draining"},
		{warppool.StatusDisabled, "Disabled"},
		{warppool.Status(200), "Unknown"},
	}
	for _, c := range cases {
		if got := c.status.String(); got != c.want {
			t.Fatalf("Status(%d).String() = %q, 期望 %q", c.status, got, c.want)
		}
	}
}
