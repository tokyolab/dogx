package logic

import (
	"math"
	"strings"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/job/rpc/types/job"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func pointer[T any](value T) *T { return &value }
func testNow() time.Time {
	return time.Date(2026, 10, 16, 12, 0, 0, 123456000, time.FixedZone("CST", 8*60*60))
}
func validInput() *job.JobInput {
	return &job.JobInput{Name: " job ", Remark: " note ", ExecutorGroupId: 3, ProcessorName: " Handler ", Params: "  raw\n value  ", CronExpr: " 0 * * * * * ", TimeoutSeconds: 900}
}
func completeInput() *job.JobInput {
	in := validInput()
	in.ExecutionMode, in.PostEnabled = pointer(int32(1)), pointer(false)
	in.MisfirePolicy, in.BlockPolicy = pointer(int32(0)), pointer(int32(0))
	in.MaxRetries, in.RetryIntervalSeconds = pointer(int32(0)), pointer(int32(10))
	return in
}

func TestNormalizeInputDefaultsAndExplicitZero(t *testing.T) {
	definition, err := normalizeInput(validInput(), true, testNow())
	if err != nil {
		t.Fatal(err)
	}
	if definition.Name != "job" || definition.Remark != "note" || definition.ProcessorName != "Handler" || definition.Params != "raw\n value" || definition.CronExpr != "0 * * * * *" {
		t.Fatalf("normalization changed internal text: %+v", definition)
	}
	if definition.ExecutionMode != 1 || definition.PostEnabled || definition.Status != 0 || definition.MaxRetries != 3 || definition.RetryIntervalSeconds != 10 || definition.MisfirePolicy != 0 || definition.BlockPolicy != 0 {
		t.Fatalf("unexpected creation defaults: %+v", definition)
	}
	definition, err = normalizeInput(completeInput(), false, testNow())
	if err != nil || definition.MaxRetries != 0 || definition.PostEnabled {
		t.Fatalf("explicit zero/false lost: %+v %v", definition, err)
	}
	in := completeInput()
	in.ExecutionMode, in.PostEnabled = pointer(int32(2)), pointer(true)
	if _, err := normalizeInput(in, false, testNow()); err != nil {
		t.Fatal(err)
	}
}

func TestNormalizeInputRejectsInvalidFields(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*job.JobInput)
	}{
		{"group", func(in *job.JobInput) { in.ExecutorGroupId = 0 }},
		{"timeout", func(in *job.JobInput) { in.TimeoutSeconds = 0 }},
		{"blank name", func(in *job.JobInput) { in.Name = " \t " }},
		{"name capacity", func(in *job.JobInput) { in.Name = strings.Repeat("任", 129) }},
		{"remark capacity", func(in *job.JobInput) { in.Remark = strings.Repeat("任", 501) }},
		{"processor capacity", func(in *job.JobInput) { in.ProcessorName = strings.Repeat("任", 129) }},
		{"blank processor", func(in *job.JobInput) { in.ProcessorName = " " }},
		{"NUL", func(in *job.JobInput) { in.Params = "secret\x00value" }},
		{"UTF8", func(in *job.JobInput) { in.Name = string([]byte{0xff}) }},
		{"mode", func(in *job.JobInput) { in.ExecutionMode = pointer(int32(0)) }},
		{"normal post", func(in *job.JobInput) { in.PostEnabled = pointer(true) }},
		{"misfire", func(in *job.JobInput) { in.MisfirePolicy = pointer(int32(2)) }},
		{"block", func(in *job.JobInput) { in.BlockPolicy = pointer(int32(-1)) }},
		{"retries", func(in *job.JobInput) { in.MaxRetries = pointer(int32(-1)) }},
		{"interval", func(in *job.JobInput) { in.RetryIntervalSeconds = pointer(int32(0)) }},
		{"cron five fields", func(in *job.JobInput) { in.CronExpr = "* * * * *" }},
		{"cron no next", func(in *job.JobInput) { in.CronExpr = "0 0 0 30 2 *" }},
		{"cron capacity", func(in *job.JobInput) { in.CronExpr = strings.Repeat("x", 129) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			in := validInput()
			test.change(in)
			if _, err := normalizeInput(in, true, testNow()); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("error=%v", err)
			}
		})
	}
	if _, err := normalizeInput(nil, true, testNow()); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("nil input: %v", err)
	}
	for _, field := range []string{"mode", "post", "misfire", "block", "retries", "interval"} {
		t.Run("missing "+field, func(t *testing.T) {
			in := completeInput()
			switch field {
			case "mode":
				in.ExecutionMode = nil
			case "post":
				in.PostEnabled = nil
			case "misfire":
				in.MisfirePolicy = nil
			case "block":
				in.BlockPolicy = nil
			case "retries":
				in.MaxRetries = nil
			case "interval":
				in.RetryIntervalSeconds = nil
			}
			if _, err := normalizeInput(in, false, testNow()); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("error=%v", err)
			}
		})
	}
	in := validInput()
	in.Name = "  " + strings.Repeat("任", 128) + "  "
	in.Remark = " " + strings.Repeat("说", 500) + " "
	in.Params = strings.Repeat("raw ", 20000)
	if _, err := normalizeInput(in, true, testNow()); err != nil {
		t.Fatalf("valid trimmed Unicode/large params rejected: %v", err)
	}
}

func TestNormalizeFilter(t *testing.T) {
	filter, err := normalizeFilter(&job.ListJobsRequest{Page: 2, PageSize: 20, Id: pointer(int64(7)), Name: "  A%_!  ", ExecutorGroupId: pointer(int64(3)), Status: pointer(int32(0))})
	if err != nil || filter.Name != "A%_!" || filter.Status == nil || *filter.Status != 0 || *filter.ID != 7 || *filter.ExecutorGroupID != 3 {
		t.Fatalf("filter=%+v error=%v", filter, err)
	}
	for _, in := range []*job.ListJobsRequest{nil, {Page: 0, PageSize: 1}, {Page: 1, PageSize: 0}, {Page: 1, PageSize: 201}, {Page: math.MaxInt64, PageSize: 200}, {Page: 1, PageSize: 20, Id: pointer(int64(0))}, {Page: 1, PageSize: 20, ExecutorGroupId: pointer(int64(-1))}, {Page: 1, PageSize: 20, Status: pointer(int32(2))}, {Page: 1, PageSize: 20, Name: strings.Repeat("任", 129)}} {
		if _, err := normalizeFilter(in); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("request=%+v error=%v", in, err)
		}
	}
}

func TestNormalizeTriggerPresenceAndUUID(t *testing.T) {
	for _, test := range []struct {
		name       string
		overridden *bool
		params     *string
		wantError  bool
		want       string
	}{
		{"missing flag", nil, nil, true, ""}, {"inherit", pointer(false), nil, false, ""}, {"inherit empty", pointer(false), pointer(""), true, ""}, {"inherit spaces", pointer(false), pointer("  "), true, ""}, {"override missing", pointer(true), nil, true, ""}, {"override empty", pointer(true), pointer(""), false, ""}, {"override spaces", pointer(true), pointer("  "), false, ""}, {"override text", pointer(true), pointer(" x\n y "), false, "x\n y"}, {"override NUL", pointer(true), pointer("\x00"), true, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			in := &job.TriggerJobRequest{Id: 7, RequestId: "12051A0F-76B8-4D88-897F-15EC600DE323", OperatorId: 42, ParamsOverridden: test.overridden, Params: test.params}
			result, err := normalizeTrigger(in)
			if test.wantError {
				if status.Code(err) != codes.InvalidArgument {
					t.Fatalf("error=%v", err)
				}
				return
			}
			if err != nil || result.Params != test.want || result.RequestID.String() != "12051a0f-76b8-4d88-897f-15ec600de323" || result.OperatorID != 42 || result.JobID != 7 {
				t.Fatalf("result=%+v error=%v", result, err)
			}
		})
	}
	for _, in := range []*job.TriggerJobRequest{nil, {Id: 7, OperatorId: 0, ParamsOverridden: pointer(false)}, {Id: 0, OperatorId: 42, ParamsOverridden: pointer(false)}, {Id: 7, OperatorId: 42, ParamsOverridden: pointer(false), RequestId: "invalid"}, {Id: 7, OperatorId: 42, ParamsOverridden: pointer(false), RequestId: "12051a0f76b84d88897f15ec600de323"}} {
		if _, err := normalizeTrigger(in); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("request=%+v error=%v", in, err)
		}
	}
}
