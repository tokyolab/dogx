package job

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tokyolab/dogx/apps/system/api/internal/types"
	"github.com/tokyolab/dogx/pkg/requestvalidator"
	"github.com/zeromicro/go-zero/rest/httpx"
)

func setHTTPValidation(t *testing.T) {
	t.Helper()
	httpx.SetValidator(requestvalidator.New())
	t.Cleanup(func() { httpx.SetValidator(nil) })
}
func parseJSON(body string, value any) error {
	request := httptest.NewRequest("POST", "/job", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	return httpx.Parse(request, value)
}
func validCreateBody() map[string]any {
	return map[string]any{"name": " job ", "executorGroupId": int64(3), "processorName": "Handler", "cronExpr": "0 * * * * *", "timeoutSeconds": 900}
}
func marshalBody(t *testing.T, body any) string {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func TestCreatePresenceThroughHTTPParser(t *testing.T) {
	setHTTPValidation(t)
	for _, mode := range []string{"omitted", "null", "explicit"} {
		t.Run(mode, func(t *testing.T) {
			body := validCreateBody()
			for _, field := range []string{"executionMode", "postEnabled", "status", "misfirePolicy", "blockPolicy", "maxRetries", "retryIntervalSeconds", "remark", "params"} {
				if mode == "null" {
					body[field] = nil
				}
			}
			if mode == "explicit" {
				body["executionMode"] = 1
				body["postEnabled"] = false
				body["status"] = 0
				body["misfirePolicy"] = 0
				body["blockPolicy"] = 0
				body["maxRetries"] = 0
				body["retryIntervalSeconds"] = 10
				body["remark"] = ""
				body["params"] = ""
			}
			var result types.CreateJobReq
			if err := parseJSON(marshalBody(t, body), &result); err != nil {
				t.Fatal(err)
			}
			if result.Remark != "" || result.Params != "" || result.Name != " job " {
				t.Fatalf("text changed: %+v", result)
			}
			if mode != "explicit" {
				if result.ExecutionMode != nil || result.PostEnabled != nil || result.Status != nil || result.MisfirePolicy != nil || result.BlockPolicy != nil || result.MaxRetries != nil || result.RetryIntervalSeconds != nil {
					t.Fatalf("presence lost: %+v", result)
				}
				return
			}
			if result.ExecutionMode == nil || *result.ExecutionMode != 1 || result.PostEnabled == nil || *result.PostEnabled || result.Status == nil || *result.Status != 0 || result.MaxRetries == nil || *result.MaxRetries != 0 {
				t.Fatalf("explicit zero/false lost: %+v", result)
			}
		})
	}
	for _, body := range []string{`{"name":"x","executorGroupId":1,"processorName":"p","cronExpr":"0 * * * * *","timeoutSeconds":2147483648}`, `{"name":"x","executorGroupId":1,"processorName":"p","cronExpr":"0 * * * * *","timeoutSeconds":900,"executionMode":0}`} {
		var result types.CreateJobReq
		if err := parseJSON(body, &result); err == nil {
			t.Fatalf("invalid numeric request accepted: %s", body)
		}
	}
	body := validCreateBody()
	body["name"] = "  " + strings.Repeat("任", 128) + "  "
	var result types.CreateJobReq
	if err := parseJSON(marshalBody(t, body), &result); err != nil {
		t.Fatalf("parser rejected valid trim-before-length text: %v", err)
	}
}

func TestUpdateAndStatusRequiredPresenceThroughHTTPParser(t *testing.T) {
	setHTTPValidation(t)
	valid := validCreateBody()
	valid["id"] = 7
	valid["executionMode"] = 1
	valid["postEnabled"] = false
	valid["misfirePolicy"] = 0
	valid["blockPolicy"] = 0
	valid["maxRetries"] = 0
	valid["retryIntervalSeconds"] = 10
	var result types.UpdateJobReq
	if err := parseJSON(marshalBody(t, valid), &result); err != nil {
		t.Fatal(err)
	}
	if result.PostEnabled == nil || *result.PostEnabled || result.MaxRetries == nil || *result.MaxRetries != 0 {
		t.Fatalf("required zero/false rejected: %+v", result)
	}
	for _, field := range []string{"postEnabled", "misfirePolicy", "blockPolicy", "maxRetries"} {
		for _, mode := range []string{"omitted", "null"} {
			t.Run(field+" "+mode, func(t *testing.T) {
				body := map[string]any{}
				for key, value := range valid {
					body[key] = value
				}
				if mode == "omitted" {
					delete(body, field)
				} else {
					body[field] = nil
				}
				var result types.UpdateJobReq
				if err := parseJSON(marshalBody(t, body), &result); err == nil {
					t.Fatal("missing required presence accepted")
				}
			})
		}
	}
	for _, test := range []struct {
		body  string
		valid bool
	}{{`{"id":7}`, false}, {`{"id":7,"status":null}`, false}, {`{"id":7,"status":0}`, true}, {`{"id":7,"status":1}`, true}, {`{"id":7,"status":2}`, false}} {
		var result types.UpdateJobStatusReq
		if err := parseJSON(test.body, &result); (err == nil) != test.valid {
			t.Fatalf("body=%s error=%v", test.body, err)
		}
	}
}

func TestListAndTriggerPresenceThroughHTTPParser(t *testing.T) {
	setHTTPValidation(t)
	for _, body := range []string{`{"page":1,"pageSize":20}`, `{"page":1,"pageSize":20,"id":null,"executorGroupId":null,"status":null}`, `{"page":1,"pageSize":20,"status":0}`} {
		var result types.JobListReq
		if err := parseJSON(body, &result); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(body, `"status":0`) && (result.Status == nil || *result.Status != 0) {
			t.Fatal("status=0 became omitted")
		}
	}
	for _, test := range []struct {
		suffix                     string
		valid, present, overridden bool
	}{
		{``, false, false, false}, {`,"paramsOverridden":null`, false, false, false}, {`,"paramsOverridden":false`, true, false, false}, {`,"paramsOverridden":false,"params":null`, true, false, false}, {`,"paramsOverridden":false,"params":""`, true, true, false}, {`,"paramsOverridden":true,"params":"  "`, true, true, true}, {`,"paramsOverridden":true,"params":null`, true, false, true},
	} {
		t.Run(test.suffix, func(t *testing.T) {
			var result types.TriggerJobReq
			err := parseJSON(`{"id":7,"requestId":"12051A0F-76B8-4D88-897F-15EC600DE323"`+test.suffix+`}`, &result)
			if (err == nil) != test.valid {
				t.Fatalf("error=%v", err)
			}
			if test.valid && ((result.Params != nil) != test.present || result.ParamsOverridden == nil || *result.ParamsOverridden != test.overridden) {
				t.Fatalf("presence changed before RPC validation: %+v", result)
			}
		})
	}
}
