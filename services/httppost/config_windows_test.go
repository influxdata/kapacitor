//go:build windows

package httppost

import (
	"testing"
)

func Test_Validate_ShouldNotReturnError_WhenAlertTemplateFileIsWindowsLikePath(t *testing.T) {
	testCases := []struct {
		config Config
		err    error
	}{
		{
			config: Config{
				Endpoint:          "test",
				URLTemplate:       "http://localhost:8080/alert",
				AlertTemplateFile: "C:\\InfluxData\\kapacitor\\templates\\alert_template.json",
			},
			err: nil,
		},
	}

	for _, tc := range testCases {
		validationErr := tc.config.Validate()

		if validationErr != nil {
			if tc.err == nil {
				t.Errorf("unexpected error: got %v", validationErr)
			} else if tc.err.Error() != validationErr.Error() {
				t.Errorf("unexpected error message: got %q exp %q", validationErr.Error(), tc.err.Error())
			}
		} else {
			if tc.err != nil {
				t.Errorf("expected error: %q got nil", tc.err.Error())
			}
		}
	}
}

func Test_Validate_ShouldNotReturnError_WhenRowTemplateFileIsWindowsLikePath(t *testing.T) {
	testCases := []struct {
		config Config
		err    error
	}{
		{
			config: Config{
				Endpoint:        "test",
				URLTemplate:     "http://localhost:8080/alert",
				RowTemplateFile: "C:\\InfluxData\\kapacitor\\templates\\row_template.json",
			},
			err: nil,
		},
	}

	for _, tc := range testCases {
		validationErr := tc.config.Validate()

		if validationErr != nil {
			if tc.err == nil {
				t.Errorf("unexpected error: got %v", validationErr)
			} else if tc.err.Error() != validationErr.Error() {
				t.Errorf("unexpected error message: got %q exp %q", validationErr.Error(), tc.err.Error())
			}
		} else {
			if tc.err != nil {
				t.Errorf("expected error: %q got nil", tc.err.Error())
			}
		}
	}
}
