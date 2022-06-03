package awstee

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfigLoadValid(t *testing.T) {
	cases := []struct {
		casename string
		path     string
	}{
		{
			casename: "default_config",
			path:     "testdata/default.yaml",
		},
	}

	for _, c := range cases {
		t.Run(c.casename, func(t *testing.T) {
			cfg := newConfig()
			err := cfg.Load(c.path)
			require.NoError(t, err)
		})
	}

}

func TestConfigLoadInValid(t *testing.T) {
	cases := []struct {
		casename string
		path     string
		expected string
	}{
		{
			casename: "s3_invalid_prefix",
			path:     "testdata/s3_invalid_prefix.yaml",
			expected: "s3 url_prefix schema is not `s3`: schema is ``",
		},
	}

	for _, c := range cases {
		t.Run(c.casename, func(t *testing.T) {
			cfg := newConfig()
			err := cfg.Load(c.path)
			require.Error(t, err)
			require.EqualError(t, err, c.expected)
		})
	}

}
