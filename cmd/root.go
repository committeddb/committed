package cmd

import (
	"encoding/json"
	"os"

	"github.com/spf13/cobra"

	"github.com/philborlin/committed/internal/version"
)

var rootCmd = &cobra.Command{
	Use:   "committed",
	Short: "A brief description of your application",
	Long: `A longer description that spans multiple lines and likely contains
examples and usage of using your application. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	// Run: func(cmd *cobra.Command, args []string) { },
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

// versionString returns the build Info as a JSON one-liner. The same
// shape is served by GET /version so scripts can diff the two without
// worrying about formatting drift.
func versionString() string {
	bs, err := json.Marshal(version.Get())
	if err != nil {
		// Info contains only strings, so Marshal cannot fail in
		// practice — but fall back to a plain Version rather than
		// panicking if it ever does.
		return version.Version
	}
	return string(bs)
}

func init() {
	rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")

	// Cobra auto-registers --version when Version is non-empty, and
	// handles the flag before Run is invoked (prints + exits 0). The
	// template prints just the version string so the stdout payload
	// is the same JSON shape as GET /version.
	rootCmd.Version = versionString()
	rootCmd.SetVersionTemplate("{{.Version}}\n")
}
