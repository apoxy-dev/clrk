package cmd

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/cobra/doc"
)

// GenerateDocs walks the cobra command tree and writes a single Markdown
// reference (`clrk.mdx`) under the given directory. The output is shaped
// for the apoxy-cloud docs2 site: every command becomes a section, and
// internal cross-references collapse to in-page anchors.
func GenerateDocs(outDir string) error {
	// Normalize machine-specific defaults so generated output is identical
	// across developers. --clrk-dir defaults to $HOME/.clrk at runtime;
	// rewrite the displayed default to use the literal "$HOME" placeholder.
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		if f := RootCmd.PersistentFlags().Lookup("clrk-dir"); f != nil {
			f.DefValue = strings.Replace(f.DefValue, home, "$HOME", 1)
		}
	}

	// Normalize the image-ref flag defaults. At runtime DefaultControllerManagerImage /
	// DefaultWorkerImage are "<gar-path>:<8-char-vcs-sha>" (or "<gar-path>:latest" when the
	// doc-gen binary carries no VCS info). The publish workflow never pushes a :latest tag,
	// so rendering the literal default into the reference would document a ref that 404s.
	// Replace the tag with a placeholder that says "matches the running clrk binary's SHA".
	var normalizeImageDefaults func(cmd *cobra.Command)
	normalizeImageDefaults = func(cmd *cobra.Command) {
		for _, name := range []string{"controller-image", "worker-image"} {
			if f := cmd.Flags().Lookup(name); f != nil {
				if i := strings.LastIndex(f.DefValue, ":"); i >= 0 {
					f.DefValue = f.DefValue[:i] + ":<clrk-commit-sha>"
				}
			}
		}
		for _, c := range cmd.Commands() {
			normalizeImageDefaults(c)
		}
	}
	normalizeImageDefaults(RootCmd)

	anchorLinks := func(s string) string {
		s = strings.ReplaceAll(s, "_", "-")
		s = strings.ToLower(s)
		s = strings.ReplaceAll(s, ".md", "")
		return fmt.Sprintf("#%s", s)
	}
	emptyStr := func(s string) string { return "" }

	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return fmt.Errorf("creating output dir: %w", err)
	}
	files, err := genMarkdownTreeCustom(RootCmd, outDir, emptyStr, anchorLinks)
	if err != nil {
		return err
	}

	combined := ""
	for _, file := range files {
		b, err := os.ReadFile(file)
		if err != nil {
			return fmt.Errorf("reading %s: %w", file, err)
		}
		combined += string(b) + "\n\n"
	}
	if err := os.WriteFile(files[0], []byte(combined), 0o644); err != nil {
		return fmt.Errorf("writing combined doc: %w", err)
	}
	for _, file := range files[1:] {
		if err := os.Remove(file); err != nil {
			return fmt.Errorf("removing %s: %w", file, err)
		}
	}
	return nil
}

func genMarkdownTreeCustom(
	cmd *cobra.Command,
	dir string,
	filePrepender, linkHandler func(string) string,
) ([]string, error) {
	basename := strings.ReplaceAll(cmd.CommandPath(), " ", "_") + ".mdx"
	filename := filepath.Join(dir, basename)
	f, err := os.Create(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	if _, err := io.WriteString(f, filePrepender(filename)); err != nil {
		return nil, err
	}
	if err := doc.GenMarkdownCustom(cmd, f, linkHandler); err != nil {
		return nil, err
	}

	newFiles := []string{filename}
	for _, c := range cmd.Commands() {
		if !c.IsAvailableCommand() || c.IsAdditionalHelpTopicCommand() {
			continue
		}
		children, err := genMarkdownTreeCustom(c, dir, filePrepender, linkHandler)
		if err != nil {
			return newFiles, err
		}
		newFiles = append(newFiles, children...)
	}
	return newFiles, nil
}
