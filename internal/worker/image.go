//go:build linux

package worker

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	ocispecv1 "github.com/opencontainers/image-spec/specs-go/v1"
	"golang.org/x/sync/singleflight"
	"oras.land/oras-go/v2"
	"oras.land/oras-go/v2/content"
	"oras.land/oras-go/v2/content/file"
	"oras.land/oras-go/v2/registry/remote"
	"oras.land/oras-go/v2/registry/remote/auth"
	orasretry "oras.land/oras-go/v2/registry/remote/retry"
	ctrl "sigs.k8s.io/controller-runtime"
)

// ImageStore manages OCI image pulling and rootfs extraction.
// Images are pulled via ORAS and layers extracted to a shared rootfs directory.
// APO-537 replaces the extraction path with nydusd FUSE mounts.
type ImageStore struct {
	baseDir string // e.g. /run/clrk/images

	mu     sync.Mutex
	images map[string]*ImageInfo // imageRef → info
	flight singleflight.Group    // Deduplicates concurrent pulls for the same image.
}

// ImageInfo holds metadata for a pulled image.
type ImageInfo struct {
	RootFS     string   // Path to extracted rootfs directory.
	Entrypoint []string // Default entrypoint from image config.
	Cmd        []string // Default cmd from image config.
}

// NewImageStore creates a new ImageStore rooted at baseDir.
func NewImageStore(baseDir string) *ImageStore {
	return &ImageStore{
		baseDir: baseDir,
		images:  make(map[string]*ImageInfo),
	}
}

// EnsureImage pulls and extracts the OCI image if not cached.
// Returns image info including the path to the extracted rootfs directory.
// Concurrent calls for the same imageRef are deduplicated via singleflight.
func (s *ImageStore) EnsureImage(ctx context.Context, imageRef string) (*ImageInfo, error) {
	s.mu.Lock()
	if info, ok := s.images[imageRef]; ok {
		s.mu.Unlock()
		return info, nil
	}
	s.mu.Unlock()

	// Deduplicate concurrent pulls for the same image.
	v, err, _ := s.flight.Do(imageRef, func() (interface{}, error) {
		// Re-check cache inside singleflight to handle the case where a
		// previous flight completed between our cache check and entering Do.
		s.mu.Lock()
		if info, ok := s.images[imageRef]; ok {
			s.mu.Unlock()
			return info, nil
		}
		s.mu.Unlock()

		return s.pullAndExtract(ctx, imageRef)
	})
	if err != nil {
		return nil, err
	}
	return v.(*ImageInfo), nil
}

// pullAndExtract does the actual OCI pull and layer extraction.
func (s *ImageStore) pullAndExtract(ctx context.Context, imageRef string) (*ImageInfo, error) {
	log := ctrl.LoggerFrom(ctx).WithValues("image", imageRef)
	log.Info("Pulling OCI image")

	// Create a temp dir for ORAS file store.
	stageDir, err := os.MkdirTemp(s.baseDir, "stage-*")
	if err != nil {
		return nil, fmt.Errorf("creating stage dir: %w", err)
	}
	defer os.RemoveAll(stageDir)

	// file.New caps unnamed-content pushes at 4 MiB by default; that's
	// where the manifest + image config end up, and many real-world
	// configs exceed it. 1 GiB is well above any practical config.
	fs, err := file.NewWithFallbackLimit(stageDir, 1<<30)
	if err != nil {
		return nil, fmt.Errorf("creating file store: %w", err)
	}
	defer fs.Close()

	repo, err := remote.NewRepository(imageRef)
	if err != nil {
		return nil, fmt.Errorf("creating repository: %w", err)
	}
	repo.Client = &auth.Client{
		Client:     orasretry.DefaultClient,
		Cache:      auth.NewCache(),
		Credential: auth.StaticCredential(repo.Reference.Registry, auth.EmptyCredential),
	}

	// Pull manifest + all layers. The platform pin matches what the worker
	// host can actually exec; bump MaxMetadataBytes well above the 4 MiB
	// default since oras-go uses the same limit for blob caching during
	// copy, and most real images have layers larger than that.
	opts := oras.CopyOptions{
		CopyGraphOptions: oras.CopyGraphOptions{MaxMetadataBytes: 1 << 30},
	}
	opts.WithTargetPlatform(&ocispecv1.Platform{
		Architecture: runtime.GOARCH,
		OS:           "linux",
	})
	desc, err := oras.Copy(ctx, repo, repo.Reference.Reference, fs, "", opts)
	if err != nil {
		return nil, fmt.Errorf("pulling image: %w", err)
	}
	log.Info("Image pulled", "digest", desc.Digest.String())

	// Parse manifest.
	manifestBlob, err := content.FetchAll(ctx, fs, desc)
	if err != nil {
		return nil, fmt.Errorf("fetching manifest: %w", err)
	}
	var manifest ocispecv1.Manifest
	if err := json.Unmarshal(manifestBlob, &manifest); err != nil {
		return nil, fmt.Errorf("unmarshaling manifest: %w", err)
	}

	// Parse image config to get entrypoint/cmd.
	configBlob, err := content.FetchAll(ctx, fs, manifest.Config)
	if err != nil {
		return nil, fmt.Errorf("fetching image config: %w", err)
	}
	var imgConfig ocispecv1.Image
	if err := json.Unmarshal(configBlob, &imgConfig); err != nil {
		return nil, fmt.Errorf("unmarshaling image config: %w", err)
	}

	// Extract layers to rootfs.
	rootFS := filepath.Join(s.baseDir, desc.Digest.Encoded(), "rootfs")
	if err := os.MkdirAll(rootFS, 0755); err != nil {
		return nil, fmt.Errorf("creating rootfs dir: %w", err)
	}
	for i, layer := range manifest.Layers {
		log.Info("Extracting layer", "index", i, "digest", layer.Digest.String(), "mediaType", layer.MediaType)
		layerBlob, err := content.FetchAll(ctx, fs, layer)
		if err != nil {
			return nil, fmt.Errorf("fetching layer %d: %w", i, err)
		}
		if err := extractLayer(rootFS, layerBlob, layer.MediaType); err != nil {
			return nil, fmt.Errorf("extracting layer %d: %w", i, err)
		}
	}

	info := &ImageInfo{
		RootFS:     rootFS,
		Entrypoint: imgConfig.Config.Entrypoint,
		Cmd:        imgConfig.Config.Cmd,
	}

	s.mu.Lock()
	s.images[imageRef] = info
	s.mu.Unlock()

	log.Info("Image extracted", "rootfs", rootFS)
	return info, nil
}

// extractLayer extracts a tar (optionally gzipped) layer into the rootfs directory,
// handling OCI whiteout files for layer squashing.
func extractLayer(rootFS string, data []byte, mediaType string) error {
	var r io.Reader = bytes.NewReader(data)

	// Handle gzip-compressed layers.
	if strings.Contains(mediaType, "gzip") {
		gr, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return fmt.Errorf("creating gzip reader: %w", err)
		}
		defer gr.Close()
		r = gr
	}

	cleanRoot := filepath.Clean(rootFS)

	tr := tar.NewReader(r)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("reading tar header: %w", err)
		}

		// Handle OCI whiteout files.
		base := filepath.Base(hdr.Name)
		dir := filepath.Dir(hdr.Name)
		if base == ".wh..wh..opq" {
			// Opaque whiteout: remove all children of this directory.
			target := filepath.Join(rootFS, dir)
			if !strings.HasPrefix(filepath.Clean(target), cleanRoot) {
				continue
			}
			entries, _ := os.ReadDir(target)
			for _, e := range entries {
				os.RemoveAll(filepath.Join(target, e.Name()))
			}
			continue
		}
		if strings.HasPrefix(base, ".wh.") {
			// File whiteout: remove the named file.
			target := filepath.Join(rootFS, dir, strings.TrimPrefix(base, ".wh."))
			if !strings.HasPrefix(filepath.Clean(target), cleanRoot) {
				continue
			}
			os.RemoveAll(target)
			continue
		}

		target := filepath.Join(rootFS, hdr.Name)
		// Guard against path traversal.
		if !strings.HasPrefix(filepath.Clean(target), cleanRoot) {
			continue
		}

		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, os.FileMode(hdr.Mode)); err != nil {
				return fmt.Errorf("creating directory %s: %w", target, err)
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
				return fmt.Errorf("creating parent dir for %s: %w", target, err)
			}
			f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(hdr.Mode))
			if err != nil {
				return fmt.Errorf("creating file %s: %w", target, err)
			}
			if _, err := io.Copy(f, tr); err != nil {
				f.Close()
				return fmt.Errorf("writing file %s: %w", target, err)
			}
			f.Close()
		case tar.TypeSymlink:
			if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
				return fmt.Errorf("creating parent dir for symlink %s: %w", target, err)
			}
			os.Remove(target) // Remove existing symlink if any.
			if err := os.Symlink(hdr.Linkname, target); err != nil {
				return fmt.Errorf("creating symlink %s: %w", target, err)
			}
		case tar.TypeLink:
			if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
				return fmt.Errorf("creating parent dir for hardlink %s: %w", target, err)
			}
			linkTarget := filepath.Join(rootFS, hdr.Linkname)
			os.Remove(target)
			if err := os.Link(linkTarget, target); err != nil {
				return fmt.Errorf("creating hardlink %s: %w", target, err)
			}
		}
	}
	return nil
}
