package stream

import (
	"bufio"
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// ValidateFolder checks that folder exists, is a directory, and contains at least one valid audio file for profile.
func ValidateFolder(profile, dir string) error {
	files, err := scanAudioFolder(dir, profile)
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return fmt.Errorf("no supported audio files in folder %s", dir)
	}
	var lastErr error
	for _, f := range files {
		if err := ValidateFile(profile, f); err == nil {
			return nil
		} else {
			lastErr = err
		}
	}
	return fmt.Errorf("no valid %s audio files in folder %s: %w", profile, dir, lastErr)
}

// ValidatePlaylist checks that the playlist exists and contains at least one valid audio file for profile.
func ValidatePlaylist(profile, path string) error {
	files, err := parsePlaylist(path, profile)
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return fmt.Errorf("no supported audio files found in playlist %s", path)
	}
	var lastErr error
	for _, f := range files {
		if err := ValidateFile(profile, f); err == nil {
			return nil
		} else {
			lastErr = err
		}
	}
	return fmt.Errorf("no valid %s audio files in playlist %s: %w", profile, path, lastErr)
}

func audioExtensionsForProfile(profile string) map[string]bool {
	switch profile {
	case "mp3":
		return map[string]bool{".mp3": true}
	case "aac-adts":
		return map[string]bool{".aac": true, ".adts": true}
	case "ogg-opus":
		return map[string]bool{".ogg": true, ".opus": true}
	default:
		return map[string]bool{
			".mp3":  true,
			".aac":  true,
			".adts": true,
			".ogg":  true,
			".opus": true,
		}
	}
}

func scanAudioFolder(folder, profile string) ([]string, error) {
	fi, err := os.Stat(folder)
	if err != nil {
		return nil, err
	}
	if !fi.IsDir() {
		return nil, fmt.Errorf("%s is not a directory", folder)
	}
	allowed := audioExtensionsForProfile(profile)
	var files []string
	err = filepath.WalkDir(folder, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return nil
		}
		if d.IsDir() {
			return nil
		}
		ext := strings.ToLower(filepath.Ext(d.Name()))
		if allowed[ext] {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("no supported audio files found in %s for profile %s", folder, profile)
	}
	sort.Strings(files)
	return files, nil
}

func parsePlaylist(path, profile string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	allowed := audioExtensionsForProfile(profile)
	baseDir := filepath.Dir(path)
	var files []string

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		line = strings.TrimPrefix(line, "\ufeff")
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		trackPath := line
		if !filepath.IsAbs(trackPath) {
			trackPath = filepath.Join(baseDir, trackPath)
		}
		trackPath = filepath.Clean(trackPath)
		ext := strings.ToLower(filepath.Ext(trackPath))
		if allowed[ext] {
			if _, err := os.Stat(trackPath); err == nil {
				files = append(files, trackPath)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("no valid audio files found in playlist %s for profile %s", path, profile)
	}
	return files, nil
}

func pumpPlaylist(ctx context.Context, files []string, shuffle bool, profile string, bitrate int, defaultTitle string, onTrackChange func(Metadata), out chan<- []byte) {
	defer close(out)
	if len(files) == 0 {
		return
	}
	if bitrate <= 0 {
		bitrate = 128000
	}

	playlist := make([]string, len(files))
	copy(playlist, files)

	var rng *rand.Rand
	if shuffle {
		rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	}

	lastPlayed := ""

	for {
		if ctx.Err() != nil {
			return
		}

		if shuffle && len(playlist) > 1 {
			rng.Shuffle(len(playlist), func(i, j int) {
				playlist[i], playlist[j] = playlist[j], playlist[i]
			})
			if playlist[0] == lastPlayed && len(playlist) > 1 {
				playlist[0], playlist[len(playlist)-1] = playlist[len(playlist)-1], playlist[0]
			}
		}

		for _, trackPath := range playlist {
			if ctx.Err() != nil {
				return
			}

			title := FormatTrackTitle(trackPath, defaultTitle)
			if onTrackChange != nil {
				onTrackChange(Metadata{Title: title})
			}
			lastPlayed = trackPath

			f, err := os.Open(trackPath)
			if err != nil {
				continue
			}

			_ = Pump(profile, f, func(data []byte) error {
				copyData := append([]byte(nil), data...)
				select {
				case out <- copyData:
				case <-ctx.Done():
					return ctx.Err()
				}
				delay := time.Duration(float64(time.Second) * float64(len(data)*8) / float64(bitrate))
				timer := time.NewTimer(delay)
				select {
				case <-timer.C:
					return nil
				case <-ctx.Done():
					timer.Stop()
					return ctx.Err()
				}
			})
			f.Close()
		}
	}
}
