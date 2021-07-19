package localfs

import (
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDirectoryCleanup(t *testing.T) {
	testDir := t.TempDir()

	makeDummyFile := func(t *testing.T, filePath string) string {
		fullPath := path.Join(testDir, filePath)
		dir := path.Dir(fullPath)
		require.NotEmpty(t, dir)
		err := os.MkdirAll(dir, 0755)
		require.NoError(t, err)
		err = os.WriteFile(fullPath, []byte("don't delete this folder"), 0755)
		require.NoError(t, err)
		return fullPath
	}

	fileExists := func(t *testing.T, filePath string) bool {
		_, err := os.Stat(filePath)
		if err == nil {
			return true
		}
		if os.IsNotExist(err) {
			return false
		}
		require.FailNow(t, "failed to get status of file %s", filePath)
		return false
	}

	require.False(t, fileExists(t, "/heythisdoesntexist/overhere"))

	// /testDir
	//   a/
	//     dummyfile3
	//     b/
	//        c/
	//           dummyfile1
	//           dummyfile2
	//     d/
	//           dummyfile4

	d1 := makeDummyFile(t, "a/b/c/dummyfile1")
	d2 := makeDummyFile(t, "a/b/c/dummyfile2")
	d3 := makeDummyFile(t, "a/dummyfile3")
	d4 := makeDummyFile(t, "a/d/dummyfile4")

	l := &LocalStore{storepath: testDir}

	t.Run("delete-nonempty-dir", func(t *testing.T) {
		err := l.deleteParentDirs(path.Join(testDir, "a/d"))
		require.NoError(t, err)
		require.True(t, fileExists(t, d1))
		require.True(t, fileExists(t, d2))
		require.True(t, fileExists(t, d3))
		require.True(t, fileExists(t, d4))
	})

	t.Run("delete-nonempty-nested-child-dir", func(t *testing.T) {
		err := l.deleteParentDirs(path.Join(testDir, "a/b/c"))
		require.NoError(t, err)
		require.True(t, fileExists(t, d1))
		require.True(t, fileExists(t, d2))
		require.True(t, fileExists(t, d3))
		require.True(t, fileExists(t, d4))
	})

	t.Run("delete-nonempty-nested-parent-dir", func(t *testing.T) {
		err := l.deleteParentDirs(path.Join(testDir, "a/b"))
		require.NoError(t, err)
		require.True(t, fileExists(t, d1))
		require.True(t, fileExists(t, d2))
		require.True(t, fileExists(t, d3))
		require.True(t, fileExists(t, d4))
	})

	require.NoError(t, os.Remove(d4))

	t.Run("delete-empty-dir", func(t *testing.T) {
		err := l.deleteParentDirs(d4)
		require.NoError(t, err)
		require.True(t, fileExists(t, d1))
		require.True(t, fileExists(t, d2))
		require.True(t, fileExists(t, d3))
		require.False(t, fileExists(t, d4))
		require.False(t, fileExists(t, path.Join(testDir, "a/d")))
	})

	require.NoError(t, os.Remove(d1))
	require.NoError(t, os.Remove(d2))

	t.Run("delete-empty-nested-dir", func(t *testing.T) {
		err := l.deleteParentDirs(d2)
		require.NoError(t, err)
		require.False(t, fileExists(t, d1))
		require.False(t, fileExists(t, d2))
		require.False(t, fileExists(t, path.Join(testDir, "a/b/c")))
		require.False(t, fileExists(t, path.Join(testDir, "a/b")))
		require.True(t, fileExists(t, d3))
		require.False(t, fileExists(t, d4))
		require.False(t, fileExists(t, path.Join(testDir, "a/d")))
	})

	t.Run("delete-missing-dir", func(t *testing.T) {
		err := l.deleteParentDirs(path.Join(testDir, "doesntexist/what"))
		require.NoError(t, err)
	})

	require.True(t, fileExists(t, testDir))
}
