package auth

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestScanUserUniqueID(t *testing.T) {
	dir := t.TempDir()
	// 旧文件里的旧 ID 与新文件里的新 ID：应以最新修改的文件为准。
	old := filepath.Join(dir, "000001.ldb")
	newF := filepath.Join(dir, "000002.log")
	if err := os.WriteFile(old, []byte(`{"user_unique_id":"1111111111111111","timestamp":1}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(newF, []byte(`junk{"user_unique_id":"2222222222222222","timestamp":2}`), 0o600); err != nil {
		t.Fatal(err)
	}
	past := time.Now().Add(-time.Hour)
	if err := os.Chtimes(old, past, past); err != nil {
		t.Fatal(err)
	}
	id, ok := scanUserUniqueID(dir)
	if !ok || id != "2222222222222222" {
		t.Fatalf("got (%q,%v), want (2222222222222222,true)", id, ok)
	}
}

func TestScanUserUniqueIDMiss(t *testing.T) {
	// 目录不存在
	if _, ok := scanUserUniqueID(filepath.Join(t.TempDir(), "nope")); ok {
		t.Fatal("missing dir should miss")
	}
	// 目录存在但没有匹配内容
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "000001.log"), []byte(`{"other":"x"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, ok := scanUserUniqueID(dir); ok {
		t.Fatal("no user_unique_id should miss")
	}
	// 非数字 ID 不匹配
	dir2 := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir2, "000001.log"), []byte(`{"user_unique_id":"abc-def"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, ok := scanUserUniqueID(dir2); ok {
		t.Fatal("non-numeric id should miss")
	}
}
