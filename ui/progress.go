

package ui


import (
	"fmt"
	"io"
	"sync/atomic"

)


type ProgressWriter struct {
	Written int64
	Writer io.WriterAt
	Size int64
}

func (pw *ProgressWriter) WriteAt(p []byte, off int64) (int, error) {
	atomic.AddInt64(&pw.Written, int64(len(p)))
	percentageDownloaded := float32(pw.Written * 100) / float32(pw.Size)
	fmt.Printf("File size:%d downloaded:%d percentage:%.2f%%\r", pw.Size, pw.Written, percentageDownloaded)
	return pw.Writer.WriteAt(p, off)
}

func ByteCountDecimal(b int64) string {
	const unit = 1000
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "kMGTPE"[exp])
}


