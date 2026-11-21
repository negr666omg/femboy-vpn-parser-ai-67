package app

import "github.com/negr666omg/subwatch/internal/sub"

// geminiAvailable is the single export gate: only an explicit, error-free
// "available" result may be written to gemini-available.txt.
func geminiAvailable(n sub.Node) bool {
	return n.GeminiStatus == "available" && n.ServiceError == ""
}
