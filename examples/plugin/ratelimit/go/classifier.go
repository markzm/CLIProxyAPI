package main

import (
	"net/http"
	"strings"
)

// Classifier identifies client type from headers and request attributes.
type Classifier struct{}

// NewClassifier creates a new client type classifier.
func NewClassifier() *Classifier {
	return &Classifier{}
}

// Resolve determines the normalized client type string.
func (c *Classifier) Resolve(headers http.Header, sourceFormat, path string) string {
	if headers != nil {
		// 1. Check explicit client header override first.
		if ct := strings.TrimSpace(headers.Get("X-Client-Type")); ct != "" {
			return strings.ToLower(ct)
		}
		if ct := strings.TrimSpace(headers.Get("X-Caller-Type")); ct != "" {
			return strings.ToLower(ct)
		}

		// 2. Inspect User-Agent fingerprint.
		ua := headers.Get("User-Agent")
		if clientType := matchUserAgent(ua); clientType != "" {
			return clientType
		}
	}

	// 3. Fallback based on path or sourceFormat if available.
	lowerPath := strings.ToLower(strings.TrimSpace(path))
	lowerFormat := strings.ToLower(strings.TrimSpace(sourceFormat))

	if strings.Contains(lowerPath, "/backend-api/codex") || strings.Contains(lowerPath, "/responses") || lowerFormat == "codex" || lowerFormat == "openai-response" {
		return "codex"
	}
	if strings.Contains(lowerPath, "/messages") || lowerFormat == "claude" {
		return "claude-code"
	}
	if strings.Contains(lowerPath, "/v1beta") || lowerFormat == "gemini" {
		return "gemini"
	}

	return "default"
}

func matchUserAgent(ua string) string {
	if ua == "" {
		return ""
	}

	lowerUA := strings.ToLower(ua)

	// Codex official client family
	if strings.Contains(lowerUA, "codex desktop") ||
		strings.Contains(lowerUA, "codex_cli_rs") ||
		strings.Contains(lowerUA, "codex-cli") ||
		strings.Contains(lowerUA, "codex/") ||
		strings.Contains(lowerUA, "codex-tui") ||
		strings.Contains(lowerUA, "codex_exec") ||
		strings.Contains(lowerUA, "codex_vscode") {
		return "codex"
	}

	// Claude tool family
	if strings.Contains(lowerUA, "claude code") ||
		strings.Contains(lowerUA, "claude-cli") ||
		strings.Contains(lowerUA, "anthropic-sdk") {
		return "claude-code"
	}

	// Gemini tool family
	if strings.Contains(lowerUA, "antigravity") ||
		strings.Contains(lowerUA, "google-genai") ||
		strings.Contains(lowerUA, "gemini-cli") {
		return "gemini"
	}

	// IDE plugins
	if strings.Contains(lowerUA, "cursor") ||
		strings.Contains(lowerUA, "continue") ||
		strings.Contains(lowerUA, "cline") ||
		strings.Contains(lowerUA, "roo-code") ||
		strings.Contains(lowerUA, "chatbox") {
		return "ide-plugin"
	}

	// Automated scripts / crawlers
	if strings.Contains(lowerUA, "python-requests") ||
		strings.Contains(lowerUA, "aiohttp") ||
		strings.HasPrefix(lowerUA, "curl/") ||
		strings.Contains(lowerUA, "go-http-client") ||
		strings.Contains(lowerUA, "node-fetch") ||
		strings.Contains(lowerUA, "urllib") {
		return "script-bot"
	}

	return ""
}
