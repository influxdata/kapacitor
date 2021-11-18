package server

// parseEnvKey gives us the key from the environment string.
func parseEnvKey(s string) string {
	// copied from how the stdlib splits the env strings
	for i := 0; i < len(s); i++ {
		if s[i] == '=' {
			return s[:i]
		}
	}
	return s
}
