package types

import "math/rand"

func ReverseSlice[T any](s []T) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}

func FloydSamplingIndices(n, k int) map[int]struct{} {
	selected := make(map[int]struct{}, k)

	for i := n - k; i < n; i++ {
		r := rand.Intn(i + 1)
		if _, exists := selected[r]; exists {
			selected[i] = struct{}{}
		} else {
			selected[r] = struct{}{}
		}
	}

	return selected
}
