package explorer

import "sort"

// findCycle looks for a circular wait among thread-to-thread edges
// (To >= 0) and returns the participating thread ids in deterministic
// order, or nil if the graph is acyclic.
func findCycle(edges []WaitEdge, nodes []int) []int {
	adj := map[int][]int{}
	for _, e := range edges {
		if e.To >= 0 {
			adj[e.From] = append(adj[e.From], e.To)
		}
	}
	for k := range adj {
		sort.Ints(adj[k])
	}
	sorted := append([]int(nil), nodes...)
	sort.Ints(sorted)

	const (
		white = 0 // unvisited
		gray  = 1 // on the current DFS path
		black = 2 // done
	)
	color := map[int]int{}
	var stack []int
	var cycle []int

	var dfs func(u int) bool
	dfs = func(u int) bool {
		color[u] = gray
		stack = append(stack, u)
		for _, v := range adj[u] {
			if color[v] == gray {
				// Extract the cycle from the stack.
				start := 0
				for i, n := range stack {
					if n == v {
						start = i
						break
					}
				}
				cycle = append([]int(nil), stack[start:]...)
				return true
			}
			if color[v] == white && dfs(v) {
				return true
			}
		}
		stack = stack[:len(stack)-1]
		color[u] = black
		return false
	}
	for _, n := range sorted {
		if color[n] == white && dfs(n) {
			return cycle
		}
	}
	return nil
}
