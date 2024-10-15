package partition

// Node in the graph, representing an account participating in transactions in the blockchain network
type Vertex struct {
	Addr string // Account address
	// Additional attributes to be added
	IsMerged bool
}

// Graph representing the current set of blockchain transactions
type Graph struct {
	VertexSet map[Vertex]bool     // Set of nodes, essentially a set
	EdgeSet   map[Vertex][]Vertex // Records transactions between nodes, adjacency list
	// lock      sync.RWMutex       // Lock, but each storage node stores a separate copy of the graph, so not needed
}

// Create a node
func (v *Vertex) ConstructVertex(s string) {
	v.Addr = s
}

func (v *Vertex) ConstructMergedVertex() {
	v.Addr = "hogehoge" //適切な名前を付ける
	v.IsMerged = true
}

// Add a node to the graph
func (g *Graph) AddVertex(v Vertex) {
	if g.VertexSet == nil {
		g.VertexSet = make(map[Vertex]bool)
	}
	g.VertexSet[v] = true
}

// Add an edge to the graph
func (g *Graph) AddEdge(u, v Vertex) {
	// If the node doesn't exist, add it. The weight is always 1.
	if _, ok := g.VertexSet[u]; !ok {
		g.AddVertex(u)
	}
	if _, ok := g.VertexSet[v]; !ok {
		g.AddVertex(v)
	}
	if g.EdgeSet == nil {
		g.EdgeSet = make(map[Vertex][]Vertex)
	}
	// Undirected graph, using bidirectional edges
	g.EdgeSet[u] = append(g.EdgeSet[u], v)
	g.EdgeSet[v] = append(g.EdgeSet[v], u)
}

// Remove an edge from the graph
func (g *Graph) RemoveEdge(u, v Vertex) {
	// Remove v from u's adjacency list
	if neighbors, ok := g.EdgeSet[u]; ok {
		g.EdgeSet[u] = removeFromSlice(neighbors, v)
	}

	// Remove u from v's adjacency list (since it's an undirected graph)
	if neighbors, ok := g.EdgeSet[v]; ok {
		g.EdgeSet[v] = removeFromSlice(neighbors, u)
	}
}

// Helper function to remove a vertex from a slice of vertices
func removeFromSlice(slice []Vertex, vertex Vertex) []Vertex {
	for i, v := range slice {
		if v == vertex {
			// Remove the vertex by creating a new slice without it
			return append(slice[:i], slice[i+1:]...)
		}
	}
	return slice
}

func (g *Graph) UpdateEdgesForNewMerge(u, v, newMergedVertex Vertex) {
	g.RemoveEdge(u, v) // u と v のエッジを削除
	// u のエッジを新しい頂点に移す
	if edgesU, exists := g.EdgeSet[u]; exists {
		var edgesToRemove []Vertex // 削除対象のエッジを保持
		for _, neighbor := range edgesU {
			g.AddEdge(newMergedVertex, neighbor)
			edgesToRemove = append(edgesToRemove, neighbor) // 後で削除するエッジを追加
		}

		for _, neighbor := range edgesToRemove {
			g.RemoveEdge(u, neighbor)
		}
		delete(g.EdgeSet, u)
	}

	// v のエッジを新しい頂点に移す
	if edgesV, exists := g.EdgeSet[v]; exists {
		var edgesToRemove []Vertex // 削除対象のエッジを保持
		for _, neighbor := range edgesV {
			g.AddEdge(newMergedVertex, neighbor)
			edgesToRemove = append(edgesToRemove, neighbor)
		}
		for _, neighbor := range edgesToRemove {
			g.RemoveEdge(v, neighbor)
		}
		delete(g.EdgeSet, v)
	}

	// u と v を VertexSet から削除
	delete(g.VertexSet, u)
	delete(g.VertexSet, v)

	// 新しい頂点を VertexSet に追加
	g.VertexSet[newMergedVertex] = true
}

func (g *Graph) UpdateEdgesForPartialMerge(u, merged Vertex) {
	// Transfer all edges of the unmerged vertex to the merged vertex
	g.RemoveEdge(u, merged) // Remove the edge between u and merged
	if edges, exists := g.EdgeSet[u]; exists {
		var edgesToRemove []Vertex // Store edges to be removed after the loop
		for _, neighbor := range edges {
			g.AddEdge(merged, neighbor)
			edgesToRemove = append(edgesToRemove, neighbor) // Mark for later removal
		}

		// Remove edges after the loop
		for _, neighbor := range edgesToRemove {
			g.RemoveEdge(u, neighbor)
		}
		delete(g.EdgeSet, u) // Remove old edges of the unmerged vertex
	}

	// Remove the unmerged vertex from the VertexSet
	delete(g.VertexSet, u)
}

func (g *Graph) UpdateEdgesForDoubleMerge(mergedU, mergedV, newMergedVertex Vertex) {
	// Transfer all edges of mergedU to the newMergedVertex
	g.RemoveEdge(mergedU, mergedV) // Remove the edge between mergedU and mergedV
	if edgesU, exists := g.EdgeSet[mergedU]; exists {
		var edgesToRemove []Vertex // Store edges to be removed after the loop
		for _, neighbor := range edgesU {
			g.AddEdge(newMergedVertex, neighbor)
			edgesToRemove = append(edgesToRemove, neighbor) // Mark for later removal
		}

		// Remove edges after the loop
		for _, neighbor := range edgesToRemove {
			g.RemoveEdge(mergedU, neighbor)
		}
		delete(g.EdgeSet, mergedU) // Remove old edges of mergedU
	}

	// Transfer all edges of mergedV to the newMergedVertex
	if edgesV, exists := g.EdgeSet[mergedV]; exists {
		var edgesToRemove []Vertex // Store edges to be removed after the loop
		for _, neighbor := range edgesV {
			g.AddEdge(newMergedVertex, neighbor)
			edgesToRemove = append(edgesToRemove, neighbor) // Mark for later removal
		}

		// Remove edges after the loop
		for _, neighbor := range edgesToRemove {
			g.RemoveEdge(mergedV, neighbor)
		}
		delete(g.EdgeSet, mergedV) // Remove old edges of mergedV
	}

	// Remove mergedU and mergedV from VertexSet
	delete(g.VertexSet, mergedU)
	delete(g.VertexSet, mergedV)

	// Add newMergedVertex to VertexSet
	g.VertexSet[newMergedVertex] = true
}

// Copy a graph
func (dst *Graph) CopyGraph(src Graph) {
	dst.VertexSet = make(map[Vertex]bool)
	for v := range src.VertexSet {
		dst.VertexSet[v] = true
	}
	if src.EdgeSet != nil {
		dst.EdgeSet = make(map[Vertex][]Vertex)
		for v := range src.VertexSet {
			dst.EdgeSet[v] = make([]Vertex, len(src.EdgeSet[v]))
			copy(dst.EdgeSet[v], src.EdgeSet[v])
		}
	}
}

// Print the graph
func (g Graph) PrintGraph() {
	for v := range g.VertexSet {
		print(v.Addr, " ")
		print("edge:")
		for _, u := range g.EdgeSet[v] {
			print(" ", u.Addr, "\t")
		}
		println()
	}
	println()
}
