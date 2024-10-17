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
	var newSlice []Vertex
	for _, v := range slice {
		if v != vertex {
			newSlice = append(newSlice, v)
		}
	}
	return newSlice
}

func (g *Graph) transferEdgesAndRemove(source, mergedVertex Vertex) {
	if edges, exists := g.EdgeSet[source]; exists {
		var edgesToRemove []Vertex // Store edges to be removed after the loop
		for _, neighbor := range edges {
			g.AddEdge(mergedVertex, neighbor)               // Add the edge to the target vertex
			edgesToRemove = append(edgesToRemove, neighbor) // Mark for later removal
		}

		// Remove edges from the source vertex
		for _, neighbor := range edgesToRemove {
			g.RemoveEdge(source, neighbor)
		}

		// Remove the source vertex's edges
		delete(g.EdgeSet, source)
	}

	// Remove the source vertex from the VertexSet
	delete(g.VertexSet, source)
}

func (g *Graph) UpdateGraphForMerge(u, v, mergedVertex Vertex) {
	g.RemoveEdge(u, v)
	g.transferEdgesAndRemove(u, mergedVertex)
	g.transferEdgesAndRemove(v, mergedVertex)

	// Add the new merged vertex to the VertexSet
	g.VertexSet[mergedVertex] = true
}

func (g *Graph) UpdateGraphForPartialMerge(u, mergedVertex Vertex) {
	g.RemoveEdge(u, mergedVertex)
	g.transferEdgesAndRemove(u, mergedVertex)
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
