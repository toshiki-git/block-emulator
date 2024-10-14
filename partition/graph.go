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

// Edges of old vertices u and v are transferred to the new merged vertex
func (g *Graph) UpdateEdgesAfterMerge(u, v, newMergedVertex Vertex) {
	// Combine all edges of u and v and reassign them to newMergedVertex
	if edgesU, exists := g.EdgeSet[u]; exists {
		for _, neighbor := range edgesU {
			if neighbor == v {
				continue
			}
			g.AddEdge(newMergedVertex, neighbor)
		}
		delete(g.EdgeSet, u) // Remove old vertex edges
	}

	if edgesV, exists := g.EdgeSet[v]; exists {
		for _, neighbor := range edgesV {
			if neighbor == u {
				continue
			}
			g.AddEdge(newMergedVertex, neighbor)
		}
		delete(g.EdgeSet, v) // Remove old vertex edges
	}

	// Ensure that any existing edges pointing to u or v are updated to point to newMergedVertex
	for vertex, edges := range g.EdgeSet {
		for i, neighbor := range edges {
			if neighbor == u || neighbor == v {
				g.EdgeSet[vertex] = append(g.EdgeSet[vertex][:i], g.EdgeSet[vertex][i+1:]...)
				i-- // Adjust index after deletion to continue correctly
			}
		}
	}

	// Remove u and v from VertexSet
	delete(g.VertexSet, u)
	delete(g.VertexSet, v)

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
