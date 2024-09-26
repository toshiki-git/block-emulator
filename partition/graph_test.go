package partition

import (
	"testing"
)

// TestVertexCreation tests the creation of a Vertex
func TestVertexCreation(t *testing.T) {
	v := Vertex{}
	v.ConstructVertex("test_address")
	if v.Addr != "test_address" {
		t.Errorf("Expected address 'test_address', got %s", v.Addr)
	}
}

// TestAddVertex tests adding a vertex to the graph
func TestAddVertex(t *testing.T) {
	g := Graph{}
	v := Vertex{Addr: "test_address"}

	g.AddVertex(v)

	if !g.VertexSet[v] {
		t.Errorf("Vertex not added correctly")
	}
}

// TestAddEdge tests adding an edge to the graph
func TestAddEdge(t *testing.T) {
	g := Graph{}
	u := Vertex{Addr: "address_u"}
	v := Vertex{Addr: "address_v"}

	g.AddEdge(u, v)

	if !g.VertexSet[u] || !g.VertexSet[v] {
		t.Errorf("Vertices not added correctly")
	}

	if len(g.EdgeSet[u]) != 1 || len(g.EdgeSet[v]) != 1 {
		t.Errorf("Edges not added correctly")
	}

	if g.EdgeSet[u][0] != v || g.EdgeSet[v][0] != u {
		t.Errorf("Edges not set correctly")
	}
}

// TestCopyGraph tests copying a graph
func TestCopyGraph(t *testing.T) {
	src := Graph{}
	u := Vertex{Addr: "address_u"}
	v := Vertex{Addr: "address_v"}

	src.AddEdge(u, v)

	dst := Graph{}
	dst.CopyGraph(src)

	if len(dst.VertexSet) != len(src.VertexSet) {
		t.Errorf("Vertex set not copied correctly")
	}

	if len(dst.EdgeSet) != len(src.EdgeSet) {
		t.Errorf("Edge set not copied correctly")
	}

	for key := range src.VertexSet {
		if !dst.VertexSet[key] {
			t.Errorf("Vertex %v not copied correctly", key)
		}
	}

	for key, value := range src.EdgeSet {
		if len(dst.EdgeSet[key]) != len(value) {
			t.Errorf("Edges for vertex %v not copied correctly", key)
		}
		for i := range value {
			if dst.EdgeSet[key][i] != value[i] {
				t.Errorf("Edge %v -> %v not copied correctly", key, value[i])
			}
		}
	}
}
