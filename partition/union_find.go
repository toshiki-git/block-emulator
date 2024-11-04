package partition

type UnionFind struct {
	Parent         map[string]string // 各アドレスの親アドレス
	Rank           map[string]int    // 各アドレスのランク
	HasUnioned     map[string]bool   // 各アドレスがUnion操作されたかどうかを追跡
	UnionCount     map[string]int    // 各アドレスのUnion操作回数
	NodeUnionCount map[string]int
	MaxUnion       int // 最大のUnion操作回数
	MaxNodeUnion   int // 各ノードの最大Union操作参加回数
}

// 初期化
func NewUnionFind() *UnionFind {
	return &UnionFind{
		Parent:         make(map[string]string),
		Rank:           make(map[string]int),
		HasUnioned:     make(map[string]bool),
		UnionCount:     make(map[string]int),
		NodeUnionCount: make(map[string]int),
		MaxUnion:       1000, //ここで最大のUnion操作回数を設定
		MaxNodeUnion:   100,  //ここで各ノードの最大Union操作参加回数を設定
	}
}

// Find操作: グループの親を返す（初期化はしない）
func (uf *UnionFind) Find(addr string) string {
	// 初期化されていない場合はaddrをそのまま返す
	if _, exists := uf.Parent[addr]; !exists {
		return addr
	}

	// 親の再設定（パス圧縮）
	if uf.Parent[addr] != addr {
		uf.Parent[addr] = uf.Find(uf.Parent[addr]) // 再帰呼び出しで親を更新
	}

	return uf.Parent[addr]
}

// Union操作: 2つのアドレスのグループをマージし、親のアドレスを返す
func (uf *UnionFind) Union(addr1, addr2 string) string {
	// addr1とaddr2を初期化（必要であれば）
	if _, exists := uf.Parent[addr1]; !exists {
		uf.Parent[addr1] = addr1
		uf.Rank[addr1] = 0
		uf.HasUnioned[addr1] = false
		uf.UnionCount[addr1] = 0     // 初期マージ回数を0に設定
		uf.NodeUnionCount[addr1] = 0 // 初期ノードマージ参加回数を0に設定
	}
	if _, exists := uf.Parent[addr2]; !exists {
		uf.Parent[addr2] = addr2
		uf.Rank[addr2] = 0
		uf.HasUnioned[addr2] = false
		uf.UnionCount[addr2] = 0
		uf.NodeUnionCount[addr2] = 0
	}

	root1 := uf.Find(addr1)
	root2 := uf.Find(addr2)

	// 既に同じグループに属している場合はスキップ
	if root1 == root2 {
		return root1
	}

	// **追加: ノードのマージ参加回数が上限に達しているか確認**
	if uf.NodeUnionCount[root1] >= uf.MaxNodeUnion || uf.NodeUnionCount[root2] >= uf.MaxNodeUnion {
		// マージを行わず、元の親を返す
		return root1
	}

	// マージ回数の合計を計算
	totalUnionCount := uf.UnionCount[root1] + uf.UnionCount[root2] + 1

	// グループのマージ回数が上限を超える場合、マージを中止
	if totalUnionCount > uf.MaxUnion {
		// マージを行わず、元の親を返す
		return root1
	}

	var parentAddr string

	if uf.Rank[root1] < uf.Rank[root2] {
		uf.Parent[root1] = root2
		parentAddr = root2
		// マージ回数を更新
		uf.UnionCount[root2] = totalUnionCount
		// ノードのマージ参加回数を更新
		uf.NodeUnionCount[root1]++
		uf.NodeUnionCount[root2]++
	} else {
		uf.Parent[root2] = root1
		if uf.Rank[root1] == uf.Rank[root2] {
			uf.Rank[root1]++
		}
		parentAddr = root1
		uf.UnionCount[root1] = totalUnionCount
		uf.NodeUnionCount[root1]++
		uf.NodeUnionCount[root2]++
	}

	// Union操作が行われたので、両方のノードに対してフラグを立てる
	uf.HasUnioned[addr1] = true
	uf.HasUnioned[addr2] = true

	return parentAddr
}

// ノードがUnion操作されたことがあるかどうかを確認する関数
func (uf *UnionFind) HasBeenUnioned(addr string) bool {
	if _, exists := uf.HasUnioned[addr]; exists {
		return uf.HasUnioned[addr]
	}
	return false // 初期化されていないノードはUnionされていないとみなす
}

func (uf *UnionFind) IsConnected(addr1, addr2 string) bool {
	return uf.Find(addr1) == uf.Find(addr2)
}

func (uf *UnionFind) GetParentMap() map[string]Vertex {
	parentMap := make(map[string]Vertex)
	for addr := range uf.Parent {
		// HasUnionedがtrueのものだけを対象にする
		if uf.HasUnioned[addr] {
			parentMap[addr] = Vertex{Addr: uf.Find(addr)}
		}
	}
	return parentMap
}

func (uf *UnionFind) GetReverseParentMap() map[string][]string {
	reverseParentMap := make(map[string][]string)
	for addr := range uf.Parent {
		// HasUnionedがtrueのものだけを対象にする
		if uf.HasUnioned[addr] {
			parentAddr := uf.Find(addr)
			reverseParentMap[parentAddr] = append(reverseParentMap[parentAddr], addr)
		}
	}
	return reverseParentMap
}
