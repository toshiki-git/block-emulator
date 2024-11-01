package partition

type UnionFind struct {
	Parent map[string]string // 各アドレスの親アドレス
	Rank   map[string]int    // 各アドレスのランク
}

// Find操作: グループの親を返す
func (uf *UnionFind) Find(addr string) string {
	// 初期化処理: addrがParentに存在しない場合は自身を親として初期化
	if _, exists := uf.Parent[addr]; !exists {
		uf.Parent[addr] = addr
		uf.Rank[addr] = 0 // Rankも初期化
	}

	// 親の再設定（パス圧縮）
	if uf.Parent[addr] != addr {
		uf.Parent[addr] = uf.Find(uf.Parent[addr]) // 再帰呼び出しで親を更新
	}

	return uf.Parent[addr]
}

// Union操作: 2つのアドレスのグループをマージし、親のアドレスを返す
func (uf *UnionFind) Union(addr1, addr2 string) string {
	root1 := uf.Find(addr1)
	root2 := uf.Find(addr2)

	var parentAddr string

	if root1 != root2 {
		if uf.Rank[root1] < uf.Rank[root2] {
			uf.Parent[root1] = root2
			parentAddr = root2
		} else if uf.Rank[root1] > uf.Rank[root2] {
			uf.Parent[root2] = root1
			parentAddr = root1
		} else {
			uf.Parent[root2] = root1
			uf.Rank[root1]++
			parentAddr = root1
		}
	} else {
		parentAddr = root1 // 既に同じグループの場合、どちらのルートを返しても同じ
	}

	return parentAddr
}

func (uf *UnionFind) GetParentMap() map[string]Vertex {
	parentMap := make(map[string]Vertex)
	for addr := range uf.Parent {
		parentMap[addr] = Vertex{Addr: uf.Find(addr), IsMerged: true}
	}
	return parentMap
}

func (uf *UnionFind) GetReverseParentMap() map[string][]string {
	reverseParentMap := make(map[string][]string)
	for addr := range uf.Parent {
		parentAddr := uf.Find(addr)
		reverseParentMap[parentAddr] = append(reverseParentMap[parentAddr], addr)
	}
	return reverseParentMap
}
