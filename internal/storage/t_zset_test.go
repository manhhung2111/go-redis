package storage

import (
	"testing"

	"github.com/manhhung2111/go-redis/internal/storage/data_structure"
	"github.com/stretchr/testify/assert"
)

func TestZAdd(t *testing.T) {
	s := NewStore()

	// base insert
	assert.Equal(t, ptr(uint32(1)),
		s.ZAdd("z", map[string]float64{"a": 1}, data_structure.ZAddOptions{}))

	// NX blocks update
	assert.Equal(t, ptr(uint32(0)),
		s.ZAdd("z", map[string]float64{"a": 2}, data_structure.ZAddOptions{NX: true}))

	// XX allows update
	assert.Equal(t, ptr(uint32(0)),
		s.ZAdd("z", map[string]float64{"a": 2}, data_structure.ZAddOptions{XX: true}))

	// GT blocks smaller
	assert.Equal(t, ptr(uint32(0)),
		s.ZAdd("z", map[string]float64{"a": 1}, data_structure.ZAddOptions{GT: true}))

	// LT blocks larger
	assert.Equal(t, ptr(uint32(0)),
		s.ZAdd("z", map[string]float64{"a": 3}, data_structure.ZAddOptions{LT: true}))

	assert.Equal(t, uint32(1), s.ZCard("z"))
}

func TestZCard(t *testing.T) {
	tests := map[string]struct {
		setup func(s Store)
		key   string
		want  uint32
	}{
		"non-existent key": {
			setup: func(s Store) {},
			key:   "zset1",
			want:  0,
		},
		"zset with members": {
			setup: func(s Store) {
				s.ZAdd("zset1", map[string]float64{"m1": 1.0, "m2": 2.0, "m3": 3.0}, data_structure.ZAddOptions{})
			},
			key:  "zset1",
			want: 3,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			s := NewStore()
			tc.setup(s)

			got := s.ZCard(tc.key)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestZCount(t *testing.T) {
	tests := map[string]struct {
		setup    func(s Store)
		key      string
		minScore float64
		maxScore float64
		want     uint32
	}{
		"non-existent key": {
			setup:    func(s Store) {},
			key:      "zset1",
			minScore: 0,
			maxScore: 10,
			want:     0,
		},
		"count in range": {
			setup: func(s Store) {
				s.ZAdd("zset1", map[string]float64{"m1": 1.0, "m2": 2.0, "m3": 3.0, "m4": 4.0}, data_structure.ZAddOptions{})
			},
			key:      "zset1",
			minScore: 2.0,
			maxScore: 3.0,
			want:     2,
		},
		"no members in range": {
			setup: func(s Store) {
				s.ZAdd("zset1", map[string]float64{"m1": 1.0, "m2": 2.0}, data_structure.ZAddOptions{})
			},
			key:      "zset1",
			minScore: 5.0,
			maxScore: 10.0,
			want:     0,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			s := NewStore()
			tc.setup(s)

			got := s.ZCount(tc.key, tc.minScore, tc.maxScore)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestZIncrBy(t *testing.T) {
	tests := map[string]struct {
		setup     func(s Store)
		key       string
		member    string
		increment float64
		wantScore float64
		wantOk    bool
	}{
		"increment new member": {
			setup:     func(s Store) {},
			key:       "zset1",
			member:    "member1",
			increment: 5.0,
			wantScore: 5.0,
			wantOk:    true,
		},
		"increment existing member": {
			setup: func(s Store) {
				s.ZAdd("zset1", map[string]float64{"member1": 3.0}, data_structure.ZAddOptions{})
			},
			key:       "zset1",
			member:    "member1",
			increment: 2.0,
			wantScore: 5.0,
			wantOk:    true,
		},
		"increment to infinity": {
			setup: func(s Store) {
				s.ZAdd("zset1", map[string]float64{"member1": 1e308}, data_structure.ZAddOptions{})
			},
			key:       "zset1",
			member:    "member1",
			increment: 1e308,
			wantScore: 0,
			wantOk:    false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			s := NewStore()
			tc.setup(s)

			gotScore, gotOk := s.ZIncrBy(tc.key, tc.member, tc.increment)
			assert.Equal(t, tc.wantOk, gotOk)
			assert.Equal(t, tc.wantScore, gotScore)
		})
	}
}

func TestZScore(t *testing.T) {
	tests := map[string]struct {
		setup  func(s Store)
		key    string
		member string
		want   *float64
	}{
		"non-existent key": {
			setup:  func(s Store) {},
			key:    "zset1",
			member: "member1",
			want:   nil,
		},
		"non-existent member": {
			setup: func(s Store) {
				s.ZAdd("zset1", map[string]float64{"member1": 1.0}, data_structure.ZAddOptions{})
			},
			key:    "zset1",
			member: "member2",
			want:   nil,
		},
		"existing member": {
			setup: func(s Store) {
				s.ZAdd("zset1", map[string]float64{"member1": 5.5}, data_structure.ZAddOptions{})
			},
			key:    "zset1",
			member: "member1",
			want:   ptr(5.5),
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			s := NewStore()
			tc.setup(s)

			got := s.ZScore(tc.key, tc.member)
			if tc.want == nil {
				assert.Nil(t, got)
			} else {
				assert.NotNil(t, got)
				assert.Equal(t, *tc.want, *got)
			}
		})
	}
}

func TestZMScore(t *testing.T) {
	tests := map[string]struct {
		setup   func(s Store)
		key     string
		members []string
		want    []*float64
	}{
		"non-existent key": {
			setup:   func(s Store) {},
			key:     "zset1",
			members: []string{"m1", "m2"},
			want:    []*float64{nil, nil},
		},
		"mixed existing and non-existing": {
			setup: func(s Store) {
				s.ZAdd("zset1", map[string]float64{"m1": 1.0, "m3": 3.0}, data_structure.ZAddOptions{})
			},
			key:     "zset1",
			members: []string{"m1", "m2", "m3"},
			want:    []*float64{ptr(1.0), nil, ptr(3.0)},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			s := NewStore()
			tc.setup(s)

			got := s.ZMScore(tc.key, tc.members)
			assert.Equal(t, len(tc.want), len(got))
			for i := range tc.want {
				if tc.want[i] == nil {
					assert.Nil(t, got[i])
				} else {
					assert.NotNil(t, got[i])
					assert.Equal(t, *tc.want[i], *got[i])
				}
			}
		})
	}
}

func TestZRem(t *testing.T) {
	tests := map[string]struct {
		setup    func(s Store)
		key      string
		members  []string
		want     uint32
		wantCard uint32
	}{
		"non-existent key": {
			setup:    func(s Store) {},
			key:      "zset1",
			members:  []string{"m1"},
			want:     0,
			wantCard: 0,
		},
		"remove existing members": {
			setup: func(s Store) {
				s.ZAdd("zset1", map[string]float64{"m1": 1.0, "m2": 2.0, "m3": 3.0}, data_structure.ZAddOptions{})
			},
			key:      "zset1",
			members:  []string{"m1", "m2"},
			want:     2,
			wantCard: 1,
		},
		"remove non-existing members": {
			setup: func(s Store) {
				s.ZAdd("zset1", map[string]float64{"m1": 1.0}, data_structure.ZAddOptions{})
			},
			key:      "zset1",
			members:  []string{"m2", "m3"},
			want:     0,
			wantCard: 1,
		},
		"remove mixed": {
			setup: func(s Store) {
				s.ZAdd("zset1", map[string]float64{"m1": 1.0, "m2": 2.0}, data_structure.ZAddOptions{})
			},
			key:      "zset1",
			members:  []string{"m1", "m3"},
			want:     1,
			wantCard: 1,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			s := NewStore()
			tc.setup(s)

			got := s.ZRem(tc.key, tc.members)
			assert.Equal(t, tc.want, got)
			assert.Equal(t, tc.wantCard, s.ZCard(tc.key))
		})
	}
}

func TestZRank(t *testing.T) {
	s := NewStore()
	s.ZAdd("z", map[string]float64{"a": 1, "b": 2}, data_structure.ZAddOptions{})

	assert.Nil(t, s.ZRank("x", "a", false))
	assert.Nil(t, s.ZRank("z", "x", false))
	assert.Equal(t, []any{1}, s.ZRank("z", "b", false))
	assert.Equal(t, []any{1, "2"}, s.ZRank("z", "b", true))
}


func TestZRevRank(t *testing.T) {
	s := NewStore()

	s.ZAdd("z", map[string]float64{
		"a": 1,
		"b": 2,
		"c": 3,
	}, data_structure.ZAddOptions{})

	// non-existent key OR member → nil
	assert.Nil(t, s.ZRevRank("x", "a", false))
	assert.Nil(t, s.ZRevRank("z", "x", false))

	// reverse rank (highest score first)
	assert.Equal(t, []any{1}, s.ZRevRank("z", "b", false))
	assert.Equal(t, []any{1, "2"}, s.ZRevRank("z", "b", true))
}


func TestZRangeByRank(t *testing.T) {
	tests := map[string]struct {
		setup      func(s Store)
		key        string
		start      int
		stop       int
		withScores bool
		want       []string
	}{
		"non-existent key": {
			setup:      func(s Store) {},
			key:        "zset1",
			start:      0,
			stop:       -1,
			withScores: false,
			want:       []string{},
		},
		"range without scores": {
			setup: func(s Store) {
				s.ZAdd("zset1", map[string]float64{"m1": 1.0, "m2": 2.0, "m3": 3.0}, data_structure.ZAddOptions{})
			},
			key:        "zset1",
			start:      0,
			stop:       1,
			withScores: false,
			want:       []string{"m1", "m2"},
		},
		"range with scores": {
			setup: func(s Store) {
				s.ZAdd("zset1", map[string]float64{"m1": 1.0, "m2": 2.0}, data_structure.ZAddOptions{})
			},
			key:        "zset1",
			start:      0,
			stop:       -1,
			withScores: true,
			want:       []string{"m1", "1", "m2", "2"},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			s := NewStore()
			tc.setup(s)

			got := s.ZRangeByRank(tc.key, tc.start, tc.stop, tc.withScores)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestZRangeByScore(t *testing.T) {
	tests := map[string]struct {
		setup      func(s Store)
		key        string
		start      float64
		stop       float64
		withScores bool
		want       []string
	}{
		"non-existent key": {
			setup:      func(s Store) {},
			key:        "zset1",
			start:      0,
			stop:       10,
			withScores: false,
			want:       []string{},
		},
		"range by score without scores": {
			setup: func(s Store) {
				s.ZAdd("zset1", map[string]float64{"m1": 1.0, "m2": 2.0, "m3": 3.0}, data_structure.ZAddOptions{})
			},
			key:        "zset1",
			start:      1.5,
			stop:       3.0,
			withScores: false,
			want:       []string{"m2", "m3"},
		},
		"range by score with scores": {
			setup: func(s Store) {
				s.ZAdd("zset1", map[string]float64{"m1": 1.0, "m2": 2.0}, data_structure.ZAddOptions{})
			},
			key:        "zset1",
			start:      0,
			stop:       10,
			withScores: true,
			want:       []string{"m1", "1", "m2", "2"},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			s := NewStore()
			tc.setup(s)

			got := s.ZRangeByScore(tc.key, tc.start, tc.stop, tc.withScores)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestZPopMin(t *testing.T) {
	tests := map[string]struct {
		setup    func(s Store)
		key      string
		count    int
		want     []string
		wantCard uint32
	}{
		"non-existent key": {
			setup:    func(s Store) {},
			key:      "zset1",
			count:    1,
			want:     []string{},
			wantCard: 0,
		},
		"pop multiple min": {
			setup: func(s Store) {
				s.ZAdd("zset1", map[string]float64{"m1": 1.0, "m2": 2.0, "m3": 3.0}, data_structure.ZAddOptions{})
			},
			key:      "zset1",
			count:    2,
			want:     []string{"m1", "1", "m2", "2"},
			wantCard: 1,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			s := NewStore()
			tc.setup(s)

			got := s.ZPopMin(tc.key, tc.count)
			assert.Equal(t, tc.want, got)
			assert.Equal(t, tc.wantCard, s.ZCard(tc.key))
		})
	}
}

func TestZPopMax(t *testing.T) {
	tests := map[string]struct {
		setup    func(s Store)
		key      string
		count    int
		want     []string
		wantCard uint32
	}{
		"non-existent key": {
			setup:    func(s Store) {},
			key:      "zset1",
			count:    1,
			want:     []string{},
			wantCard: 0,
		},
		"pop multiple max": {
			setup: func(s Store) {
				s.ZAdd("zset1", map[string]float64{"m1": 1.0, "m2": 2.0, "m3": 3.0}, data_structure.ZAddOptions{})
			},
			key:      "zset1",
			count:    2,
			want:     []string{"m3", "3", "m2", "2"},
			wantCard: 1,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			s := NewStore()
			tc.setup(s)

			got := s.ZPopMax(tc.key, tc.count)
			assert.Equal(t, tc.want, got)
			assert.Equal(t, tc.wantCard, s.ZCard(tc.key))
		})
	}
}

func TestZRandMember(t *testing.T) {
	tests := map[string]struct {
		setup      func(s Store)
		key        string
		count      int
		withScores bool
		wantLen    int
	}{
		"non-existent key": {
			setup:      func(s Store) {},
			key:        "zset1",
			count:      1,
			withScores: false,
			wantLen:    0,
		},
		"random member with score": {
			setup: func(s Store) {
				s.ZAdd("zset1", map[string]float64{"m1": 1.0, "m2": 2.0}, data_structure.ZAddOptions{})
			},
			key:        "zset1",
			count:      1,
			withScores: true,
			wantLen:    2, // member + score
		},
		"negative count allows duplicates": {
			setup: func(s Store) {
				s.ZAdd("zset1", map[string]float64{"m1": 1.0}, data_structure.ZAddOptions{})
			},
			key:        "zset1",
			count:      -5,
			withScores: false,
			wantLen:    5,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			s := NewStore()
			tc.setup(s)

			got := s.ZRandMember(tc.key, tc.count, tc.withScores)
			assert.Equal(t, tc.wantLen, len(got))
		})
	}
}

func TestZLexCount(t *testing.T) {
	tests := map[string]struct {
		setup    func(s Store)
		key      string
		minValue string
		maxValue string
		want     uint32
	}{
		"non-existent key": {
			setup:    func(s Store) {},
			key:      "zset1",
			minValue: "a",
			maxValue: "z",
			want:     0,
		},
		"count in lex range": {
			setup: func(s Store) {
				s.ZAdd("zset1", map[string]float64{"apple": 0, "banana": 0, "cherry": 0}, data_structure.ZAddOptions{})
			},
			key:      "zset1",
			minValue: "a",
			maxValue: "c",
			want:     2,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			s := NewStore()
			tc.setup(s)

			got := s.ZLexCount(tc.key, tc.minValue, tc.maxValue)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestZRangeByLex(t *testing.T) {
	tests := map[string]struct {
		setup func(s Store)
		key   string
		start string
		stop  string
		want  []string
	}{
		"non-existent key": {
			setup: func(s Store) {},
			key:   "zset1",
			start: "a",
			stop:  "z",
			want:  []string{},
		},
		"lex range": {
			setup: func(s Store) {
				s.ZAdd("zset1", map[string]float64{"apple": 0, "banana": 0, "cherry": 0, "date": 0}, data_structure.ZAddOptions{})
			},
			key:   "zset1",
			start: "b",
			stop:  "d",
			want:  []string{"banana", "cherry"},
		},
		"all members": {
			setup: func(s Store) {
				s.ZAdd("zset1", map[string]float64{"a": 0, "b": 0, "c": 0}, data_structure.ZAddOptions{})
			},
			key:   "zset1",
			start: "",
			stop:  "~",
			want:  []string{"a", "b", "c"},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			s := NewStore()
			tc.setup(s)

			got := s.ZRangeByLex(tc.key, tc.start, tc.stop)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestZRevRangeByLex(t *testing.T) {
	tests := map[string]struct {
		setup func(s Store)
		key   string
		start string
		stop  string
		want  []string
	}{
		"non-existent key": {
			setup: func(s Store) {},
			key:   "zset1",
			start: "z",
			stop:  "a",
			want:  []string{},
		},
		"reverse lex range": {
			setup: func(s Store) {
				s.ZAdd("zset1", map[string]float64{"apple": 0, "banana": 0, "cherry": 0, "date": 0}, data_structure.ZAddOptions{})
			},
			key:   "zset1",
			start: "d",
			stop:  "b",
			want:  []string{"cherry", "banana"},
		},
		"all members reversed": {
			setup: func(s Store) {
				s.ZAdd("zset1", map[string]float64{"a": 0, "b": 0, "c": 0}, data_structure.ZAddOptions{})
			},
			key:   "zset1",
			start: "~",
			stop:  "",
			want:  []string{"c", "b", "a"},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			s := NewStore()
			tc.setup(s)

			got := s.ZRevRangeByLex(tc.key, tc.start, tc.stop)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestZRevRangeByRank(t *testing.T) {
	tests := map[string]struct {
		setup      func(s Store)
		key        string
		start      int
		stop       int
		withScores bool
		want       []string
	}{
		"non-existent key": {
			setup:      func(s Store) {},
			key:        "zset1",
			start:      0,
			stop:       -1,
			withScores: false,
			want:       []string{},
		},
		"reverse range without scores": {
			setup: func(s Store) {
				s.ZAdd("zset1", map[string]float64{"m1": 1.0, "m2": 2.0, "m3": 3.0}, data_structure.ZAddOptions{})
			},
			key:        "zset1",
			start:      0,
			stop:       1,
			withScores: false,
			want:       []string{"m3", "m2"},
		},
		"reverse range with scores": {
			setup: func(s Store) {
				s.ZAdd("zset1", map[string]float64{"m1": 1.0, "m2": 2.0}, data_structure.ZAddOptions{})
			},
			key:        "zset1",
			start:      0,
			stop:       -1,
			withScores: true,
			want:       []string{"m2", "2", "m1", "1"},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			s := NewStore()
			tc.setup(s)

			got := s.ZRevRangeByRank(tc.key, tc.start, tc.stop, tc.withScores)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestZRevRangeByScore(t *testing.T) {
	tests := map[string]struct {
		setup      func(s Store)
		key        string
		start      float64
		stop       float64
		withScores bool
		want       []string
	}{
		"non-existent key": {
			setup:      func(s Store) {},
			key:        "zset1",
			start:      10,
			stop:       0,
			withScores: false,
			want:       []string{},
		},
		"reverse range by score without scores": {
			setup: func(s Store) {
				s.ZAdd("zset1", map[string]float64{"m1": 1.0, "m2": 2.0, "m3": 3.0}, data_structure.ZAddOptions{})
			},
			key:        "zset1",
			start:      3.0,
			stop:       1.5,
			withScores: false,
			want:       []string{"m3", "m2"},
		},
		"reverse range by score with scores": {
			setup: func(s Store) {
				s.ZAdd("zset1", map[string]float64{"m1": 1.0, "m2": 2.0}, data_structure.ZAddOptions{})
			},
			key:        "zset1",
			start:      10,
			stop:       0,
			withScores: true,
			want:       []string{"m2", "2", "m1", "1"},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			s := NewStore()
			tc.setup(s)

			got := s.ZRevRangeByScore(tc.key, tc.start, tc.stop, tc.withScores)
			assert.Equal(t, tc.want, got)
		})
	}
}

// Helper function to create pointer to uint32
func ptr[T any](v T) *T {
	return &v
}

