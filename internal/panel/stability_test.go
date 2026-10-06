package panel

import "testing"

func TestCalcStabilityFull(t *testing.T) {
	c := "x0.00"
	s := calcStability(stabilityInput{
		Requests: 100, Errors: 5, Credits: 0,
		Accounts: 4, Exhausted: 1, Catalog: c,
	})
	if s.Score == nil || *s.Score != 90 {
		t.Fatalf("score=%v want 90 (50*95+30*75+20*100=9000/100)", s.Score)
	}
	if *s.Success != 95 || *s.Quota != 75 || s.Price != 100 {
		t.Fatalf("breakdown wrong: %+v", s)
	}
}

func TestCalcStabilityPriceHike(t *testing.T) {
	// 目录涨价但实扣为 0 → 价格 50。
	s := calcStability(stabilityInput{
		Requests: 100, Errors: 0, Credits: 0,
		Accounts: 2, Exhausted: 0, Catalog: "x0.29",
	})
	if s.Price != 50 {
		t.Fatalf("price=%v want 50", s.Price)
	}
	if s.Score == nil || *s.Score != 90 { // 50*100+30*100+20*50=9000/100
		t.Fatalf("score=%v want 90", s.Score)
	}
}

func TestCalcStabilityCharged(t *testing.T) {
	// 实扣 >0 → 价格 0。
	s := calcStability(stabilityInput{
		Requests: 100, Errors: 0, Credits: 0.01,
		Accounts: 1, Exhausted: 0, Catalog: "x0.11",
	})
	if s.Price != 0 {
		t.Fatalf("price=%v want 0", s.Price)
	}
}

func TestCalcStabilityLowSample(t *testing.T) {
	s := calcStability(stabilityInput{Requests: 5, Errors: 0})
	if s.Score != nil {
		t.Fatalf("score should be nil on low sample, got %v", *s.Score)
	}
	if s.Note == "" {
		t.Fatal("note should explain low sample")
	}
}

func TestCalcStabilityNoQuota(t *testing.T) {
	// 无账号在用：额度分缺失，权重归一到 70。
	s := calcStability(stabilityInput{
		Requests: 100, Errors: 10, Credits: 0,
		Accounts: 0, Exhausted: 0, Catalog: "x0.00",
	})
	if s.Quota != nil {
		t.Fatal("quota should be nil")
	}
	// (50*90 + 20*100)/70 = 6500/70 = 92.857 → 93
	if s.Score == nil || *s.Score != 93 {
		t.Fatalf("score=%v want 93", s.Score)
	}
}
