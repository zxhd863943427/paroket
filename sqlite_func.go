package paroket

import "math"

type customFuncImpl struct {
	impl any
	pure bool
}

func custmFunc() map[string]customFuncImpl {
	v := map[string]customFuncImpl{
		"xor": {xor, true},
	}
	return v
}

func custmAggrFunc() map[string]customFuncImpl {

	v := map[string]customFuncImpl{
		"std": {newStddev, true},
	}
	return v
}

func xor(xs ...int64) int64 {
	var ret int64
	for _, x := range xs {
		ret ^= x
	}
	return ret
}

type stddev struct {
	xs []int64
	// Running average calculation
	sum int64
	n   int64
}

func newStddev() *stddev { return &stddev{} }

func (s *stddev) Step(x int64) {
	s.xs = append(s.xs, x)
	s.sum += x
	s.n++
}

func (s *stddev) Done() float64 {
	mean := float64(s.sum) / float64(s.n)
	var sqDiff []float64
	for _, x := range s.xs {
		sqDiff = append(sqDiff, math.Pow(float64(x)-mean, 2))
	}
	var dev float64
	for _, x := range sqDiff {
		dev += x
	}
	dev /= float64(len(sqDiff))
	return math.Sqrt(dev)
}
