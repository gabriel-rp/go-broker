package pkg

type Stack[T any] struct {
	items []T
}

func NewStack[T any]() Stack[T] {
	return Stack[T]{items: make([]T, 0)}
}

func (s *Stack[T]) Push(item T) {
	s.items = append(s.items, item)
}

func (s *Stack[T]) Pop() (T, bool) {
	lastItem, ok := s.Peek()
	if !ok {
		return lastItem, ok
	}
	s.items = s.items[:len(s.items)-1]
	return lastItem, ok
}

func (s *Stack[T]) Peek() (T, bool) {
	if s.IsEmpty() {
		var zero T
		return zero, false
	}
	lastItemIndex := len(s.items) - 1
	return s.items[lastItemIndex], true
}

func (s *Stack[T]) IsEmpty() bool {
	return len(s.items) == 0
}

func (s *Stack[T]) Size() int {
	return len(s.items)
}

func (s *Stack[T]) Items() []T {
	return s.items
}
