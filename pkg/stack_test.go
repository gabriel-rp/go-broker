package pkg

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateStack(t *testing.T) {
	_ = Stack[int]{items: []int{1, 2, 3}}
}

func TestPushStack(t *testing.T) {
	stack := Stack[int]{items: make([]int, 0)}
	stack.Push(1)
	assert.Equal(t, []int{1}, stack.items)
	stack.Push(2)
	assert.Equal(t, []int{1, 2}, stack.items)
}

func TestPopStack(t *testing.T) {
	stack := Stack[int]{items: make([]int, 0)}
	stack.items = []int{1, 2}
	item, ok := stack.Pop()
	assert.True(t, ok)
	assert.Equal(t, 2, item)
	assert.Equal(t, []int{1}, stack.items)
	item, ok = stack.Pop()
	assert.True(t, ok)
	assert.Equal(t, 1, item)
	assert.Equal(t, []int{}, stack.items)
	item, ok = stack.Pop()
	assert.False(t, ok)
	assert.Equal(t, 0, item)
	assert.Equal(t, []int{}, stack.items)
}

func TestPeekStack(t *testing.T) {
	stack := Stack[int]{items: make([]int, 0)}
	stack.items = []int{1, 2}
	item, ok := stack.Peek()
	assert.True(t, ok)
	assert.Equal(t, 2, item)
	assert.Equal(t, []int{1, 2}, stack.items)
}

func TestIsEmpty(t *testing.T) {
	stack := Stack[int]{items: make([]int, 0)}
	stack.items = []int{1, 2, 3}
	assert.False(t, stack.IsEmpty())

	stack = Stack[int]{items: make([]int, 0)}
	stack.items = []int{}
	assert.True(t, stack.IsEmpty())

	stack = Stack[int]{items: make([]int, 0)}
	stack.items = []int{1}
	stack.Pop()
	assert.True(t, stack.IsEmpty())
}

func TestItems(t *testing.T) {
	stack := Stack[int]{items: make([]int, 0)}
	stack.Push(1)
	stack.Push(2)
	stack.Push(3)
	assert.Equal(t, []int{1, 2, 3}, stack.Items())
}
