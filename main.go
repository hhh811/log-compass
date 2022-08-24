package main

import "fmt"

func main() {
	s := []int{1, 2, 3}
	s1 := s[:]
	s2 := s[0:0]
	fmt.Println(s1)
	fmt.Println(s2)
}
