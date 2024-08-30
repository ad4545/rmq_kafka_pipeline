package channel

import (
	"fmt"
	"sync"
)

type Bridge[S any, P any] struct {
	name      string // message name passing through this bridge
	input     <-chan []byte
	output    chan<- []byte
	done      chan int
	errCh     chan error
	converter func(in S) (P, error)
}

func NewBridge[S any, P any](name string, in <-chan []byte, out chan<- []byte, done chan int, errCh chan error) *Bridge[S, P] {
	return &Bridge[S, P]{
		name:   name,
		input:  in,
		output: out,
		done:   done,
		errCh:  errCh,
	}
}

func (b *Bridge[S, P]) SetConverter(c func(in S) (P, error)) *Bridge[S, P] {
	b.converter = c
	return b
}

func (b *Bridge[S, P]) Run(wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {

		case msg, ok := <-b.input:
			fmt.Println(msg, "message on bridge--------")
			if !ok {
				fmt.Println("called---ok")
				continue
			}

			// if b.converter == nil {
			// 	b.errCh <- fmt.Errorf("Converter not set")
			// 	continue
			// }

			// out, err := b.converter(msg)

			// //let the higher level code deal with the error
			// if err != nil {
			// 	b.errCh <- fmt.Errorf("Failure converting %s message", b.name)
			// 	continue
			// }

			b.output <- msg

		case <-b.done:
			return
		}
	}
}
