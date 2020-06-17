package db


type TransactionIsolation int

const (
	Serializable TransactionIsolation = iota + 1
	PhantomRead
	RepeatableRead
)


type TxOptions struct {
	txIsolation TransactionIsolation
	ro          *ReadOptions
	co          *CommitOptions
}

type ReadOptions struct {
	QuorumSize int
}

type CommitOptions struct {
	QuorumSize int
}

