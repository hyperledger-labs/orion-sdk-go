package db

type Options struct {
	connectionOptions []*ConnectionOption
	*TxOptions
}

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

type ConnectionOption struct {
	server string
	port   int
}

type ReadOptions struct {
	QuorumSize int
}

type CommitOptions struct {
	QuorumSize int
}
