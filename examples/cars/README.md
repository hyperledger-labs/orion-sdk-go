# Car registry demo

* A car registry owned by the `dmv`

* Car `dealer` can mint a new car

* Owners `alice`, `bob` (and `dealer`) can own a car & transfer ownership 


##To build:

`cd examples/cars`

`go build`

##To run the demo
* Setup the demo directory env var

`export CARS_DEMO_DIR=/tmp/cars-demo`
 
* Generate crypto and config material

`./cars generate`

* Start the server in another process on the same host (in `server/cmd/bdb` dir):

`./bdb start --configpath /tmp/cars-demo/config &`

* Initialise the database: create `carDB`, onboard users `dmv` `dealer` `alice` `bob`

`./cars init`

* Mint a new car

`./cars mint-request -u dealer -c RED`

`./cars mint-approve -u dmv -k <mint-request-key>`

* List the car

`./cars list-car -u dmv -c RED`

`./cars list-car -u dealer -c RED`

`./cars list-car -u bob -c RED`

`./cars list-car -u dmv -c Blue`

* Transfer ownership

`./cars transfer-to -u dealer -b alice -c RED`

`./cars transfer-receive -u alice -c RED -k <transfer-to-key>`

`./cars transfer -u dmv -k <transfer-to-key> -r <transfer-receive-key`

* Provenance

`./cars list-car -u alice -c RED`

`./cars list-car -u dmv -c RED --provenance`

* Verify Tx evidence (envelope & receipt) against Tx proof

`ls $CARS_DEMO_DIR/txs`

`./cars verify-tx -u alice -t <tx-id>`
