# Car registry demo

An imaginary and oversimplified car registry, in which the DMV (department of motor vehicles) keeps track of all cars and their ownership.

## Roles
* A car registry owned by the DMV, with user `dmv`.
  * The `dmv` approves a mint request of a new car by a car dealer, inserting a new car record into the database.
  * The `dmv` approves a transfer of ownership between a car owner (seller) to a new car owner (buyer).
* Car dealer, with user `dealer`. 
  * Issues a mint request for a new car, which the `dmv` must approve.
  * The `dealer` can then transfer ownership by selling the car to a new owner, say `alice`, the buyer.
* Owners `alice`, `bob` (and `dealer`) 
  * Can own a car. 
  * Can transfer ownership of the car they own.
  * Can approve the reception (buying) of a car, and assume ownership of it. 

## To build:

Build the demo binary:

`cd examples/cars`

`go build`

Download and build the `orion-server`, see [instructions here](http://labs.hyperledger.org/orion-server/docs/getting-started/launching-one-node/binary).

## To run the demo

### Setup and initialization

* Set up the demo directory env var. for example:

`export CARS_DEMO_DIR=/tmp/cars-demo`

* Generate crypto and config material

`./cars generate`

* Start the server in another process on the same host (in `orion-server/bin` dir):

`./bdb start --configpath $CARS_DEMO_DIR/config &`

* Initialise the database: create `carDB`, onboard users `dmv` `dealer` `alice` `bob`

`./cars init`

### Mint a new car

* Mint a new car
Issue a car mint request transaction:

`./cars mint-request -u dealer -c <car-registration>`

for example:

`./cars mint-request -u dealer -c RED`

Issue a car mint approval transaction:

`./cars mint-approve -u dmv -k <mint-request-key>`

for example:

`./cars mint-approve -u dmv -k mint-request~Y1wBzSDMxYH3C5m5PVi_Tn6sBpZVv-NT0MkHBjsFQME=`

* List the car

`./cars list-car -u dmv -c RED` 

`./cars list-car -u dealer -c RED` 

These will fail, as `bob` has no access and the `BLUE` car does not exist.

`./cars list-car -u bob -c RED` 

`./cars list-car -u dmv -c BLUE`

###  Transfer ownership

* Transfer ownership between a seller (current owner) and a buyer (next owner) with the `dmv` as a notary.  

Issue a transfer ownership multi-sig transaction:

`./cars transfer -u dmv -s <seller> -b <buyer> -c <car-registration>`

for example:

`./cars transfer -u dmv -s dealer -b alice -c RED`

This issues a multi-sig transaction between the `dealer`, `alice`, and the `dmv`.

###  Provenance and proofs

* Provenance

Inspect some provenance information on the car, for example:
`./cars list-car -u alice -c RED`

`./cars list-car -u dmv -c RED --provenance`

* Verify Tx evidence (envelope & receipt) against Tx proof

`ls $CARS_DEMO_DIR/txs`

If a user has the transaction evidence (Tx envelope & Tx receipt), he can obtain a proof that the TX exists in the ledger and verify the proof: 
`./cars verify-tx -u <user-id> -t <tx-id>`

for example, to prove the existence of the car ownership transaction, assume `alice` gave the Tx evidence to `bob`:

`./cars verify-tx -u bob -t l0c7CvOpuMQjdlH1NAbqtSPzkihr1LN3Kn1eHOg64xs=`

