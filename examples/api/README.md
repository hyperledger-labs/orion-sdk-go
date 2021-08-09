## Transactions examples

The examples' folder contains examples for basic transactions, here we describe how to build and run examples inside docker.

### Prerequisites

To build a docker image, the following are the prerequisites that should be installed.

- **Docker**: To run the BCDB image.
- **Git**: To clone the code repository.

## Build

To clone the repository, create the required directory
```
mkdir -p github.com/hyperledger-labs
```
Change the current working directory to the above created folder 
```
cd github.com/hyperledger-labs
```
#### Clone server and sdk repositories
```
git clone https://github.com/hyperledger-labs/orion-server
```
```
git clone https://github.com/hyperledger-labs/orion-sdk-go
```
Change the current working directory to the server root directory
```
cd orion-server
```
View orion-sdk-go/go.mod and copy the server <commit-hash> 
```
cat ../orion-sdk-go/go.mod 
```
Switch to the right version of the server that runs with the sdk
```
git checkout <commit-hash>
```


#### Generate cryptographic materials
   
Generate crypto materials for users used in the examples
```
./scripts/cryptoGen.sh sampleconfig alice bob charlie
```

#### Generate docker image
```
make docker
```
#### Start the server inside a docker container

```
docker run -it --rm -v $(pwd)/sampleconfig/:/etc/orion-server -p 6001:6001 -p 7050:7050 orion-server
``` 
#### After this step, you can run multiple examples one after another without repeating the build process

## Run an example

Go to the example directory 

``` 
cd ../orion-sdk-go/examples/api/<example-dir>
``` 
Build and run  
``` 
go build
``` 
``` 
./<example-dir>
```

## Clean up

Delete directories after running the examples 
```
cd ../../../../orion-server/sampleconfig
``` 
```
rm -r txs ledger
``` 

