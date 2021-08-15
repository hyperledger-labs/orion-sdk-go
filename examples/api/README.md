## Transactions examples

The examples' folder contains examples for basic transactions, here we describe how to build and run examples inside docker.

### Prerequisites

To build a docker image, the following are the prerequisites that should be installed.

- **Docker**: To run the BCDB image.
- **Git**: To clone the code repository.

## Build

To clone the repository, create the required directory
```
mkdir -p github.com/IBM-Blockchain
```
Change the current working directory to the above created folder 
```
cd github.com/IBM-Blockchain
```
#### Clone server and sdk repositories
```
git clone https://github.com/IBM-Blockchain/bcdb-server
```
```
git clone https://github.com/IBM-Blockchain/bcdb-sdk
```
Change the current working directory to the server root directory
```
cd bcdb-server
```
View bcdb-sdk/go.mod and copy the server <commit-hash> 
```
cat ../bcdb-sdk/go.mod 
```
Switch to the right version of the server that runs with the sdk
```
git checkout <commit-hash>
```


#### Generate cryptographic materials
   
Generate crypto materials for users used in the examples
```
./scripts/cryptoGen.sh sampleconfig alice bob 
```

#### Generate docker image
```
make docker
```
#### Start the server inside a docker container

```
docker run -it --rm -v $(pwd)/sampleconfig/:/etc/bcdb-server -p 6001:6001 -p 7050:7050 bcdb-server
``` 
#### After this step, you can run multiple examples one after another without repeating the build process

## Run an example

Go to the example directory 

``` 
cd ../bcdb-sdk/examples/api/<example-dir>
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
cd ../../../../bcdb-server/sampleconfig
``` 
```
rm -r txs ledger
``` 

