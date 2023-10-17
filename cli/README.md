# Config Orion via CLI

This command-line tool provides a simple way to config an orion database server.

## Building the tool
1. Run from `orion-sdk` root folder
2. Run `make binary` to create an executable file named bcdbadmin under `bin` directory.

## Commands

Here we list and describe the available commands.
We give a short explanation of their usage and describe the flags for each command.
We provide real-world examples demonstrating how to use the CLI tool for various tasks.


### Version Command
This command prints the version of the CLI tool.
1. Run from `orion-sdk` root folder.
2. Run `./bin/bcdbadmin version`. This command has no flags.



### Config Command
This command enables to config an orion server or ask for the configuration of an orion server. 

#### Get Config Command
1. Run from 'orion-sdk' root folder.
2. For Get Config Run `bin/bcdbadmin config get [args]`.

   Replace `[args]` with flags.

###
##### Flags
| Flags                             | Description                                                                   |
|-----------------------------------|-------------------------------------------------------------------------------|
| `-d, --db-connection-config-path` | the absolute or relative path of CLI connection configuration file            |
| `-c, --cluster-config-path`       | the absolute or relative path to which the server configuration will be saved |

Both flags are necessary flags. If any flag is missing, the cli will raise an error.

###
##### Example:

Running 
`bin/bcdbadmin config get -d "connection-session-config.yaml" -c "local/config"`
reads the connection and session details needed for connecting to a server from `connection-session-config.yaml` and 
sends a config TX.
It creates directories in `local/config` with the respective certificates, a yaml file, named shared_cluster_config.yml, that includes the cluster configuration
and a yaml file, named version.yml, that includes the version.