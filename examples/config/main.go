package main

import (
	"fmt"
	"github.com/IBM-Blockchain/bcdb-sdk/examples/config/generator"
	"gopkg.in/alecthomas/kingpin.v2"
)

const configEnvar = "CONFIG_PATH"

func main() {
	app := kingpin.New("config", "Config example")
	configPath := app.Flag("configPath",
		fmt.Sprintf("Path to the folder that will contain all the material for config. If missing, taken from envar: %s", configEnvar)).
		Short('c').
		Envar(configEnvar).
		Required().
		String()
	generator.Generate(*configPath)
}


