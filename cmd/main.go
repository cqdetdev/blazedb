package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/cqdetdev/blazedb"
	"github.com/df-mc/dragonfly/server"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "convert" {
		runConvert(os.Args[2:])
		return
	}
	runServer(os.Args[1:])
}

func runServer(args []string) {
	fs := flag.NewFlagSet("blazedb-dragonfly", flag.ExitOnError)
	worldPath := fs.String("world", "world_blazedb", "BlazeDB world folder to open")
	addr := fs.String("addr", ":19132", "UDP address for Bedrock clients")
	name := fs.String("name", "BlazeDB Test Server", "server name shown in the Bedrock server list")
	auth := fs.Bool("auth", false, "require Xbox Live authentication")
	readOnly := fs.Bool("readonly", false, "prevent Dragonfly from saving world changes")
	turbo := fs.Bool("turbo", false, "use BlazeDB turbo options for the provider")
	maxPlayers := fs.Int("max-players", 4, "maximum players allowed to join")
	chunkRadius := fs.Int("chunk-radius", 32, "maximum chunk radius allowed for clients")
	resources := fs.String("resources", filepath.Join(".dragonfly-test", "resources"), "resource pack folder for Dragonfly")
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "Usage: go run ./cmd [flags]\n\n")
		fmt.Fprintf(fs.Output(), "Starts a minimal Dragonfly server backed by BlazeDB so you can join in-game.\n\n")
		fs.PrintDefaults()
		fmt.Fprintf(fs.Output(), "\nSubcommands:\n")
		fmt.Fprintf(fs.Output(), "  convert    Convert worlds between LevelDB and BlazeDB formats.\n")
	}
	_ = fs.Parse(args)
	if fs.NArg() != 0 {
		fmt.Fprintf(os.Stderr, "unexpected argument: %s\n", fs.Arg(0))
		fs.Usage()
		os.Exit(2)
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	opts := blazedb.DefaultOptions()
	if *turbo {
		opts = blazedb.TurboOptions()
	}
	opts.Log = logger

	provider, err := blazedb.Config{Options: opts}.Open(*worldPath)
	if err != nil {
		logger.Error("open BlazeDB world", "path", *worldPath, "error", err)
		os.Exit(1)
	}

	userConf := server.DefaultConfig()
	userConf.Network.Address = *addr
	userConf.Server.Name = *name
	userConf.Server.AuthEnabled = *auth
	userConf.World.SaveData = false
	userConf.Players.SaveData = false
	userConf.Players.MaxCount = *maxPlayers
	userConf.Players.MaximumChunkRadius = *chunkRadius
	userConf.Resources.AutoBuildPack = false
	userConf.Resources.Folder = *resources

	conf, err := userConf.Config(logger)
	if err != nil {
		_ = provider.Close()
		logger.Error("create Dragonfly config", "error", err)
		os.Exit(1)
	}
	conf.WorldProvider = provider
	conf.ReadOnlyWorld = *readOnly

	srv := conf.New()
	srv.CloseOnProgramEnd()
	srv.Listen()
	logger.Info("BlazeDB Dragonfly test server ready", "addr", *addr, "world", *worldPath, "auth", *auth, "readonly", *readOnly)

	for p := range srv.Accept() {
		logger.Info("player joined", "name", p.Name())
	}
}
