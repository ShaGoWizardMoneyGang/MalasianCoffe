package bitacora

//Paquete BitaCora para simplifcar logging.

import (
	"log/slog"
	"os"
)

// La unica diferencia entre uno y otro es que los errores en debug mode
// paniquean, mientras que en release mode solo muestran un mensaje.
type BCMode uint
const (
	DebugMode BCMode = iota
	ReleaseMode BCMode = iota
)

type BCLevel uint

const (
	BCDebug BCLevel = iota
	BCInfo BCLevel = iota
	BCError BCLevel = iota
)

// Nos guardamos el modo con el que el logger va actuar.
// Por defecto esta en debug.
var globalMode BCMode = DebugMode;

func InitializeLogging(level BCLevel, mode BCMode) {
	var slogLevel slog.Level
	switch level {
	case BCDebug:
		slogLevel = slog.LevelDebug
	case BCInfo:
		slogLevel = slog.LevelInfo
	case BCError:
		slogLevel = slog.LevelError
	}
	// Create a new TextHandler that writes to os.Stdout
	handler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slogLevel, // Set the minimum log level
		AddSource: true,  // Add file and line number to logs
	})
	slog.SetDefault(slog.New(handler))

	globalMode = mode

}

// Programacion orientada a objetos: Enterprise edition.
func Debug(message string) {
	slog.Debug(message)
}

func Info(message string) {
	slog.Info(message)
}

// :0
func Error(message string) {
	if globalMode == DebugMode {
		panic(message)
	} else {
	   slog.Error(message)
	}
}
