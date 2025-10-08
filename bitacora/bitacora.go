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

type Logger struct {
	level BCLevel
	mode BCMode

	logger *slog.Logger
}

func NewLogger(level BCLevel, mode BCMode) Logger {
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

	// Create a new Logger with the custom handler
	logger := slog.New(handler)


	return Logger {
		level: level,
		mode: mode,
		logger: logger,
	}
}

// Programacion orientada a objetos: Enterprise edition.
func (l *Logger) Debug(message string) {
	slog.Debug(message)
}

func (l *Logger) Info(message string) {
	slog.Info(message)
}

// :0
func (l *Logger) Error(message string) {
	if l.mode == DebugMode {
		panic(message)
	} else {
	   slog.Error(message)
	}
}
